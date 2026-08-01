from __future__ import annotations

import importlib.util
import shutil
import tarfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest


MIGRATION_CONTENT = b"CREATE TABLE demo(id INTEGER);\n"
ENTRY_POINTS = "[console_scripts]\nmal-updater = mal_updater.cli:main\n"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_check_distribution() -> ModuleType:
    script_path = REPO_ROOT / "scripts" / "check_distribution.py"
    spec = importlib.util.spec_from_file_location("mal_updater_check_distribution_under_test", script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check_distribution = _load_check_distribution()


def _write_source_tree(root: Path, *, root_content: bytes = MIGRATION_CONTENT, package_content: bytes = MIGRATION_CONTENT) -> None:
    root_migrations = root / "migrations"
    package_migrations = root / "src" / "mal_updater" / "migrations"
    root_migrations.mkdir(parents=True)
    package_migrations.mkdir(parents=True)
    (root_migrations / "001_initial.sql").write_bytes(root_content)
    (package_migrations / "001_initial.sql").write_bytes(package_content)


def _build_wheel(
    dist_dir: Path,
    *,
    migration_name: str = "001_initial.sql",
    migration_content: bytes | None = MIGRATION_CONTENT,
    entry_points: str | None = ENTRY_POINTS,
) -> Path:
    wheel_path = dist_dir / "mal_updater-0.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel_path, "w") as wheel:
        if migration_content is not None:
            wheel.writestr(f"mal_updater/migrations/{migration_name}", migration_content)
        if entry_points is not None:
            wheel.writestr("mal_updater-0.0.0.dist-info/entry_points.txt", entry_points)
    return wheel_path


def _build_sdist(
    dist_dir: Path,
    *,
    package_migration_name: str = "001_initial.sql",
    package_migration_content: bytes = MIGRATION_CONTENT,
    root_migration_name: str = "001_initial.sql",
    root_migration_content: bytes = MIGRATION_CONTENT,
    include_package_migration: bool = True,
    include_root_migration: bool = True,
) -> Path:
    sdist_path = dist_dir / "mal_updater-0.0.0.tar.gz"
    with tarfile.open(sdist_path, "w:gz") as sdist:
        _add_bytes(sdist, "mal_updater-0.0.0/pyproject.toml", b"[project]\nname = 'mal-updater'\n")
        if include_package_migration:
            _add_bytes(
                sdist,
                f"mal_updater-0.0.0/src/mal_updater/migrations/{package_migration_name}",
                package_migration_content,
            )
        if include_root_migration:
            _add_bytes(sdist, f"mal_updater-0.0.0/migrations/{root_migration_name}", root_migration_content)
    return sdist_path


def _add_bytes(sdist: tarfile.TarFile, name: str, data: bytes) -> None:
    import io

    info = tarfile.TarInfo(name)
    info.size = len(data)
    sdist.addfile(info, io.BytesIO(data))


def _fixture(repo_root: Path, artifact_mutator: Callable[[Path], None] | None = None) -> tuple[Path, Path]:
    _write_source_tree(repo_root)
    dist_dir = repo_root / "dist"
    dist_dir.mkdir()
    _build_wheel(dist_dir)
    _build_sdist(dist_dir)
    if artifact_mutator is not None:
        artifact_mutator(dist_dir)
    return repo_root, dist_dir


def _assert_distribution_failure(repo_root: Path, dist_dir: Path, expected: str) -> None:
    with pytest.raises(SystemExit, match="distribution check failed") as excinfo:
        check_distribution.check_distribution(repo_root, dist_dir)
    assert expected in str(excinfo.value)


def test_distribution_check_passes_complete_temp_fixture(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo_root, dist_dir = _fixture(tmp_path)

    assert check_distribution.check_distribution(repo_root, dist_dir) == 0

    output = capsys.readouterr().out
    assert "source migration parity: 1 SQL files" in output
    assert "sdist package/root migrations: mal_updater-0.0.0.tar.gz: 1 SQL files each" in output


def test_distribution_check_fails_source_migration_name_mismatch(tmp_path: Path) -> None:
    _write_source_tree(tmp_path)
    (tmp_path / "src" / "mal_updater" / "migrations" / "001_initial.sql").unlink()
    (tmp_path / "src" / "mal_updater" / "migrations" / "002_other.sql").write_bytes(MIGRATION_CONTENT)
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _build_wheel(dist_dir)
    _build_sdist(dist_dir)

    _assert_distribution_failure(tmp_path, dist_dir, "source migration name mismatch")


def test_distribution_check_fails_source_migration_content_mismatch(tmp_path: Path) -> None:
    _write_source_tree(tmp_path, package_content=b"CREATE TABLE drifted(id INTEGER);\n")
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    _build_wheel(dist_dir)
    _build_sdist(dist_dir)

    _assert_distribution_failure(tmp_path, dist_dir, "source migration content mismatch")


def test_distribution_check_fails_missing_wheel_artifact(tmp_path: Path) -> None:
    def remove_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").unlink()

    repo_root, dist_dir = _fixture(tmp_path, remove_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "expected exactly one *.whl artifact")


def test_distribution_check_fails_extra_wheel_artifact(tmp_path: Path) -> None:
    def add_wheel(dist_dir: Path) -> None:
        shutil.copyfile(
            dist_dir / "mal_updater-0.0.0-py3-none-any.whl",
            dist_dir / "mal_updater-0.0.1-py3-none-any.whl",
        )

    repo_root, dist_dir = _fixture(tmp_path, add_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "expected exactly one *.whl artifact")


def test_distribution_check_fails_invalid_wheel_artifact(tmp_path: Path) -> None:
    def replace_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").write_text("not a zip", encoding="utf-8")

    repo_root, dist_dir = _fixture(tmp_path, replace_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "invalid wheel artifact")


def test_distribution_check_fails_invalid_wheel_package_data(tmp_path: Path) -> None:
    def replace_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").unlink()
        _build_wheel(dist_dir, migration_content=b"CREATE TABLE wrong(id INTEGER);\n")

    repo_root, dist_dir = _fixture(tmp_path, replace_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "wheel migration content mismatch")


def test_distribution_check_fails_missing_wheel_package_data(tmp_path: Path) -> None:
    def replace_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").unlink()
        _build_wheel(dist_dir, migration_content=None)

    repo_root, dist_dir = _fixture(tmp_path, replace_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "wheel migration mismatch")


def test_distribution_check_fails_missing_wheel_entry_point(tmp_path: Path) -> None:
    def replace_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").unlink()
        _build_wheel(dist_dir, entry_points=None)

    repo_root, dist_dir = _fixture(tmp_path, replace_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "expected exactly one wheel entry_points.txt")


def test_distribution_check_fails_wrong_wheel_entry_point(tmp_path: Path) -> None:
    def replace_wheel(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0-py3-none-any.whl").unlink()
        _build_wheel(dist_dir, entry_points="[console_scripts]\nother = mal_updater.cli:main\n")

    repo_root, dist_dir = _fixture(tmp_path, replace_wheel)

    _assert_distribution_failure(repo_root, dist_dir, "missing console script entry point")


def test_distribution_check_fails_missing_sdist_artifact(tmp_path: Path) -> None:
    def remove_sdist(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0.tar.gz").unlink()

    repo_root, dist_dir = _fixture(tmp_path, remove_sdist)

    _assert_distribution_failure(repo_root, dist_dir, "expected exactly one *.tar.gz artifact")


def test_distribution_check_fails_invalid_sdist_artifact(tmp_path: Path) -> None:
    def replace_sdist(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0.tar.gz").write_text("not a tarball", encoding="utf-8")

    repo_root, dist_dir = _fixture(tmp_path, replace_sdist)

    _assert_distribution_failure(repo_root, dist_dir, "invalid sdist artifact")


def test_distribution_check_fails_missing_sdist_package_data(tmp_path: Path) -> None:
    def replace_sdist(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0.tar.gz").unlink()
        _build_sdist(dist_dir, include_package_migration=False)

    repo_root, dist_dir = _fixture(tmp_path, replace_sdist)

    _assert_distribution_failure(repo_root, dist_dir, "sdist package migration mismatch")


def test_distribution_check_fails_invalid_sdist_package_migration_data(tmp_path: Path) -> None:
    def replace_sdist(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0.tar.gz").unlink()
        _build_sdist(dist_dir, package_migration_content=b"CREATE TABLE wrong(id INTEGER);\n")

    repo_root, dist_dir = _fixture(tmp_path, replace_sdist)

    _assert_distribution_failure(repo_root, dist_dir, "sdist package migration content mismatch")


def test_distribution_check_fails_invalid_sdist_root_migration_data(tmp_path: Path) -> None:
    def replace_sdist(dist_dir: Path) -> None:
        (dist_dir / "mal_updater-0.0.0.tar.gz").unlink()
        _build_sdist(dist_dir, root_migration_content=b"CREATE TABLE wrong(id INTEGER);\n")

    repo_root, dist_dir = _fixture(tmp_path, replace_sdist)

    _assert_distribution_failure(repo_root, dist_dir, "sdist root migration content mismatch")


def test_distribution_cli_accepts_repo_root_for_temp_fixture(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo_root, dist_dir = _fixture(tmp_path)

    assert check_distribution.main([str(dist_dir), "--repo-root", str(repo_root)]) == 0

    assert "wheel entry point" in capsys.readouterr().out


def test_current_manifest_preserves_root_migrations_for_source_invocation_compatibility() -> None:
    manifest = REPO_ROOT / "MANIFEST.in"

    assert "include migrations/*.sql" in manifest.read_text(encoding="utf-8")


def test_fixture_helper_does_not_depend_on_build_backend(tmp_path: Path) -> None:
    repo_root, dist_dir = _fixture(tmp_path)

    assert sorted(path.name for path in dist_dir.iterdir()) == [
        "mal_updater-0.0.0-py3-none-any.whl",
        "mal_updater-0.0.0.tar.gz",
    ]
    assert not shutil.which("definitely-not-a-real-build-tool")
