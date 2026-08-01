from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath


EXPECTED_ENTRY_POINT = "mal-updater = mal_updater.cli:main"
EXPECTED_METADATA_FIELDS = {
    "License-Expression": "MIT",
    "License-File": "LICENSE",
    "Requires-Python": ">=3.10",
}
EXPECTED_PROJECT_URLS = {
    "Homepage, https://github.com/kklouzal/MAL-Updater",
    "Repository, https://github.com/kklouzal/MAL-Updater",
    "Issues, https://github.com/kklouzal/MAL-Updater/issues",
    "Documentation, https://github.com/kklouzal/MAL-Updater#readme",
}
EXPECTED_CLASSIFIERS = {
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Operating System :: POSIX :: Linux",
}
EXPECTED_KEYWORDS = {"anime", "myanimelist", "mal", "crunchyroll", "hidive", "openclaw", "recommendations"}


def _die(message: str) -> None:
    raise SystemExit(f"distribution check failed: {message}")


def _sql_map(directory: Path) -> dict[str, Path]:
    return {path.name: path for path in sorted(directory.glob("*.sql"))}


def _check_source_migration_parity(repo_root: Path) -> dict[str, bytes]:
    root_migrations = _sql_map(repo_root / "migrations")
    package_migrations = _sql_map(repo_root / "src" / "mal_updater" / "migrations")
    if not root_migrations:
        _die("no root migrations/*.sql files found")
    if set(root_migrations) != set(package_migrations):
        missing_in_package = sorted(set(root_migrations) - set(package_migrations))
        missing_in_root = sorted(set(package_migrations) - set(root_migrations))
        _die(f"source migration name mismatch; missing_in_package={missing_in_package}; missing_in_root={missing_in_root}")
    mismatched = [name for name, path in root_migrations.items() if path.read_bytes() != package_migrations[name].read_bytes()]
    if mismatched:
        _die(f"source migration content mismatch: {mismatched}")
    return {name: root_migrations[name].read_bytes() for name in sorted(root_migrations)}


def _single_artifact(dist_dir: Path, pattern: str) -> Path:
    artifacts = sorted(dist_dir.glob(pattern))
    if len(artifacts) != 1:
        _die(f"expected exactly one {pattern} artifact in {dist_dir}, found {len(artifacts)}")
    return artifacts[0]


def _check_sql_member_names(actual: dict[str, str], expected_sql: list[str], *, label: str, artifact_name: str) -> None:
    actual_sql = sorted(actual)
    if actual_sql != expected_sql:
        _die(f"{label} migration mismatch in {artifact_name}; expected={expected_sql}; actual={actual_sql}")


def _check_sql_member_contents(
    read_member: Callable[[str], bytes],
    members_by_sql_name: dict[str, str],
    expected_sql_bytes: dict[str, bytes],
    *,
    label: str,
    artifact_name: str,
) -> None:
    mismatched = [name for name, member in members_by_sql_name.items() if read_member(member) != expected_sql_bytes[name]]
    if mismatched:
        _die(f"{label} migration content mismatch in {artifact_name}: {sorted(mismatched)}")


def _metadata_values(metadata_text: str, field_name: str) -> list[str]:
    prefix = f"{field_name}:"
    return [line[len(prefix) :].strip() for line in metadata_text.splitlines() if line.startswith(prefix)]


def _check_metadata(metadata_text: str, *, artifact_name: str) -> None:
    for field_name, expected_value in EXPECTED_METADATA_FIELDS.items():
        values = _metadata_values(metadata_text, field_name)
        if expected_value not in values:
            _die(f"missing metadata field in {artifact_name}: {field_name}: {expected_value}")
    project_urls = set(_metadata_values(metadata_text, "Project-URL"))
    missing_urls = sorted(EXPECTED_PROJECT_URLS - project_urls)
    if missing_urls:
        _die(f"missing metadata project URLs in {artifact_name}: {missing_urls}")
    classifiers = set(_metadata_values(metadata_text, "Classifier"))
    missing_classifiers = sorted(EXPECTED_CLASSIFIERS - classifiers)
    if missing_classifiers:
        _die(f"missing metadata classifiers in {artifact_name}: {missing_classifiers}")
    keyword_values = _metadata_values(metadata_text, "Keywords")
    keywords = {part.strip().lower() for value in keyword_values for part in value.split(",") if part.strip()}
    missing_keywords = sorted(EXPECTED_KEYWORDS - keywords)
    if missing_keywords:
        _die(f"missing metadata keywords in {artifact_name}: {missing_keywords}")


def _check_wheel(wheel_path: Path, expected_sql_bytes: dict[str, bytes]) -> None:
    expected_sql = sorted(expected_sql_bytes)
    try:
        wheel_context = zipfile.ZipFile(wheel_path)
    except zipfile.BadZipFile as exc:
        _die(f"invalid wheel artifact {wheel_path.name}: {exc}")
    with wheel_context as wheel:
        names = wheel.namelist()
        wheel_sql_members = {
            PurePosixPath(name).name: name
            for name in names
            if name.startswith("mal_updater/migrations/") and name.endswith(".sql")
        }
        _check_sql_member_names(wheel_sql_members, expected_sql, label="wheel", artifact_name=wheel_path.name)
        _check_sql_member_contents(
            wheel.read,
            wheel_sql_members,
            expected_sql_bytes,
            label="wheel",
            artifact_name=wheel_path.name,
        )
        entry_point_files = [name for name in names if name.endswith(".dist-info/entry_points.txt")]
        if len(entry_point_files) != 1:
            _die(f"expected exactly one wheel entry_points.txt, found {len(entry_point_files)}")
        entry_points = wheel.read(entry_point_files[0]).decode("utf-8")
        if EXPECTED_ENTRY_POINT not in entry_points:
            _die(f"missing console script entry point {EXPECTED_ENTRY_POINT!r} in wheel")
        metadata_files = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_files) != 1:
            _die(f"expected exactly one wheel METADATA, found {len(metadata_files)}")
        license_files = [name for name in names if name.endswith(".dist-info/licenses/LICENSE")]
        if len(license_files) != 1:
            _die(f"expected exactly one wheel dist-info/licenses/LICENSE, found {len(license_files)}")
        _check_metadata(wheel.read(metadata_files[0]).decode("utf-8"), artifact_name=wheel_path.name)


def _tar_member_bytes(sdist: tarfile.TarFile, member_name: str) -> bytes:
    extracted = sdist.extractfile(member_name)
    if extracted is None:
        _die(f"sdist member is not a regular file: {member_name}")
    with extracted:
        return extracted.read()


def _sdist_sql_member_maps(names: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    package_sql_members: dict[str, str] = {}
    root_sql_members: dict[str, str] = {}
    for name in names:
        path = PurePosixPath(name)
        if not name.endswith(".sql"):
            continue
        if len(path.parts) >= 5 and path.parts[1:4] == ("src", "mal_updater", "migrations"):
            package_sql_members[path.name] = name
        if len(path.parts) >= 3 and path.parts[1] == "migrations":
            root_sql_members[path.name] = name
    return package_sql_members, root_sql_members


def _check_sdist(sdist_path: Path, expected_sql_bytes: dict[str, bytes]) -> None:
    expected_sql = sorted(expected_sql_bytes)
    try:
        sdist_context = tarfile.open(sdist_path, "r:gz")
    except tarfile.TarError as exc:
        _die(f"invalid sdist artifact {sdist_path.name}: {exc}")
    with sdist_context as sdist:
        names = sdist.getnames()
        package_sql_members, root_sql_members = _sdist_sql_member_maps(names)
        _check_sql_member_names(package_sql_members, expected_sql, label="sdist package", artifact_name=sdist_path.name)
        _check_sql_member_names(root_sql_members, expected_sql, label="sdist root", artifact_name=sdist_path.name)
        _check_sql_member_contents(
            lambda member: _tar_member_bytes(sdist, member),
            package_sql_members,
            expected_sql_bytes,
            label="sdist package",
            artifact_name=sdist_path.name,
        )
        _check_sql_member_contents(
            lambda member: _tar_member_bytes(sdist, member),
            root_sql_members,
            expected_sql_bytes,
            label="sdist root",
            artifact_name=sdist_path.name,
        )
        pyproject_files = [name for name in names if PurePosixPath(name).name == "pyproject.toml"]
        if len(pyproject_files) != 1:
            _die(f"expected exactly one pyproject.toml in sdist, found {len(pyproject_files)}")
        license_files = [name for name in names if len(PurePosixPath(name).parts) == 2 and PurePosixPath(name).name == "LICENSE"]
        if len(license_files) != 1:
            _die(f"expected exactly one top-level LICENSE in sdist, found {len(license_files)}")
        pkg_info_files = [name for name in names if len(PurePosixPath(name).parts) == 2 and PurePosixPath(name).name == "PKG-INFO"]
        if len(pkg_info_files) != 1:
            _die(f"expected exactly one PKG-INFO in sdist, found {len(pkg_info_files)}")
        _check_metadata(_tar_member_bytes(sdist, pkg_info_files[0]).decode("utf-8"), artifact_name=sdist_path.name)


def check_distribution(repo_root: Path, dist_dir: Path) -> int:
    if not dist_dir.is_dir():
        _die(f"dist directory does not exist: {dist_dir}")

    expected_sql_bytes = _check_source_migration_parity(repo_root)
    wheel_path = _single_artifact(dist_dir, "*.whl")
    sdist_path = _single_artifact(dist_dir, "*.tar.gz")
    _check_wheel(wheel_path, expected_sql_bytes)
    _check_sdist(sdist_path, expected_sql_bytes)

    sql_count = len(expected_sql_bytes)
    print(f"source migration parity: {sql_count} SQL files")
    print(f"wheel migrations: {wheel_path.name}: {sql_count} SQL files")
    print(f"sdist package/root migrations: {sdist_path.name}: {sql_count} SQL files each")
    print("wheel entry point: mal-updater -> mal_updater.cli:main")
    print("package metadata: MIT license expression/file, project URLs, classifiers, and keywords")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify MAL-Updater sdist/wheel package data parity.")
    parser.add_argument("dist_dir", nargs="?", default="dist", help="directory containing exactly one .tar.gz and one .whl")
    parser.add_argument(
        "--repo-root",
        default=Path(__file__).resolve().parents[1],
        type=Path,
        help="repository/source fixture root (default: parent of scripts/)",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    dist_arg = Path(args.dist_dir)
    dist_dir = dist_arg.resolve() if dist_arg.is_absolute() else (repo_root / dist_arg).resolve()
    return check_distribution(repo_root, dist_dir)


if __name__ == "__main__":
    sys.exit(main())
