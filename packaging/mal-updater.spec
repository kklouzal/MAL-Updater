# PyInstaller spec for the MAL-Updater desktop app: pyinstaller packaging/mal-updater.spec
# Builds a one-folder app (faster start than one-file): dist/MAL-Updater/ on Windows
# and Linux, dist/MAL-Updater.app on macOS. Build on the platform you are targeting.
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

root = Path(SPECPATH).parent
icon = root / "src" / "mal_updater" / "assets" / "icon.png"

analysis = Analysis(
    [str(root / "packaging" / "desktop_entry.py")],
    pathex=[str(root / "src")],
    datas=collect_data_files("mal_updater"),
    # Tasks import some modules lazily by name; ship the whole package.
    hiddenimports=collect_submodules("mal_updater"),
    excludes=["pytest", "mypy", "ruff", "coverage"],
)
pyz = PYZ(analysis.pure)
exe = EXE(
    pyz,
    analysis.scripts,
    [],
    exclude_binaries=True,
    name="MAL-Updater",
    console=False,  # the app shows its own window; tasks run without console pop-ups
    icon=[str(icon)] if sys.platform in {"win32", "darwin"} else None,  # Linux executables carry no icon
)
collected = COLLECT(exe, analysis.binaries, analysis.datas, name="MAL-Updater")
if sys.platform == "darwin":
    app = BUNDLE(
        collected,
        name="MAL-Updater.app",
        icon=str(icon),
        bundle_identifier="io.github.kklouzal.mal-updater",
        info_plist={"NSHighResolutionCapable": True, "LSApplicationCategoryType": "public.app-category.utilities"},
    )
