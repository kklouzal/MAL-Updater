# Quality gates

CI and local quality runs use the same entrypoint:

```bash
python -m pip install -c constraints/ci.txt -e ".[dev]"
scripts/quality.sh
```

`constraints/ci.txt` pins the direct runtime/build/quality tools validated by CI. It is **not** a full transitive lock; pip still resolves transitive dependencies per Python minor/platform.

## What `scripts/quality.sh` runs

- Hermetic runtime/config setup under a unique `/tmp` directory (`MAL_UPDATER_RUNTIME_DIR`, `MAL_UPDATER_RUNTIME_ROOT`, `MAL_UPDATER_CONFIG`, and `MAL_UPDATER_SETTINGS_PATH` all point at the same isolated tree/settings file).
- Ruff critical lint gate over `conftest.py`, `src`, `tests`, and `scripts/check_distribution.py`.
- Mypy scoped type gate from `[tool.mypy]` in `pyproject.toml`.
- Full pytest suite under coverage, with source measured from `src/mal_updater` and an 80% floor.
- Isolated `python -m build --sdist --wheel`, distribution migration/package-data inspection, clean-venv wheel install, console `mal-updater --help`, and installed-wheel `mal-updater init` migration smoke.

## Distribution migration checks

The repo keeps root `migrations/*.sql` in sdists for source-tree/repo migration compatibility. Installed wheels use the packaged copy under `src/mal_updater/migrations/` as the runtime migration source.

`scripts/check_distribution.py` enforces that the root and package source migration trees have identical SQL filenames and bytes, then checks the built wheel and sdist for the same migration names and contents. Sdists must carry both root and package migration copies; wheels must carry the package migration copy and the `mal-updater = mal_updater.cli:main` console entry point.

## Known scoped-gate debt

- Default repo-wide `ruff check` is not enabled yet; the current blocking lint is limited to parse/runtime-critical rule families (`E9`, `F63`, `F7`, `F82`) because the existing tree has unused-import/unused-variable/import-order style debt.
- Repo-wide `mypy src/mal_updater` is not enabled yet; the current type gate covers the typed modules listed in `[tool.mypy].files` while broader CLI/db/recommendation modules are cleaned up incrementally.
- Coverage intentionally does not chase 100%; the current full-suite floor is 80%.
