# Quality gates

CI and local quality runs use the same entrypoint:

```bash
python -m pip install -c constraints/ci.txt -e ".[dev]"
scripts/quality.sh
```

`constraints/ci.txt` pins the direct runtime/build/quality tools validated by CI. It is **not** a full transitive lock; pip still resolves transitive dependencies per Python minor/platform.

## What `scripts/quality.sh` runs

- Hermetic runtime/config setup under a unique `/tmp` directory, or `MAL_UPDATER_QUALITY_TMP` when set (`MAL_UPDATER_RUNTIME_DIR`, `MAL_UPDATER_RUNTIME_ROOT`, `MAL_UPDATER_CONFIG`, and `MAL_UPDATER_SETTINGS_PATH` all point at the same isolated tree/settings file). `scripts/quality.sh --print-env` prints that setup and exits.
- Ruff lint gate (syntax errors and all pyflakes rules) over `conftest.py`, `src`, `tests`, and `scripts/check_distribution.py`.
- Mypy scoped type gate from `[tool.mypy]` in `pyproject.toml`.
- Full pytest suite under coverage, with source measured from `src/mal_updater` and an 80% floor.
- Isolated `python -m build --sdist --wheel`, distribution migration/package-data inspection, clean-venv wheel install, console `mal-updater --help`, and installed-wheel `mal-updater init` migration smoke.

## Distribution migration checks

Migrations live only in `src/mal_updater/migrations/`, the package resource the runtime loads.

`scripts/check_distribution.py` checks that the built wheel and sdist carry exactly those migration files with identical bytes, plus the console entry points and package metadata.

## Known scoped-gate debt

- Ruff enforces syntax errors plus all pyflakes rules (`E9`, `F`); style rules are not enforced.
- Repo-wide `mypy src/mal_updater` is not enabled yet; the current type gate covers the typed modules listed in `[tool.mypy].files` while broader CLI/db/recommendation modules are cleaned up incrementally.
- Coverage intentionally does not chase 100%; the current full-suite floor is 80%.
