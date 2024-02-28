# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Fixed

### Removed

## [v0.1.0] - 2024-02-28

### Added

- `parametrize_cases` and `Case` code for use in test scripts.
- Add in PR template.
- README with additional information and guidelines for contributors.
- Pull Request Workflow includes `test` job which installs Poetry and Run Tests.
- Add `.pre-commit-config.yaml` for pre-commit hooks.
- Add CODEOWNERS file to repository.
- Add mkdocs; `deploy_mkdocs.yaml` and `docs` Folder.
- Add the helpers_spark.py and test_helpers_spark.py modules from cprices-utils.
- Add logging.py and test_logging.py module from cprices-utils.
- Add the helpers_python.py and test_helpers_python.py modules from cprices-utils.
- Add averaging_methods.py and test_averaging_methods.py.
- Add `init_logger_advanced` in `helpers/logging.py` module.
- Add in the general validation functions from cprices-utils.
- Add `invalidate_impala_metadata` function to the `cdsw/impala.py` module.
- Add "search" Plugin and mkdocs GOV UK Theme via `mkdocs-tech-docs-template`.
- Add `pipeline_runlog.py` and `hdfs_utils.py` modules from `epds_utils`.
- Add common custom exceptions.
- Add config load class.
- Add generic IO input functions.
- Add `docs/contribution_guide.md`
- Add functions from `epds_utils` into `helpers/pyspark.py`, `io/input.py`, `io/output.py`.
- Add various I/O functions from the io.py module in cprices-utils.
- Add modules to `docs/reference.md`
- Add mkdocs Plugins: `mkdocs-git-revision-date-localized-plugin`, `mkdocs-jupyter`.
- Add better navigation to `mkdocs.yml`.
- Add `save_csv_to_hdfs` function to `cdsw/io/output.py`.
- Add `docs/branch_and_deploy_guide.md`.
- Add `.github/workflows/deploy_pypi/version_check.yaml` and `.github/workflows/deploy_pypi/deploy_pypi.yaml`.

### Changed

- Renamed `_typing` module to `typing`.
- Renamed modules in helpers directory to remove `helper_` from names.
- Relocated `logging.py` and `validation.py` to root level.
- Relocated `Getting Started for Developers` into `docs/contribution_guide.md`.
- Migrated from `poetry` to `setup.py` for Python Code Packaging.
- Upgrade `mkdocs-tech-docs-template` to `0.1.2`.
- Moved CDSW related from `io/input.py` & `io/output.py` into `cdsw/io/input.py` & `cdsw/io/output.py`
- Pin `pytest` version `<8.0.0` due to https://github.com/TvoroG/pytest-lazy-fixture/issues/65
- Updated the license information.

### Deprecated

### Fixed

- Fix paths for `get_window_spec` in `averaging_methods.py`.
- Fix `deploy_mkdocs.yaml`, changed `mkdocs-material` to `mkdocs-tech-docs-template`.
- Fix module paths for unit test patches in `tests/cdsw/`.
- Fix `pull_request_workflow.yaml`; ensured pytest failures are accurately reported in GitHub workflow by removing `|| true` condition.
- Fix `deploy_mkdocs.yaml`, fixed Python version to `3.10`.
- Fix `deploy_mkdocs.yaml`, missing quotes for Python version.

### Removed

- Remove `_version.py`.
- Remove all references to Poetry.

### Release Links

RDSA-utils [v0.1.0](https://github.com/ONSdigital/rdsa-utils/releases/tag/0.1.0)