# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
- Add `init_logger_advanced` in `helpers/logging.py` module.
- Add in the general validation functions from cprices-utils.
- Add `invalidate_impala_metadata` function to the `cdsw/impala.py` module.
- Add "search" Plugin and mkdocs GOV UK Theme via `mkdocs-tech-docs-template`.
- Add `pipeline_runlog.py` and `hdfs_utils.py` modules from `epds_utils`.
- Add common custom exceptions.
- Add config load class
- Add generic IO input functions
- Add functions from `epds_utils` into `helpers/pyspark.py`, `io/input_hive.py`, `io/output_hive.py`.

### Changed
- Renamed `_typing` module to `typing`.
- Renamed modules in helpers directory to remove `helper_` from names.
- Relocated `logging.py` and `validation.py` to root level.

### Deprecated

### Fixed
- Fix `deploy_mkdocs.yaml`, changed `mkdocs-material` to `mkdocs-tech-docs-template`.
- Fix module paths for unit test patches in `tests/cdsw/`.

### Removed
- Remove `_version.py`.