# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Added `mkdocs-mermaid2-plugin` to the `doc` extras_require in `setup.cfg`, 
  enhancing documentation with MermaidJS diagram support.
- Enhanced `README.md` headers with relevant emojis for improved readability and engagement.

### Changed
- Modified `README.md`: Added Installation section and Git Workflow Diagram section 
  with a MermaidJS diagram.
- Updated `python_requires` in `setup.cfg` to support Python versions `>=3.8` and `<3.12`, 
  including all `3.11.x` versions.
- Modified `pull_request_workflow.yaml` to add Python `3.11` to the testing matrix.
- Moved `pyspark` from primary dependencies to `dev` section in `extras_require` to 
  streamline installation for users with pre-installed environments, 
  requiring manual installation where necessary.
- Renamed `isdir` function in `cdsw/helpers/hdfs_utils` to `is_dir` for 
  improved compliance with PEP 8 naming conventions.
- Removed line stopping existing SparkSession in `create_spark_session`
  to prevent Py4JError and enable seamless SparkContext management on GCP.
- Refactor `save_csv_to_hdfs` to use functions in `/cdsw/helpers/hdfs_utils.py`
- Add function `delete_path` in `/cdsw/helpers/hdfs_utils.py`, and refactor docstring for `delete_file` and `delete_dir`. 
  
### Deprecated

### Fixed

### Removed


## [v0.1.8] - 2024-02-28

### Added
- Added `pyproject.toml` and `setup.cfg`.

### Changed

### Deprecated

### Fixed

### Removed
- Removed `requirements.txt` now in `setup.cfg`.

## [v0.1.7] - 2024-02-28

### Added

### Changed

### Deprecated

### Fixed
- Added `build` dependency in `.github/workflows/deploy_pypi.yaml`

### Removed


## [v0.1.6] - 2024-02-28

### Added

### Changed
- Modified Workflow Trigger in `.github/workflows/deploy_pypi.yaml`

### Deprecated

### Fixed

### Removed
- Removed `.github/workflows/version_check.yaml`


## [v0.1.5] - 2024-02-28

### Added

### Changed

### Deprecated

### Fixed

- Fix GitHub Branch Reference for deployment.


## [v0.1.4] - 2024-02-28

### Added

### Changed

### Deprecated

### Fixed

- Remove check of branch for deployment.


## [v0.1.3] - 2024-02-28

### Added

### Changed

- Take workflows out of nested folder to have PyPI listing on merge to main branch.

### Deprecated

### Fixed



## [v0.1.2] - 2024-02-28

### Added

### Changed

- Workflows to have PyPI listing on merge to main branch.

### Deprecated

### Fixed


## [v0.1.1] - 2024-02-28

### Added

### Changed

### Deprecated

### Fixed

- Typo in the documentation to install Python.

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

> Note: Releases prior to v0.1.8 are not available on GitHub Releases and PyPI 
> due to bugs in the GitHub Action `deploy_pypi.yaml`, which deploys to PyPI
> and GitHub Releases.

- rdsa-utils v0.1.8: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.1.8) | 
  [PyPI](https://pypi.org/project/rdsa-utils/0.1.8/)
- rdsa-utils v0.1.7 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.6 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.5 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.4 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.3 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.2 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.1 - Not available on GitHub Releases or PyPI
- rdsa-utils v0.1.0 - Not available on GitHub Releases or PyPI
