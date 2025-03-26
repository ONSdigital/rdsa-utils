# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added a function in `io/input.py` called `file_size` to check a
  file size in a local drive.

### Changed

### Deprecated

### Fixed

### Removed

## [0.8.0] 2025-03-18

### Added
- Added a function in `cdp/helpers/s3_utils.py` called `file_size` to check a
  file size in an s3 bucket.
- Added a function in `cdp/helpers/s3_utils.py` called `md5_sum` to create md5
  hash for an object in s3 bucket.
- Added a function in `cdp/helpers/s3_utils.py` called `read_header` to read
  the first line of a file in s3 bucket.
- Added a function in `cdp/helpers/s3_utils.py` called `write_string_to_file`
  to write a string into an exiting file in s3 bucket.
- Added a function in `cdp/helpers/s3_utils.py` called `s3_walk` that mimics
  the functionality of `os.walk` in s3 bucket using long filenames with slashes.

### Changed

### Deprecated

### Fixed

### Removed

## [0.7.4] 2025-03-17

### Added

### Changed

### Deprecated

### Fixed
- Fixed `setup.cfg` to include `data/*.db` files in the `pyspark_log_parser` module.

### Removed

## [0.7.3] 2025-03-17

### Added
- Added `include_package_data = True` and
  `rdsa_utils.helpers.pyspark_log_parser = *.db, *.ipynb`
  to `setup.cfg` to include `.db` and `.ipynb` files
  in the `pyspark_log_parser` module.

### Changed

### Deprecated

### Fixed

### Removed

## [0.7.2] 2025-03-17

### Added
- Added `include_package_data = True` and `* = *.db` to `setup.cfg`
  to include SQLite database files in the package.

### Changed

### Deprecated

### Fixed

### Removed

## [0.7.1] 2025-03-17

### Added
- Added `__init__.py` to `helpers/pyspark_log_parser`.

### Changed

### Deprecated

### Fixed

### Removed

## [0.7.0] 2025-03-17

### Added
- Added `pyspark_log_parser/` module in `helpers/`.
- Added `papermill`, `nbconvert`, `matplotlib` dependencies.

### Changed
- Added `multi_line` param to `load_json` in `cdp/helpers/s3_utils`.
- Removed trailing whitespaces from `CHANGELOG.md`.

### Deprecated

### Fixed

### Removed

## [0.6.0] - 2025-01-29

### Added
- Added `time_it`, `setdiff`, `flatten_iterable`, `convert_types_iterable`,
  `interleave_iterables`, `pairwise_iterable`, `merge_multi_dfs` to `helpers/python.py`.
- Added `cache_time_df`, `count_nulls`, `aggregate_col`, `get_unique`,
  `drop_duplicates_reproducible`, `apply_col_func`, `pyspark_random_uniform`,
  `cumulative_array`, `union_mismatched_dfs`, `sum_columns`, `set_nulls`,
  `union_multi_dfs`, `join_multi_dfs`, `map_column_values` to `helpers/pyspark.py`.
- Added `codetiming` package as a dependency.
- Added `write_excel` function to `cdp/helpers/io/s3_utils.py`.
- Added `xlsxwriter` and `openpyxl` dependency due to `write_excel` function
  in `cdp/helpers/io/s3_utils.py`.

### Changed
- Ran `ruff check . fix` on the codebase to comply with new PEP rules.
- Added rules to `ruff.toml` to ignore A005 warnings for `rdsa_utils/logging.py`
  and `rdsa_utils/typing.py`.
- Upgraded `black`, `ruff`, `gitleaks` to the latest version
  in `.pre-commit-config.yaml`.
- Removed module-level scope for `spark_session` fixture in `test_utils.py`
  to ensure test isolation.
- Updated Project Description for Python 3.12 and 3.13.
- Updated Copyright for 2025.
- Added acknowledgements to colleagues from DSC and MQD in `README.md`.

### Deprecated

### Fixed

### Removed

## [0.5.0] - 2025-01-09

### Added
- Added link and description of `easy_pipeline_run` repo to `README.md`.

### Changed
- Modified `list_files` function in `cdp/helpers/s3_utils.py` to use pagination
  when listing objects from S3 buckets, improving handling of large buckets.
- Added test cases for new pagination functionality in `list_files` function
  in `tests/cdp/helpers/test_s3_utils.py`.

### Deprecated

### Fixed

### Removed

## [0.4.4] - 2024-12-13

### Added

### Changed
- Modified `insert_df_to_hive_table` function in `cdp/io/output.py`. Added support
  for creating non-existent Hive tables, repartitioning by column or partition count,
  and handling missing columns with explicit type casting.

### Deprecated

### Fixed

### Removed

## [0.4.3] - 2024-12-05

### Added

### Changed
- Update `CODEOWNERS` file, changed email to GitHub username.

### Deprecated

### Fixed

### Removed

## [0.4.2] - 2024-11-26

### Added

### Changed
- Updated `ons-mkdocs-theme` version from `1.1.2` to `1.1.3` to fix issues with the crest
  not showing in the footer of documentation site.

### Deprecated

### Fixed

### Removed

## [0.4.1] - 2024-11-25

### Added

### Changed
- Updated the `ons-mkdocs-theme` version number in `doc` requirements in `setup.cfg`.

### Deprecated

### Fixed

### Removed

## [0.4.0] - 2024-11-21

### Added

### Changed
- Unpinned `pandas` version in `setup.cfg` to allow for more flexibility
  in dependency management.
- Removed `numpy` from `setup.cfg` as it will be installed automatically by `pandas`.

### Deprecated

### Fixed

### Removed

## [v0.3.7] - 2024-11-20

### Added
- Added `write_csv` function inside `cdp/helpers/s3_utils.py`.

### Changed

### Deprecated

### Fixed

### Removed

## [v0.3.6] - 2024-10-16

### Added

### Changed

### Deprecated

### Fixed
- Changed `cut_lineage` function inside `helpers/pyspark.py` to make it compatible
  with newer PySpark versions.

### Removed

## [v0.3.5] - 2024-10-04

### Added

### Changed
- Added "How the Project is Organised" section to `README.md`.
- Fix docstring for `test_load_json_with_encoding` in `test_s3_utils.py`.

### Deprecated

### Fixed

### Removed

## [v0.3.4] - 2024-09-30

### Added
- Added `load_json` to `s3_utils.py`.

### Changed

### Deprecated

### Fixed

### Removed

## [v0.3.3] - 2024-09-10

### Added
- Added `InvalidS3FilePathError` to `exceptions.py`.
- Added `validate_s3_file_path` to `s3_utils.py`.

### Changed
- Fixed docstring for `load_csv` in `helpers/pyspark.py`.
- Call `validate_s3_file_path` function inside `save_csv_to_s3`.
- Call `validate_bucket_name` and `validate_s3_file_path` function
  inside `cdp/helpers/s3_utils/load_csv`.

### Deprecated

### Fixed
- Improved `truncate_external_hive_table` to handle both partitioned and
  non-partitioned Hive tables, with enhanced error handling and support
  for table identifiers in `<database>.<table>` or `<table>` formats.

### Removed

## [v0.3.2] - 2024-09-02

### Added
- Added `load_csv` to `helpers/pyspark.py` with kwargs parameter.
- Added `truncate_external_hive_table` to `helpers/pyspark.py`.
- Added `get_tables_in_database` to `cdp/io/input.py`.
- Added `load_csv` to `cdp/helpers/s3_utils.py`. This loads a CSV from S3 bucket
  into a Pandas DataFrame.

### Changed
- Removed `.config("spark.shuffle.service.enabled", "true")`
  from `create_spark_session()` not compatible with CDP. Added
  `.config("spark.dynamicAllocation.shuffleTracking.enabled", "true")` &
  `.config("spark.sql.adaptive.enabled", "true")`.
- Change `mkdocs` theme from `mkdocs-tech-docs-template` to `ons-mkdocs-theme`.
- Added more parameters to `load_and_validate_table()` in `cdp/io/input.py`.

### Deprecated

### Fixed
- Temporarily pin `numpy==1.24.4` due to https://github.com/numpy/numpy/issues/267100

### Removed

## [v0.3.1] - 2024-05-24

### Added
- Added `zip_folder` function to `io/output.py`.

### Changed
- Modified `gcp_utils.py`, added more helper functions for GCS.
- Modified docstring for `InvalidBucketNameError` in `exceptions.py`.

### Deprecated

### Fixed

### Removed

## [v0.3.0] - 2024-05-20

### Added
- Added `.isort.cfg` to configure `isort` with the `black` profile
  and recognize `rdsa-utils` as a local repository.
- Reformatted the entire codebase using `black` and `isort`.

### Changed
- Updated `.pre-commit-config.yaml` to include `black` and `isort`
  as pre-commit hooks for code formatting.
- Updated `setup.cfg` to include `black` and `isort` in the `dev` requirements.
- Updated `README.md` to include `black` formatting badge.
- Updated `ruff.toml` to align with `black`'s formatting rules.

### Deprecated

### Fixed

### Removed

## [v0.2.3] - 2024-05-20

### Added
- Added `save_csv_to_s3` function in `cdp/io/output.py`.

### Changed
- Modified docstrings in `cdp/helpers/s3_utils.py`; remove type-hints
  from docstrings, type-hints already in function signatures.
- Add Examples section in `delete_folder` function in `s3_utils.py`.
- Modified docstrings in `cdp/io/input.py` & `cdp/io/output.py`; remove
  type-hints from docstrings, type-hints already in function signatures.
- Updated `.gitignore` to exclude `metastore_db/` directory.
- Standardised parameter names for consistency across
  S3 utility functions `s3_utils.py`

### Deprecated

### Fixed

### Removed

## [v0.2.2] - 2024-05-14

### Added
- Added `s3_utils.py` module located in `cdp/helpers/`.

### Changed
- Updated `reference.md`; included `s3_utils.py`.
- Updated `README.md`; added Ruff and Python versions badges.

### Deprecated

### Fixed

### Removed

## [v0.2.1] - 2024-05-10

### Added

### Changed
- Revised the "Further Reading on Reproducible Analytical Pipelines" section
  in the `README.md` for clarity.

### Deprecated

### Fixed

### Removed

## [v0.2.0] - 2024-05-10

### Added

### Changed
- **Breaking Change**: Renamed module `cdsw` to `cdp` (Cloudera Data Platform).
- Added a "Further Reading on Reproducible Analytical Pipelines" section to `README.md`
  to enhance resources on RAP best practices.
- Added section on synchronising the `development` branch with `main` to
  the `branch_and_deploy_guide.md` file.

### Deprecated

### Fixed
- Updated `contribution_guide.md`; fix code block rendering issue in `mkdocs` by
  removing extra whitespaces.

### Removed

## [v0.1.10] - 2024-05-08

### Added
- Updated `branch_and_deploy_guide.md`, added section titled:
  "Merging Development to Main: A Guide for Maintainers"

### Changed
- Updated `README.md` to include new badges for Deployment Status and PyPI version.

### Deprecated

### Fixed

### Removed

## [v0.1.9] - 2024-04-03

### Added
- Added `mkdocs-mermaid2-plugin` to the `doc` extras_require in `setup.cfg`,
  enhancing documentation with MermaidJS diagram support.
- Added `gitleaks` and local `restrict-filenames` hooks to `.pre-commit-config.yaml`.
- Enhanced `README.md` headers with relevant emojis for improved readability and engagement.

### Changed
- Modified `README.md`: Added Installation section and Git Workflow Diagram section
  with a MermaidJS diagram.
- Improved the `branch_and_deploy_guide.md` and `contribution_guide.md`
  documentation on branching strategy.
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
- Modified `CHANGELOG.md` added note on missing `pre-v0.1.8` releases due to `deploy_pypi.yaml` issues

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

- rdsa-utils v0.8.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.8.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.8.0/)
- rdsa-utils v0.7.4: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.7.4) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.7.4/)
- rdsa-utils v0.7.3: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.7.3) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.7.3/)
- rdsa-utils v0.7.2: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.7.2) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.7.2/)
- rdsa-utils v0.7.1: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.7.1) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.7.1/)
- rdsa-utils v0.7.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.7.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.7.0/)
- rdsa-utils v0.6.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.6.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.6.0/)
- rdsa-utils v0.5.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.5.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.5.0/)
- rdsa-utils v0.4.4: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.4.4) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.4.4/)
- rdsa-utils v0.4.3: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.4.3) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.4.3/)
- rdsa-utils v0.4.2: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.4.2) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.4.2/)
- rdsa-utils v0.4.1: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.4.1) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.4.1/)
- rdsa-utils v0.4.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.4.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.4.0/)
- rdsa-utils v0.3.7: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.7) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.7/)
- rdsa-utils v0.3.6: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.6) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.6/)
- rdsa-utils v0.3.5: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.5) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.5/)
- rdsa-utils v0.3.4: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.4) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.4/)
- rdsa-utils v0.3.3: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.3) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.3/)
- rdsa-utils v0.3.2: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.2) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.2/)
- rdsa-utils v0.3.1: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.1) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.1/)
- rdsa-utils v0.3.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.3.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.3.0/)
- rdsa-utils v0.2.3: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.2.3) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.2.3/)
- rdsa-utils v0.2.2: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.2.2) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.2.2/)
- rdsa-utils v0.2.1: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.2.1) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.2.1/)
- rdsa-utils v0.2.0: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.2.0) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.2.0/)
- rdsa-utils v0.1.10: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.1.10) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.1.10/)
- rdsa-utils v0.1.9: [GitHub Release](https://github.com/ONSdigital/rdsa-utils/releases/tag/v0.1.9) |
  [PyPI](https://pypi.org/project/rdsa-utils/0.1.9/)
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
