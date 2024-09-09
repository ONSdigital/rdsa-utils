"""Common custom exceptions that can be raised in pipelines.

The purpose of these is to provide a clearer indication of why an error is
being raised over the standard builtin errors (e.g. `ColumnNotInDataframeError`
vs `ValueError`).
"""


class ColumnNotInDataframeError(Exception):
    """Custom exception to raise when a column is not present in dataframe."""

    pass


class ConfigError(Exception):
    """Custom exception to raise when there is an issue in a config object."""

    pass


class DataframeEmptyError(Exception):
    """Custom exception to raise when a dataframe is empty."""

    pass


class PipelineError(Exception):
    """Custom exception to raise when there is a generic pipeline issue."""

    pass


class TableNotFoundError(Exception):
    """Custom exception to raise when a table to be read is not found."""

    pass


class InvalidBucketNameError(Exception):
    """Custom exception to raise when an AWS S3 or GCS bucket name is invalid."""

    pass


class InvalidS3FilePathError(Exception):
    """Custom exception to raise when an AWS S3 file path is invalid."""

    pass
