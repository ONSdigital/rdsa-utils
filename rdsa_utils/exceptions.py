"""Common custom exceptions that can be raised in pipelines.

The purpose of these is to provide a clearer indication of why an error is
being raised over the standard builtin errors (e.g. `ColumnNotInDataframeError`
vs `ValueError`).
"""
class ColumnNotInDataframeError(Exception):
    pass


class ConfigError(Exception):
    pass


class DataframeEmptyError(Exception):
    pass


class PipelineError(Exception):
    pass


class TableNotFoundError(Exception):
    pass
