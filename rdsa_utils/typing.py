"""Contains custom types for type hinting."""

import os
import pathlib
from typing import Any, Literal, Mapping, TypeVar, Union

from pandas.core.generic import NDFrame

# Table paths are in the format "database_name.table_name".
TablePath = str

# File paths are in the format "/path/to/file/filename.ext".
FilePath = TypeVar("FilePath", str, bytes, os.PathLike, pathlib.Path)
PathLike = TypeVar("PathLike", str, bytes, os.PathLike, pathlib.Path)

# NDFrame inclues pandas series and pandas dataframes.
FrameOrSeries = TypeVar("FrameOrSeries", bound=NDFrame)

Config = Mapping[str, Any]

# See https://cloud.google.com/bigquery/docs/partitioned-tables for details on
# the use of time based partitions in Google BigQuery.
BigQueryTimePartitions = Union[Literal["hour", "day", "month", "year"], None]
