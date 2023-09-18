"""Contains custom types for type hinting."""
import os
import pathlib
from typing import (
    Any,
    Hashable,
    Mapping,
    Optional,
    TypeVar,
)

from pandas.core.generic import NDFrame


# Table paths are in the format "database_name.table_name".
TablePath = str

# File paths are in the format "/path/to/file/filename.ext".
FilePath = TypeVar('FilePath', str, bytes, os.PathLike, pathlib.Path)
PathLike = TypeVar('PathLike', str, bytes, os.PathLike, pathlib.Path)

# NDFrame inclues pandas series and pandas dataframes.
FrameOrSeries = TypeVar('FrameOrSeries', bound=NDFrame)

# Home unknown.
Config = Mapping[str, Any]
Label = Optional[Hashable]
