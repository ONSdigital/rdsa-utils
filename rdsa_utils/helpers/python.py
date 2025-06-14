"""Miscellaneous helper functions for Python."""

import hashlib
import itertools
import json
import logging
import subprocess
from datetime import datetime, time
from functools import reduce, wraps
from itertools import tee
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Tuple, Union

import pandas as pd
from codetiming import Timer
from more_itertools import always_iterable
from pandas.tseries.offsets import MonthEnd

logger = logging.getLogger(__name__)


def always_iterable_local(obj: Any) -> Callable:
    """Supplement more-itertools `always_iterable` to also exclude dicts.

    By default it would convert a dictionary to an iterable of just its keys,
    dropping all the values. This change makes it so dictionaries are not
    altered (similar to how strings aren't broken down).
    """
    return always_iterable(obj, base_type=(str, bytes, dict))


def tuple_convert(obj: Any) -> Tuple[Any]:
    """Convert object to tuple using more-itertools' `always_iterable`."""
    return tuple(always_iterable_local(obj))


def list_convert(obj: Any) -> List[Any]:
    """Convert object to list using more-itertools' `always_iterable`."""
    return list(always_iterable_local(obj))


def extend_lists(
    sections: List[List[str]],
    elements_to_add: List[str],
) -> None:
    """Check list elements are unique then append to existing list.

    Note the `.extend` method in Python overwrites each section. There
    is no need to assign a variable to this function, the section will
    update automatically.

    The function can be used with the `load_config` function to extend a
    value list in a config yaml file. For example with a `config.yaml`
    file as per below:

    ```
    input_columns
        - col_a
        - col_b

    output_columns
        - col_b
    ```

    To add column `col_c` the function can be utilised as follows:

    ```
    config = load_config("config.yaml")

    sections = [config['input_columns'], config['output_columns']]
    elements_to_add = ['col_c']

    extend_lists(sections, elements_to_add)
    ```

    The output will be as follows.

    ```
    input_columns
        - col_a
        - col_b
        - col_c

    output_columns
        - col_b
        - col_c
    ```

    Parameters
    ----------
    sections
        The section to be updated with the extra elements.
    elements_to_add
        The new elements to add to the specified sections.

    Returns
    -------
    None
        Note the `.extend` method in Python overwrites the sections.
        There is no need to assign a variable to this function, the
        section will update automatically.
    """
    for section in sections:
        missing_elements = [
            element for element in elements_to_add if element not in section
        ]
        section.extend(missing_elements)

    return None


def overwrite_dictionary(
    base_dict: Mapping[str, Any],
    override_dict: Mapping[str, Any],
) -> Dict[str, Any]:
    """Overwrite dictionary values with user defined values.

    The following restrictions are in place:
    * base_dict and override_dict have the same value which is not dictionary
      then override_dict has priority.
    * If base_dict contains dictionary and override_dict contains a value (e.g.
      string or list) with the same key, priority is upon base_dict and
      the override_dict value is ignored.
    * If key is in override_dict but not in base_dict then an Exception is
      raised and code stops.
    * Any other restrictions will require code changes.

    Parameters
    ----------
    base_dict
        Dictionary containing existing key value pairs.
    override_dict
        Dictionary containing new keys/values to inset into base_dict.

    Returns
    -------
    Dict[str, Any]
        The base_dict with any keys matching the override_dict being replaced.
        Any keys not present in base_dict are appended.

    Example
    -------
    >>> dic1 = {"var1": "value1", "var2": {"var3": 1.1, "var4": 4.4}, "var5": [1, 2, 3]}
    >>> dic2 = {"var2": {"var3": 9.9}}

    >>> overwrite_dictionary(dic1, dic2)
    {'var1': 'value1', 'var2': {'var3': 9.9, 'var4': 4.4}, 'var5': [1, 2, 3]}

    >>> dic3 = {"var2": {"var3": 9.9}, "var6": -1}
    >>> overwrite_dictionary(dic1, dic3)
    ERROR __main__: ('var6', -1) not in base_dict

    Notes
    -----
    Modified from: https://stackoverflow.com/a/58742155

    Warning
    -------
    Due to recursive nature of function, the function will overwrite the
    base_dict object that is passed into the original function.

    Raises
    ------
    ValueError
        If a key is present in override_dict but not base_dict.
    """  # noqa: E501
    for key, val in base_dict.items():
        if type(val) == dict:
            if key in override_dict and type(override_dict[key]) == dict:
                overwrite_dictionary(base_dict[key], override_dict[key])
            elif key in override_dict and type(override_dict[key]) != dict:
                logger.warning(
                    f"""
                Not overriding key: {key} in base dictionary as the value type
                for the base dictionary are of higher priority than the
                override.

                Base dictionary values for key are of type:
                {type(base_dict[key])}
                and have values:
                {base_dict[key]}

                Override dictionary values for key are of type:
                {type(override_dict[key])}
                and have values:
                {override_dict[key]}
                """,
                )
            else:
                # Key in base_dict not present in override_dict so do nothing.
                pass
        else:
            if key in override_dict:
                base_dict[key] = override_dict[key]
            else:
                # Key in base_dict not present in override_dict so do nothing.
                pass

    for key, val in override_dict.items():
        if key not in base_dict:
            msg = f"""
            The key, value pair:
            {key, val}
            is not in the base dictionary
            {json.dumps(base_dict, indent=4)}
            """
            logger.error(msg)
            raise ValueError(msg)

    return base_dict


def calc_product_of_dict_values(
    **kwargs: Mapping[str, Union[str, float, Iterable]],
) -> Mapping[str, any]:
    """Create cartesian product of values for each kwarg.

    In order to create product of values, the values are converted to
    a list so that product of values can be derived.

    Yields
    ------
        Next result of cartesian product of kwargs values.

    Example
    -------
    my_dict = {
        'key1': 1,
        'key2': [2, 3, 4]
    }

    list(calc_product_of_dict_values(**my_dict))
    >>> [{'key1': 1, 'key2': 2}, {'key1': 1, 'key2': 3}, {'key1': 1, 'key2': 4}]

    Notes
    -----
    Modified from: https://stackoverflow.com/a/5228294
    """
    # noqa: E501
    kwargs = {key: list_convert(value) for key, value in kwargs.items()}

    keys = kwargs.keys()
    vals = kwargs.values()

    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))  # noqa: B905


def convert_date_strings_to_datetimes(
    start_date: str,
    end_date: str,
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Convert start and end dates from strings to timestamps.

    Parameters
    ----------
    start_date
        Datetime like object which is used to define the start date for filter.
        Acceptable string formats include (but not limited to): MMMM YYYY,
        YYYY-MM, YYYY-MM-DD, DD MMM YYYY etc. If only month and year specified
        the start_date is set as first day of month.
    end_date
        Datetime like object which is used to define the start date for filter.
        Acceptable string formats include (but not limited to): MMMM YYYY,
        YYYY-MM, YYYY-MM-DD, DD MMM YYYY etc. If only month and year specified
        the end_date is set as final day of month.

    Returns
    -------
    tuple[pd.Timestamp, pd.Timestamp]
        Tuple where the first value is the start date and the second the end
        date.
    """
    shift_end_date_to_month_end = False

    year_month_formats = [
        "%B %Y",  # January 2020
        "%b %Y",  # Jan 2020
        "%Y %B",  # 2020 January
        "%Y %b",  # 2020 Jan
        # '%Y-%m',  # 2020-01 - also matches 2020-01-01
        # '%Y-%-m',  # 2020-1 - also matches 2020-01-01
        # '%Y %m',  # 2020 01 - also matches 2020-01-01
        # '%Y %-m',  # 2020 1 - also matches 2020-01-01
        "%m-%Y",  # 01-2020
        "%-m-%Y",  # 1-2020
        "%m %Y",  # 01 2020
        "%-m %Y",  # 1 2020
    ]

    # if the end_date format matches one of the above then it is assumed the
    # used wants to use all days in that month.
    for date_format in year_month_formats:
        try:
            pd.to_datetime(end_date, format=date_format)
            shift_end_date_to_month_end = True

        except ValueError:
            pass

    if shift_end_date_to_month_end:
        end_date = pd.to_datetime(end_date) + MonthEnd(0)

    # Obtain the last "moment" of the end_date to ensure any hourly data for
    # the date is included
    # https://medium.com/@jorlugaqui/how-to-get-the-latest-earliest-moment-from-a-day-in-python-aa8999bea945  # noqa: E501
    end_date = datetime.combine(pd.to_datetime(end_date), time.max)

    # Ensure dates are timestamp to enable inclusive filtering of provided end
    # date, see https://stackoverflow.com/a/43403904 for info.
    return (pd.Timestamp(start_date), pd.Timestamp(end_date))


def time_it(*timer_args, **timer_kwargs) -> Callable:
    """Measure the execution time of a function, with options to configure Timer.

    Parameters
    ----------
    timer_args
        Positional arguments to pass to the Timer object.
    timer_kwargs
        Keyword arguments to pass to the Timer object.

    Returns
    -------
    Callable
        A wrapped function that includes timing measurement.

    Example
    -------
    @time_it()
    def example_function():
        # Function implementation
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrap(*args, **kwargs) -> Any:
            with Timer(*timer_args, **timer_kwargs) as t:
                result = func(*args, **kwargs)
            logger.info(f"<Executed {func.__name__} in {t.last:.2f} seconds>")
            return result

        return wrap

    return decorator


def setdiff(a: Iterable, b: Iterable) -> List[Any]:
    """Return a list of elements that are present in `a` but not in `b`.

    Parameters
    ----------
    a
        The first iterable from which elements are to be selected.
    b
        The second iterable containing elements to be excluded.

    Returns
    -------
    list
        A list of elements that are in `a` but not in `b`.

    Examples
    --------
    >>> setdiff([1, 2, 3, 4], [3, 4, 5, 6])
    [1, 2]
    >>> setdiff('abcdef', 'bdf')
    ['a', 'c', 'e']
    >>> setdiff({1, 2, 3}, {2, 3, 4})
    [1]
    >>> setdiff(range(5), range(2, 7))
    [0, 1]
    """
    if not isinstance(a, Iterable) or not isinstance(b, Iterable):
        msg = "Both inputs must be iterable."
        raise TypeError(msg)

    return list(set(a) - set(b))


def flatten_iterable(
    iterable: Iterable,
    types_to_flatten: Union[type, Tuple] = (list, tuple),
) -> List:
    """Flatten an iterable.

    Parameters
    ----------
    iterable
        An iterable that may contain elements of various types.
    types_to_flatten
        Data type(s) that should be flattened. Defaults to (list, tuple).

    Returns
    -------
    list
        A flattened list with all elements from the input iterable, with
        specified types unpacked.

    Examples
    --------
    >>> flatten_iterable([1, [2, 3], (4, 5), 'abc'])
    [1, 2, 3, 4, 5, 'abc']
    >>> flatten_iterable([1, [2, 3], (4, 5), 'abc'], types_to_flatten=list)
    [1, 2, 3, (4, 5), 'abc']
    >>> flatten_iterable(['a', 'bc', ['d', 'e']], types_to_flatten=str)
    ['a', 'b', 'c', 'd', 'e']
    >>> flatten_iterable((1, [2, 3], (4, 5), 'abc'), types_to_flatten=(list, tuple))
    (1, 2, 3, 4, 5, 'abc')
    """
    if not hasattr(iterable, "__iter__"):
        msg = "`iterable` must be an iterable."
        raise TypeError(msg)

    if not isinstance(types_to_flatten, (type, tuple)):
        msg = "`types_to_flatten` must be a type or a tuple of types."
        raise TypeError(msg)

    if isinstance(types_to_flatten, tuple):
        if not all(isinstance(t, type) for t in types_to_flatten):
            msg = "All elements in `types_to_flatten` must be types."
            raise ValueError(msg)
    else:
        if not isinstance(types_to_flatten, type):
            msg = "`types_to_flatten` must be a type or a tuple of types."
            raise TypeError(msg)

    flattened = []
    for item in iterable:
        if isinstance(item, types_to_flatten):
            flattened.extend(item)
        else:
            flattened.append(item)
    return flattened


def convert_types_iterable(lst: Iterable, dtype: type = float) -> List:
    """Convert the data type of elements in an iterable.

    Parameters
    ----------
    lst
        The iterable whose elements are to be converted.
    dtype
        The target data type to which elements in the iterable should be
        converted. Defaults to `float`.

    Returns
    -------
    list
        A new list with elements converted to the specified data type.

    Examples
    --------
    >>> convert_types_iterable([1, 2, 3])
    [1.0, 2.0, 3.0]

    >>> convert_types_iterable((10, 20, 30), dtype=str)
    ['10', '20', '30']

    >>> convert_types_iterable({'a', 'b', 'c'}, dtype=ord)
    [97, 98, 99]

    >>> convert_types_iterable(['10', '20', '30'], dtype=int)
    [10, 20, 30]
    """
    if not isinstance(lst, (list, tuple, set, frozenset, range)):
        msg = (
            "Input must be an iterable type such as list, tuple, set, "
            "frozenset, or range."
        )
        raise TypeError(msg)

    if not isinstance(dtype, type):
        msg = "`dtype` must be a valid type."
        raise TypeError(msg)

    return list(map(dtype, lst))


def interleave_iterables(iterable1: Iterable, iterable2: Iterable) -> List:
    """Interleave two iterables element by element.

    Parameters
    ----------
    iterable1
        The first iterable to interleave.
    iterable2
        The second iterable to interleave.

    Returns
    -------
    list
        A new list with elements from `iterable1` and `iterable2` interleaved.

    Raises
    ------
    TypeError
        If either of the inputs is not an iterable of types: list, tuple,
        string, or range.
    ValueError
        If the lengths of the two iterables do not match.

    Examples
    --------
    >>> interleave_iterables([1, 2, 3], [4, 5, 6])
    [1, 4, 2, 5, 3, 6]

    >>> interleave_iterables((1, 2, 3), ('a', 'b', 'c'))
    [1, 'a', 2, 'b', 3, 'c']

    >>> interleave_iterables('ABC', '123')
    ['A', '1', 'B', '2', 'C', '3']

    >>> interleave_iterables(range(3), range(10, 13))
    [0, 10, 1, 11, 2, 12]
    """
    if not isinstance(iterable1, (list, tuple, str, range)) or not isinstance(
        iterable2,
        (list, tuple, str, range),
    ):
        msg = (
            "Both inputs must be iterable types such as list, tuple,"
            "string, or range."
        )
        raise TypeError(msg)

    if len(iterable1) != len(iterable2):
        msg = "Both iterables must have the same length."
        raise ValueError(msg)

    result = [None] * (len(iterable1) + len(iterable2))
    result[::2] = iterable1
    result[1::2] = iterable2

    return result


def pairwise_iterable(iterable: Iterable) -> zip:
    """Return pairs of adjacent values from the input iterable.

    Parameters
    ----------
    iterable
        An iterable object (e.g., list, tuple, string) from which pairs of
        adjacent values will be generated.

    Returns
    -------
    zip
        An iterator of tuples, each containing a pair of adjacent values
        from the input iterable.

    Raises
    ------
    TypeError
        If the input is not an iterable.

    Examples
    --------
    >>> list(pairwise_iterable([1, 2, 3, 4]))
    [(1, 2), (2, 3), (3, 4)]

    >>> list(pairwise_iterable('abcde'))
    [('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e')]

    >>> list(pairwise_iterable((10, 20, 30)))
    [(10, 20), (20, 30)]
    """
    if not hasattr(iterable, "__iter__"):
        msg = "Input must be an iterable."
        raise TypeError(msg)

    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def merge_multi_dfs(
    df_list: list,
    on: Union[str, list],
    how: str,
    fillna_val: Union[None, object] = None,
) -> pd.DataFrame:
    """Perform consecutive merges on a list of pandas DataFrames.

    Parameters
    ----------
    df_list
        A list of DataFrames to be merged.
    on
        Column name(s) to merge on.
    how
        Type of merge to be performed. Must be one of 'left', 'right', 'outer',
        'inner'.
    fillna_val
        Value to replace missing values with. Default is None.

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame after merging and optional filling of missing
        values.

    Raises
    ------
    TypeError
        If `df_list` is not a list of pandas DataFrames, or `on` is not a string
        or list of strings, or `how` is not a string.
    ValueError
        If the `how` argument is not one of 'left', 'right', 'outer', or 'inner'.

    Examples
    --------
    >>> import pandas as pd
    >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
    >>> df2 = pd.DataFrame({'key': ['A', 'B'], 'value2': [4, 5]})
    >>> df3 = pd.DataFrame({'key': ['A'], 'value3': [6]})
    >>> merge_multi_dfs([df1, df2, df3], on='key', how='inner')
      key  value1  value2  value3
    0   A       1       4       6

    >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
    >>> df2 = pd.DataFrame({'key': ['A', 'B'], 'value2': [4, 5]})
    >>> merge_multi_dfs([df1, df2], on='key', how='outer',  fillna_val=0)
      key  value1  value2
    0   A        1        4
    1   B        2        5
    2   C        3        0
    """
    if not isinstance(df_list, list) or not all(
        isinstance(df, pd.DataFrame) for df in df_list
    ):
        msg = "`df_list` must be a list of pandas DataFrames."
        raise TypeError(msg)

    if not isinstance(on, (str, list)):
        msg = "`on` must be a string or a list of strings."
        raise TypeError(msg)

    if not isinstance(how, str):
        msg = "`how` must be a string."
        raise TypeError(msg)

    valid_how_options = ["left", "right", "outer", "inner"]
    if how not in valid_how_options:
        msg = f"`how` Must be one of {valid_how_options}."
        raise ValueError(msg)

    merged_df = reduce(
        lambda left, right: left.merge(right, on=on, how=how),
        df_list,
    )

    if fillna_val is not None:
        merged_df = merged_df.fillna(fillna_val)

    return merged_df


def file_size(
    filepath: str,
) -> int:
    """Return the size of the file from the network drive in bytes.

    Parameters
    ----------
    filepath
        The filepath of file to check for size.

    Returns
    -------
    int
        An integer value indicating the size of the file in bytes

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    Example
    -------
    >>> file_size("folder/file.txt")
    90
    >>> file_size("folder/non_existing_file.txt")
    FileNotFoundError: filepath='.../folder/non_existing_file.txt' cannot be found.
    """
    if Path(filepath).exists():
        return Path(filepath).stat().st_size
    else:
        msg = f"{filepath=} cannot be found."
        logger.error(msg)
        raise FileNotFoundError(msg)


def md5_sum(
    filepath: str,
) -> str:
    """Get md5sum of a specific file on the local file system.

    Parameters
    ----------
    filepath
        Filepath of file to create md5 hash from.

    Returns
    -------
    str
        The md5sum of the file.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    Example
    -------
    >>> md5_sum("folder/file.txt")
    "d41d8cd98f00b204e9800998ecf8427e"
    >>> md5_sum("folder/non_existing_file.txt")
    FileNotFoundError: filepath='../folder/non_existing_file.txt' cannot be found.
    """
    if Path(filepath).exists():
        with open(filepath, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    else:
        msg = f"{filepath=} cannot be found."
        logger.error(msg)
        raise FileNotFoundError(msg)


def file_exists(
    filepath: str,
) -> bool:
    """Test if file exists on the local file system.

    Parameters
    ----------
    filepath
        Filepath of file check exists.

    Returns
    -------
    bool
        True if the file exists, else False.

    Example
    -------
    >>> file_exists("folder/file.txt")
    True
    >>> file_exists("folder/non_existing_file.txt")
    filepath='.../folder/non_existing_file.txt' cannot be found.
    False
    """
    if Path(filepath).exists():
        return Path(filepath).is_file()
    else:
        logger.warning(f"{filepath=} cannot be found.")
        return False


def directory_exists(
    dirpath: str,
) -> bool:
    """Test if given path is a directory on the local file system.

    Parameters
    ----------
    dirpath
        The directory path to check exists.

    Returns
    -------
    bool
        True if the dirpath is a directory, False otherwise.

    Example
    -------
    >>> directory_exists("folder")
    True
    >>> directory_exists("non_existing_folder")
    dirpath='.../non_existing_folder' cannot be found.
    False
    """
    if Path(dirpath).exists():
        return Path(dirpath).is_dir()
    else:
        logger.warning(f"{dirpath=} cannot be found.")
        return False


def check_file(
    filepath: str,
) -> bool:
    """Check if a file exists on the local file system and meets specific criteria.

    This function checks whether the given path corresponds to a valid file
    on the local or network file system. It ensures the file exists, is not
    a directory, and its size is greater than zero bytes.

    Parameters
    ----------
    filepath
        The path to a local/network file.

    Returns
    -------
    bool
        True if the file exists, is not a directory, and size > 0,
        otherwise False.

    Example
    -------
    >>> check_file("folder/file.txt")
    True
    >>> check_file("folder/file_0_bytes.txt")
    False
    """
    if file_exists(filepath):
        isdir = directory_exists(filepath)
        size = file_size(filepath)
        response = (not isdir) and (size > 0)
    else:
        response = False
    return response


def read_header(
    filepath: str,
) -> str:
    """Return the first line of a file on the local file system.

    Reads the first line and removes the newline/returncarriage symbol.

    Parameters
    ----------
    filepath
        The path to a local/network file.

    Returns
    -------
    str
        The first line of the file as a string.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    Example
    -------
    >>> read_header("folder/file.txt")
    "This is the first line of the file."
    >>> read_header("folder/non_existing_file.txt")
    FileNotFoundError: filepath='.../folder/non_existing_file.txt' cannot be found.
    """
    try:
        with open(filepath, "r") as f:
            first_line = f.readline()
            return first_line.rstrip("\n\r")
    except FileNotFoundError:
        msg = f"{filepath=} cannot be found."
        logger.error(msg)
        raise FileNotFoundError(msg) from None


def write_string_to_file(
    content: bytes,
    filepath: str,
) -> None:
    """Write a string into the specified file path on the local file system.

    Parameters
    ----------
    content
        The content to write into the file.
    filepath
        The path to the file where the content will be written.
        If the file already exists, it will be overwritten.

    Returns
    -------
    None

    Example
    -------
    >>> write_string_to_file(b"Hello, World!", "example.txt")
    # The content "Hello, World!" will be written to "example.txt"
    """
    with open(filepath, "wb") as f:
        f.write(content)


def create_folder(dirpath: str) -> None:
    """Create a directory on a local network drive.

    Parameters
    ----------
    dirpath
        The path to the directory to create.

    Returns
    -------
    None
        The directory will be created if it does not already exist,
        including parent directories.

    Example
    -------
    >>> create_folder("example_folder/subfolder")
    # The directory "example_folder/subfolder" will be created if it does not exist.
    """
    Path(dirpath).mkdir(parents=True, exist_ok=True)
    return None


def dump_environment_requirements(
    output_file: str,
    tool: str = "pip",
    args: List[str] = ["list", "--format=freeze"],
) -> None:
    """Dump the current Python environment dependencies to a text file.

    Parameters
    ----------
    output_file
        Path to the output text file where the list of dependencies will be saved.
        If the directory does not exist, it will be created.

    tool
        The command-line tool to use for exporting dependencies
        (e.g. 'pip', 'poetry', or 'uv').
        Default is 'pip'.

    args
        The arguments to pass to the selected tool.
        For pip, the default is ['list', '--format=freeze'].
        For poetry, a common option is ['export', '--without-hashes'].
        For uv, you might use ['pip', 'freeze'].

    Returns
    -------
    None
        This function writes to the specified file and does not return anything.

    Examples
    --------
    >>> dump_environment_requirements("requirements.txt")
    >>> dump_environment_requirements(
    ...     "requirements.txt",
    ...     tool="pip",
    ...     args=["freeze"]
    ... )
    >>> dump_environment_requirements(
    ...     "requirements.txt",
    ...     tool="poetry",
    ...     args=["export", "--without-hashes"]
    ... )
    >>> dump_environment_requirements(
    ...     "requirements.txt",
    ...     tool="uv",
    ...     args=["pip", "freeze"]
    ... )
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [tool] + args,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    logger.info(
        f"Dumping environment to '{output_path}' using tool='{tool}' "
        f"with args={args}",
    )
    output_path.write_text(result.stdout)
