"""Miscellaneous helper functions for Python."""

import itertools
import json
import logging
import time
from datetime import datetime, time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Tuple, Union

import pandas as pd
import numpy as np
from functools import reduce, wraps
from itertools import tee
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


def time_it(func):
    """
    A decorator that measures the execution time of a function.

    This decorator wraps a function to measure and print the time it takes 
    to execute. The execution time is printed in seconds, rounded to two 
    decimal places.

    Args:
        func (callable): The function whose execution time is to be measured.

    Returns:
        callable: A wrapped function that includes timing measurement.

    Example:
        @time_it
        def some_function():
            # Function implementation
    """
    @wraps(func)
    def wrap(*args, **kw):
        time_start = time.time()
        result = func(*args, **kw)
        time_end = time.time()
        print(f'  <Executed {func.__name__} in {round(time_end-time_start, 2)} seconds>')
        return result

    return wrap


def setdiff(a, b):
    """
    Return a list of elements that are present in `a` but not in `b`.

    This function computes the set difference between two iterables and 
    returns the resulting elements as a list. Elements that are in `b` 
    but not in `a` are excluded from the result.

    The input arguments must be iterable types that support set operations, 
    such as lists, tuples, strings, sets, dictionaries (keys only), and ranges.

    Args:
        a (iterable): The first iterable from which elements are to be selected.
        b (iterable): The second iterable containing elements to be excluded.

    Returns:
        list: A list of elements that are in `a` but not in `b`.

    Examples:
        >>> setdiff([1, 2, 3, 4], [3, 4, 5, 6])
        [1, 2]

        >>> setdiff('abcdef', 'bdf')
        ['a', 'c', 'e']

        >>> setdiff({1, 2, 3}, {2, 3, 4})
        [1]

        >>> setdiff(range(5), range(2, 7))
        [0, 1]
    """
    #--------------------------------------------------------------------------
    # Run input checks
    
    if not isinstance(a, Iterable) or not isinstance(b, Iterable):
        raise TypeError("Both inputs must be iterable.")

    #--------------------------------------------------------------------------
    # Apply function
    
    return list(set(a) - set(b))


def flatten(iterable, types_to_flatten=(list, tuple)):
    """
    Flatten an iterable containing both specified types of nested objects 
    and non-object elements.

    This function iterates through the input iterable, unpacking elements 
    that are of the types specified in `types_to_flatten` and adding non-specified 
    elements directly to the result. The resulting list contains all individual 
    elements from the input, preserving their original order.

    Args:
        iterable (iterable): An iterable that may contain elements of various types.
        types_to_flatten (type or tuple of types, optional): Data type(s) that should 
        be flattened. Elements of these types are unpacked into the result list. 
        Defaults to (list, tuple).

    Returns:
        list: A flattened list with all elements from the input iterable, with 
        specified types unpacked.

    Examples:
        >>> flatten([1, [2, 3], (4, 5), 'abc'])
        [1, 2, 3, 4, 5, 'abc']

        >>> flatten([1, [2, 3], (4, 5), 'abc'], types_to_flatten=list)
        [1, 2, 3, (4, 5), 'abc']

        >>> flatten(['a', 'bc', ['d', 'e']], types_to_flatten=str)
        ['a', 'b', 'c', 'd', 'e']

        >>> flatten((1, [2, 3], (4, 5), 'abc'), types_to_flatten=(list, tuple))
        (1, 2, 3, 4, 5, 'abc')
    """
    #--------------------------------------------------------------------------
    # Run input checks
    if not hasattr(iterable, '__iter__'):
        raise TypeError("`iterable` must be an iterable.")
    
    if not isinstance(types_to_flatten, (type, tuple)):
        raise TypeError("`types_to_flatten` must be a type or a tuple of types.")
    
    if isinstance(types_to_flatten, tuple):
        if not all(isinstance(t, type) for t in types_to_flatten):
            raise ValueError("All elements in `types_to_flatten` must be types.")
    else:
        if not isinstance(types_to_flatten, type):
            raise TypeError("`types_to_flatten` must be a type or a tuple of types.")

    #--------------------------------------------------------------------------
    # Apply function
    

    flattened = []
    for item in iterable:
        if isinstance(item, types_to_flatten):
            flattened.extend(item)
        else:
            flattened.append(item)
    return flattened


def convert_types(lst, dtype=float):
    """
    Convert the data type of elements in an iterable.

    This function takes an iterable and a target data type, converting each 
    element in the iterable to the specified data type. By default, elements 
    are converted to floats. The function is compatible with various iterable 
    types, such as lists, tuples, sets, and more.

    Args:
        lst (iterable): The iterable whose elements are to be converted.
        dtype (type, optional): The target data type to which elements in the 
        iterable should be converted. Defaults to `float`.

    Returns:
        list: A new list with elements converted to the specified data type.

    Examples:
        >>> convert_list_elements([1, 2, 3])
        [1.0, 2.0, 3.0]
        
        >>> convert_list_elements((10, 20, 30), dtype=str)
        ['10', '20', '30']
        
        >>> convert_list_elements({'a', 'b', 'c'}, dtype=ord)
        [97, 98, 99]
        
        >>> convert_list_elements(['10', '20', '30'], dtype=int)
        [10, 20, 30]
    """
    #--------------------------------------------------------------------------
    # Run input checks
    if not isinstance(lst, (list, tuple, set, frozenset, range)):
        raise TypeError(
                "Input must be an iterable type such as list, tuple, set, "
                "frozenset, or range."
        )
    
    if not isinstance(dtype, type):
        raise TypeError("`dtype` must be a valid type.")

    #--------------------------------------------------------------------------
    # Apply function
        
    return list(map(dtype, lst))


def interleave_iterables(iterable1, iterable2):
    """
    Interleave two iterables element by element.

    This function takes two iterables of equal length and interleaves their 
    elements. The first element of the result is from the first iterable, 
    the second is from the second iterable, and so on.

    The function works with various types of iterables, such as lists, 
    tuples, strings, and ranges. The resulting interleaved elements are 
    returned as a list.

    Args:
        iterable1 (iterable): The first iterable to interleave.
        iterable2 (iterable): The second iterable to interleave.

    Returns:
        list: A new list with elements from `iterable1` and `iterable2` 
        interleaved.

    Examples:
        Interleave two lists:
        >>> interleave_iterables([1, 2, 3], [4, 5, 6])
        [1, 4, 2, 5, 3, 6]

        Interleave two tuples:
        >>> interleave_iterables((1, 2, 3), ('a', 'b', 'c'))
        [1, 'a', 2, 'b', 3, 'c']

        Interleave two strings:
        >>> interleave_iterables('ABC', '123')
        ['A', '1', 'B', '2', 'C', '3']

        Interleave two ranges:
        >>> interleave_iterables(range(3), range(10, 13))
        [0, 10, 1, 11, 2, 12]
    """
    #--------------------------------------------------------------------------
    # Run input checks
    
    if not isinstance(iterable1, (list, tuple, str, range)) or \
       not isinstance(iterable2, (list, tuple, str, range)):
        raise TypeError(
            "Both inputs must be iterable types such as list, tuple, "
            "string, or range."
        )
    
    if len(iterable1) != len(iterable2):
        raise ValueError("Both iterables must have the same length.")

    #--------------------------------------------------------------------------
    # Apply function
    
    result = [None] * (len(iterable1) + len(iterable2))
    result[::2] = iterable1
    result[1::2] = iterable2
    
    return result


def pairwise(iterable):
    """
    Return pairs of adjacent values from the input iterable.

    This function takes an iterable and returns an iterator of tuples, 
    where each tuple contains two adjacent values from the input iterable.

    Args:
        iterable (iterable): An iterable object (e.g., list, tuple, string) 
        from which pairs of adjacent values will be generated.

    Returns:
        zip: An iterator of tuples, each containing a pair of adjacent values 
        from the input iterable.

    Raises:
        TypeError: If the input is not an iterable.

    Examples:
        >>> list(pairwise([1, 2, 3, 4]))
        [(1, 2), (2, 3), (3, 4)]

        >>> list(pairwise('abcde'))
        [('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e')]

        >>> list(pairwise((10, 20, 30)))
        [(10, 20), (20, 30)]
    """
    #--------------------------------------------------------------------------
    # Run input checks
    
    if not hasattr(iterable, '__iter__'):
        raise TypeError("Input must be an iterable.")

    #--------------------------------------------------------------------------
    # Apply function
    
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def merge_multi(df_list, on, how, fillna_val=None):
    """
    Perform consecutive merges on a list of pandas DataFrames.

    This function merges a list of DataFrames into a single DataFrame using
    the specified merge parameters. It applies the same merge operation
    sequentially to each DataFrame in the list. Optionally, it can fill
    missing values in the resulting DataFrame.

    Args:
        -df_list (list of pandas.DataFrame): A list of DataFrames to be merged.
        -on (str or list of str): Column name(s) to merge on. If a list is
         provided, it should contain column names common to all DataFrames.
        -how (str): Type of merge to be performed. Must be one of 'left', 
         'right', 'outer', 'inner'.
        -fillna_val (optional): Value to replace missing values with 

    Returns:
        pandas.DataFrame: The resulting DataFrame after merging and optional
        filling of missing values.

    Examples:
        >>> import pandas as pd
        >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
        >>> df2 = pd.DataFrame({'key': ['A', 'B'], 'value2': [4, 5]})
        >>> df3 = pd.DataFrame({'key': ['A'], 'value3': [6]})
        >>> merge_multi([df1, df2, df3], on='key', how='inner')
          key  value1  value2  value3
        0   A       1       4       6
        
        >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
        >>> df2 = pd.DataFrame({'key': ['A', 'B'], 'value2': [4, 5]})
        >>> merge_multi([df1, df2], on='key', how='outer',  fillna_val=0)
          key  value1  value2
        0   A        1        4
        1   B        2        5
        2   C        3        0
    """
    
    #--------------------------------------------------------------------------
    # Run input checks
    
    if not isinstance(df_list, list) or not all(isinstance(df, pd.DataFrame) for df in df_list):
        raise TypeError("`df_list` must be a list of pandas DataFrames.")
    
    if not isinstance(on, (str, list)):
        raise TypeError("`on` must be a string or a list of strings.")
    
    if not isinstance(how, str):
        raise TypeError("`how` must be a string.")
    
    valid_how_options = ['left', 'right', 'outer', 'inner']
    if how not in valid_how_options:
        raise ValueError(f"Invalid merge method: {how}. Must be one of {valid_how_options}.")
    
    #--------------------------------------------------------------------------
    # Apply function
        
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=on, how=how), df_list)
    
    if fillna_val is not None:
        merged_df = merged_df.fillna(fillna_val)
    
    return merged_df


