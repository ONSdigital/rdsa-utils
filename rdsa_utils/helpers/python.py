"""Miscellaneous helper functions for Python."""

import itertools
import json
import logging
from datetime import datetime, time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Tuple, Union

import pandas as pd
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
