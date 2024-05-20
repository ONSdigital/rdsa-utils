"""Contains the logging configuration for files and method to initialise it."""

import functools
import logging
from functools import partial
from textwrap import dedent
from typing import Callable, Dict, List, Optional

import pandas as pd
from humanfriendly import format_timespan
from pyspark import StorageLevel
from pyspark.sql import DataFrame as SparkDF

# A logger object named after module:
# https://docs.python.org/3/howto/logging.html#advanced-logging-tutorial
logger = logging.getLogger(__name__)


LOG_DEV_LEVEL_NUM = 15
logging.addLevelName(LOG_DEV_LEVEL_NUM, "DEV")


def log_dev(self, message, *args, **kwargs):  # noqa: E302
    """Create a custom log level between INFO and DEBUG named DEV.

    This is lifted from: https://stackoverflow.com/a/13638084
    """
    if self.isEnabledFor(LOG_DEV_LEVEL_NUM):
        # Yes, logger takes its '*args' as 'args'.
        self._log(LOG_DEV_LEVEL_NUM, message, args, **kwargs)  # noqa: W291


logging.Logger.dev = log_dev  # noqa: E305


def init_logger_basic(log_level: int) -> None:
    """Instantiate a basic logger object to be used across modules.

    By using this function to instantiate the logger, you also have access to
    `logger.dev` for log_level=15, as this is defined in the same module scope
    as this function.

    Parameters
    ----------
    log_level
        The level of logging to be recorded. Can be defined either as the
        integer level or the logging.<LEVEL> values in line with the
        definitions of the logging module
        (see - https://docs.python.org/3/library/logging.html#levels)

    Returns
    -------
    None
        The logger created by this function is available in any other modules
        by using `logger = logging.getLogger(__name__)` at the global scope
        level in a module (i.e. below imports, not in a function).
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.dev(
        """
    Initialised logger for pipeline.

    Also have access to `logger.dev` by using this function.
    """,
    )


def init_logger_advanced(
    log_level: int,
    handlers: Optional[List[logging.Handler]] = None,
    log_format: str = None,
    date_format: str = None,
) -> None:
    """Instantiate a logger with provided handlers.

    This function allows the logger to be used across modules. Logs can be
    handled by any number of handlers, e.g., FileHandler, StreamHandler, etc.,
    provided in the `handlers` list.

    Parameters
    ----------
    log_level
        The level of logging to be recorded. Can be defined either as the
        integer level or the logging.<LEVEL> values in line with the definitions
        of the logging module.
        (see - https://docs.python.org/3/library/logging.html#levels)
    handlers
        List of handler instances to be added to the logger. Each handler
        instance must be a subclass of `logging.Handler`. Default is an
        empty list, and in this case, basicConfig with `log_level`,
        `log_format`, and `date_format` is used.
    log_format
        The format of the log message. If not provided, a default format
        `'%(asctime)s %(levelname)s %(name)s: %(message)s'` is used.
    date_format
        The format of the date in the log message. If not provided, a default
        format `'%Y-%m-%d %H:%M:%S'` is used.

    Returns
    -------
    None
        The logger created by this function is available in any other modules
        by using `logging.getLogger(__name__)` at the global scope level in a
        module (i.e., below imports, not in a function).

    Raises
    ------
    ValueError
        If any item in the `handlers` list is not an instance of
        `logging.Handler`.

    Examples
    --------
    >>> file_handler = logging.FileHandler('logfile.log')
    >>> rich_handler = RichHandler()
    >>> init_logger_advanced(
    ...     logging.DEBUG,
    ...     [file_handler, rich_handler],
    ...     "%(levelname)s: %(message)s",
    ...     "%H:%M:%S"
    ... )
    """
    # Set default log format and date format if not provided
    if log_format is None:
        log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"

    # Prepare a formatter
    formatter = logging.Formatter(log_format, date_format)

    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # Check if handlers is None, if so assign an empty list to it
    if handlers is None:
        handlers = []

    # Validate each handler
    for handler in handlers:
        if not isinstance(handler, logging.Handler):
            msg = (
                f"Handler {handler} is not an instance of "
                f"logging.Handler or its subclasses"
            )
            raise ValueError(
                msg,
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # If no handlers provided, use basicConfig
    if not handlers:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            datefmt=date_format,
        )

    logger.debug("Initialised logger for pipeline.")


def timer_args(
    name: str,
    logger: Optional[Callable[[str], None]] = logger.info,
) -> Dict[str, str]:
    """Initialise timer args workaround for 'text' args in codetiming package.

    Works with codetiming==1.4.0

    Parameters
    ----------
    name
        The name of the specific timer log.
    logger
        Optional logger function that can accept a string argument.

    Returns
    -------
    Dict[str, str]
        Dictionary of arguments to pass to specifc codetiming package Timer.
    """
    return {
        "name": name,
        "text": lambda secs: name + f": {format_timespan(secs)}",
        "logger": logger,
        "initial_text": "Running {name}",
    }


def print_full_table_and_raise_error(
    df: pd.DataFrame,
    message: str,
    stop_pipeline: bool = False,
    show_records: bool = False,
) -> None:
    """Output dataframe records to logger.

    The purpose of this function is to enable a user to output a message
    to the logger with the added functionality of stopping the pipeline
    and showing dataframe records in a table format. It may be used for
    instance if a user wants to check the records in a dataframe when it
    expected to be empty.

    Parameters
    ----------
    df
        The dataframe to display records from.
    message
        The message to output to the logger.
    stop_pipeline
        Switch for the user to stop the pipeline and raise an error.
    show_records
        Switch to show records in a dataframe.

    Returns
    -------
    None
        Displays message to user however nothing is returned from
        function.

    Raises
    ------
    ValueError
        Raises error and stops pipeline if switch applied.
    """
    if show_records & stop_pipeline:
        logger.error(df.to_string())
        logger.error(message)
        raise ValueError

    elif stop_pipeline:
        logger.error(message)
        raise ValueError

    elif show_records:
        logger.info(df.to_string())
        logger.info(message)

    else:
        logger.info(message)

    return None


def log_spark_df_schema(
    _func: Callable = None,
    *,
    log_schema_on_input: bool = True,
) -> Callable:
    """Apply decorator to log dataframe schema before and after a function.

    If you use the `df.printSchema() method directly in a print/log statement
    the code is processed and printed regardless of logging leve. Instead you
    need to capture the output and pass this to the logger. See explanaition
    here - https://stackoverflow.com/a/59935109

    Requires that the function being decorated has a parameter called `df` and
    that the function is called with `df` being a keyword argument (e.g.
    `df=df`). If not the decorator will report back that it could not count the
    number of rows of the dataframe before running the decorated function.

    Parameters
    ----------
    log_schema_on_input
        If set to false, then no schema is attempted to be printed for the
        decorated function on input. This is useful for instance where function
        has no df input but does return one (such as when reading a table).

    Notes
    -----
    Explainer on complex decorators (and template for decorator structure):
    https://realpython.com/primer-on-python-decorators/#both-please-but-never-mind-the-bread

    Usage
    -----
    To use decorator to record input and output schema:
    ```python
    >>> @log_spark_df_schema
    >>> def my_func_that_changes_some_columns(some_args, df, some_other_args):
    >>>    ...
    >>>    returns final_df
    >>>
    >>> some_df = my_func_that_changes_some_columns(
    >>>     some_args='hello',
    >>>     df=input_df,
    >>>     some_other_args='world'
    >>> )

    Schema of dataframe before my_func_that_changes_some_columns:
    root
    |-- price: double (nullable = true)
    |-- quantity: long (nullable = true)

    Schema of dataframe after my_func_that_changes_some_columns:
    root
    |-- price: double (nullable = true)
    |-- quantity: long (nullable = true)
    |-- expenditure: double (nullable = true)
    ```

    To use decorator to record output schema only:
    ```python
    >>> @log_spark_df_schema(log_schema_on_input=False)
    >>> def my_func_that_changes_some_columns(some_args, df, some_other_args):
    >>>    ...
    >>>    returns final_df
    >>>
    >>> some_df = my_func_that_changes_some_columns(
    >>>     some_args='hello',
    >>>     df=input_df,
    >>>     some_other_args='world'
    >>> )

    Not printing schema of dataframe before my_func_that_changes_some_columns

    Schema of dataframe after my_func_that_changes_some_columns:
    root
    |-- price: double (nullable = true)
    |-- quantity: long (nullable = true)
    |-- expenditure: double (nullable = true)
    ```
    """  # noqa: E501

    def decorator_function(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            # Define the name of the function being decorated for use in logs.
            func_name = func.__name__

            if log_schema_on_input:
                if not kwargs.get("df"):
                    logger.warning(
                        dedent(
                            f"""
                    Cannot find `df` in keyword named arguments.

                    To use the log_spark_df_schema decorator with the function
                    {func_name}:
                    * it must have a parameter called df that is a spark dataframe.
                    * it must be called specifying the argument names e.g.
                    {func_name}(df=input_df, ... )
                    """,
                        ),
                    )  # noqa: E501
                elif isinstance(kwargs["df"], SparkDF):
                    schema = kwargs["df"]._jdf.schema().treeString()
                    logger.info(
                        f"Schema of dataframe before {func_name}:\n{schema}",
                    )  # noqa: E501
                else:
                    logger.warning(
                        dedent(
                            f"""
                    {func_name} keyword argument `df` has type {type(kwargs['df'])}.

                    Cannot print spark schema for this type of object.
                    """,
                        ),
                    )  # noqa: E501

            else:
                logger.info(
                    f"Not printing schema of dataframe before {func_name}",
                )  # noqa: E501

            # Run the decorated function in its normal way, but catch its
            # output so its schema can be printed.
            df_return = func(*args, **kwargs)

            # Check to ensure that the function returns a single value that is
            # a spark dataframe, as otherwise the print schema operation will
            # fail.
            if isinstance(df_return, SparkDF):
                schema = df_return._jdf.schema().treeString()
                logger.info(
                    f"Schema of dataframe after {func_name}:\n{schema}",
                )  # noqa: E501
            else:
                logger.warning(
                    f"{func_name} should return a spark dataframe for decorator, "
                    f"but returned {type(df_return)}",
                )

            return df_return

        return wrapper_decorator

    if _func is None:
        return decorator_function
    else:
        return decorator_function(_func)


def log_rows_in_spark_df(func: Callable) -> Callable:
    """Apply decorator to log dataframe row count before and after a function.

    Requires that the function being decorated has a parameter called `df` and
    that the function is called with `df` being a keyword argument (e.g.
    `df=df`). If not the decorator will report back that it could not count the
    number of rows of the dataframe before running the decorated function.

    Usage
    -----
    ```python
    @log_rows_in_spark_df
    def my_func_that_changes_no_rows(some_args, df, some_other_args):
       ...
       returns final_df

    some_df = my_func_that_changes_no_rows(
        some_args='hello',
        df=input_df,
        some_other_args='world'
    )

    >>> Rows in dataframe before my_func_that_changes_no_rows : 12345
    >>> Rows in dataframe after my_func_that_changes_no_rows  : 6789
    ```

    Warning:
    -------
    `.count()` is an expensive spark operation to perform. Overuse of this
    decorator can be detrimental to performance. This decorator will cache the
    input dataframe prior to running the count and decorated function, as well
    as persisting the output dataframe prior to counting. The input dataframe
    is also unpersisted from memory prior to the decorator completing.
    """
    logger.debug(
        """
    log_rows_in_spark_df caches and persists spark dataframes to memory.
    It also performs count operations. Both of these could have an adverse
    effect on pipelines if used incorrectly, so use as decorator with care.
    """,
    )

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        # Define the name of the function being decorated for use in logs.
        func_name = func.__name__

        if not kwargs.get("df"):
            logger.warning(
                dedent(
                    f"""
            Cannot find `df` in keyword named arguments.

            To use the log_rows_in_spark_df decorator with the function
            {func_name}:
            * it must have a parameter called df that is a spark dataframe.
            * it must be called specifying the argument names e.g.
            {func_name}(df=input_df, ... )
            """,
                ),
            )
        elif isinstance(kwargs["df"], SparkDF):
            # If not already cached, cache the dataframe prior to counting to
            # allow more efficient processing in function. Is unpersisted at
            # end of decorator.
            if not kwargs["df"].is_cached:
                kwargs["df"].cache()

            logger.info(
                f"Rows in dataframe before {func_name} : {kwargs['df'].count()}",
            )  # noqa: E501
        else:
            logger.warning(
                dedent(
                    f"""
            {func_name} keyword argument `df` has type {type(kwargs['df'])}.

            Cannot count rows for this type of object.
            """,
                ),
            )

        # Run the decorated function in its normal way, but catch its output
        # so it can be counted.
        df_return = func(*args, **kwargs)

        # Check to ensure that the function returns a single value that is a
        # spark dataframe, as otherwise the count operation will fail.
        if isinstance(df_return, SparkDF):
            # Persist the dataframe to be returned prior to counting to allow
            # more efficient processing downstream. We persist here as we will
            # not be removing this dataframe from the in-memory cache in this
            # decorator. Therefore, we don't want it to get pushed onto disk
            # (and incur an expensive swap operation).
            df_return.persist(StorageLevel.MEMORY_ONLY)
            logger.info(
                f"Rows in dataframe after {func_name}  : {df_return.count()}",
            )  # noqa: E501

        else:
            logger.warning(
                f"{func_name} should return a spark dataframe for decorator, "
                f"but returned {type(df_return)}",
            )

        if kwargs.get("df"):
            # Unpersist the cached input df to manage memory.
            kwargs["df"].unpersist()

        return df_return

    return wrapper_decorator


def add_warning_message_to_function(
    _func: Callable = None,
    *,
    message: Optional[str] = None,
) -> Callable:
    """Apply decorator to log a warning message.

    If a message is passed, this decorator adds a warning log of the form
    function_name: message

    Parameters
    ----------
    message
        The message to be logged along with the function name.

    Notes
    -----
    Explainer on complex decorators (and template for decorator structure):
    https://realpython.com/primer-on-python-decorators/#both-please-but-never-mind-the-bread

    Usage
    -----
    To use decorator to log a warning:
    ```python
    >>> @_add_warning_message_to_function(message='here be dragons...')
    >>> def my_func(some_args, some_other_args):
    >>>    ...
    >>>
    >>> some_output = my_func(...)

    Warning my_func: here be dragons...
    ```
    """  # noqa: E501

    def decorator_function(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            # Define the name of the function being decorated for use in logs.
            func_name = func.__name__
            if message:
                logger.warning(f"{func_name}: {message}")

            # Run the decorated function in its normal way.
            output = func(*args, **kwargs)

            return output

        return wrapper_decorator

    if _func is None:
        return decorator_function
    else:
        return decorator_function(_func)


not_undergone_functional_test_warning = partial(
    add_warning_message_to_function,
    message="is unit tested, but not formally end-to-end tested.",
)
