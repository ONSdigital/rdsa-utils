"""A selection of helper functions for building in pyspark."""
import functools
import itertools
import logging
from typing import Any, Callable, List, Mapping, Optional, Sequence, Union

from pyspark.sql import (
    Column as SparkCol,
    DataFrame as SparkDF,
    functions as F,
    SparkSession,
    types as T,
    Window,
    WindowSpec,
)


logger = logging.getLogger()


def create_colname_to_value_map(cols: Sequence[str]) -> SparkCol:
    """Create a column name to value MapType column."""
    colname_value_tups = [(F.lit(name), F.col(name)) for name in cols]
    # Chain chains multiple lists into one for create_map.
    return F.create_map(*(itertools.chain(*colname_value_tups)))


def set_df_columns_nullable(
    df: SparkDF,
    column_list: List[str],
    nullable: Optional[bool] = True,
) -> SparkDF:
    """Change specified columns nullable value.

    Sometimes find that spark creates columns that have the nullable attribute
    set to False, which can cause issues if this dataframe is saved to a table
    as it will set the scheme for that column to not allow missing values.

    Changing this parameter for a column appears to be very difficult (and also
    potentially costly [see so answer comments] - SO USE ONLY IF NEEDED).

    The solution implemented is taken from Stack Overflow post:
    https://stackoverflow.com/a/51821437

    Parameters
    ----------
    df
        The dataframe with columns to have nullable attribute changed.
    column_list
        List of columns to change nullable attribute.
    nullable
        The value to set the nullable attribute to for the specified columns.

    Returns
    -------
    SparkDF
        The input dataframe but with nullable attribute changed for specified
        columns.
    """
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable

    # Create a new dataframe using the underlying RDDs and the updated schemas.
    return SparkSession._instantiatedSession.createDataFrame(df.rdd, df.schema)


def melt(
    df: SparkDF,
    id_vars: Union[str, Sequence[str]],
    value_vars: Union[str, Sequence[str]],
    var_name: str = 'variable',
    value_name: str = 'value',
) -> SparkDF:
    """Melt a spark dataframe in a pandas like fashion.

    Parameters
    ----------
    df
        The pyspark dataframe to melt.
    id_vars
        The names of the columns to use as identifier variables.
    value_vars
        The names of the columns containing the data to unpivot.
    var_name
        The name of the target column containing variable names
        (i.e. the original column names).
    value_name
        The name of the target column containing the unpivoted
        data.

    Returns
    -------
    SparkDF
        The "melted" input data as a pyspark data frame.

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...     [[1, 2, 3, 4],
    ...      [5, 6, 7, 8],
    ...      [9, 10, 11, 12]],
    ...     ["col1", "col2", "col3", "col4"])
    >>> melt(df=df, id_vars="col1", value_vars=["col2", "col3"]).show()
    +----+--------+-----+
    |col1|variable|value|
    +----+--------+-----+
    |   1|    col2|    2|
    |   1|    col3|    3|
    |   5|    col2|    6|
    |   5|    col3|    7|
    |   9|    col2|   10|
    |   9|    col3|   11|
    +----+--------+-----+

    >>> melt(df=df, id_vars=["col1", "col2"], value_vars=["col3", "col4"]
    ... ).show()
    +----+----+--------+-----+
    |col1|col2|variable|value|
    +----+----+--------+-----+
    |   1|   2|    col3|    3|
    |   1|   2|    col4|    4|
    |   5|   6|    col3|    7|
    |   5|   6|    col4|    8|
    |   9|  10|    col3|   11|
    |   9|  10|    col4|   12|
    +----+----+--------+-----+
    """
    # Create array<struct<variable: str, value: ...>, <struct<...>>
    # Essentially a list of column placeholders
    # Tuple comprehension ensures we have a lit element and col reference for
    # each column to melt
    _vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
            for c in value_vars
        ),
    )

    # Add to the DataFrame and explode, which extends the dataframe
    _tmp = df.withColumn('_vars_and_vals', F.explode(_vars_and_vals))

    # We only want to select certain columns
    cols = id_vars + [
        F.col('_vars_and_vals')[x].alias(x) for x in [var_name, value_name]
    ]

    return _tmp.select(*cols)


def to_spark_col(_func=None, *, exclude: Sequence[str] = None) -> Callable:
    """Convert str args to Spark Column if not already.

    Usage
    -----
    Use as a decorator on a function.

    To convert all string arguments to spark column
    >>> @to_spark_col
    >>> def my_func(arg1, arg2)

    To exclude a string arguments from being converted to a spark column
    >>> @to_spark_col(exclude=['arg2'])
    >>> def my_func(arg1, arg2)
    """
    if not exclude:
        exclude = []

    def caller(func: Callable[[Union[str, SparkCol]], SparkCol]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            varnames = func.__code__.co_varnames
            if args:
                args = [
                    _convert_to_spark_col(arg) if varnames[i] not in exclude else arg # noqa: E501
                    for i, arg in enumerate(args)
                ]
            if kwargs:
                kwargs = {
                    k: _convert_to_spark_col(kwarg) if k not in exclude else kwarg # noqa: E501
                    for k, kwarg in kwargs.items()
                }
            return func(*args, **kwargs)

        return wrapper

    if _func is None:
        return caller
    else:
        return caller(_func)


def _convert_to_spark_col(s: Union[str, SparkCol]) -> SparkCol:
    """Convert strings to Spark Columns, otherwise returns input."""
    if isinstance(s, str):
        return F.col(s)
    elif isinstance(s, SparkCol):
        return s
    else:
        msg = f"""
        expecting a string or pyspark column but received obj
        of type {type(s)}
        """
        raise ValueError(msg)


def to_list(df: SparkDF) -> List[Union[Any, List[Any]]]:
    """Convert Spark DF to a list.

    Returns
    -------
    list or list of lists
        If the input DataFrame has a single column then a list of column
        values will be returned. If the DataFrame has multiple columns
        then a list of row data as lists will be returned.
    """
    if len(df.columns) == 1:
        return df.toPandas().squeeze().tolist()
    else:
        return df.toPandas().to_numpy().tolist()


def map_column_names(df: SparkDF, mapper: Mapping[str, str]) -> SparkDF:
    """Map column names to the given values in the mapper.

    If the column name is not in the mapper the name doesn't change.
    """
    cols = [
        F.col(col_name).alias(mapper.get(col_name, col_name))
        for col_name in df.columns
    ]
    return df.select(*cols)


def transform(self, f, *args, **kwargs):
    """Chain Pyspark function."""
    return f(self, *args, **kwargs)


def is_df_empty(df: SparkDF) -> bool:
    """Check whether a spark dataframe contains any records."""
    if not df.head(1):
        return True
    else:
        return False


def unpack_list_col(
    df: SparkDF,
    list_col: str,
    unpacked_col: str,
) -> SparkDF:
    """Unpack a spark column containing a list into multiple rows.

    Parameters
    ----------
    df
        Contains the list column to unpack.
    list_col
        The name of the column which contains lists.
    unpacked_col
        The name of the column containing the unpacked list items.

    Returns
    -------
    SparkDF
        Contains a new row for each unpacked list item.
    """
    return df.withColumn(unpacked_col, F.explode(list_col))


def get_window_spec(
    partition_cols: Optional[Union[str, Sequence[str]]] = None,
    order_cols: Optional[Union[str, Sequence[str]]] = None,
) -> WindowSpec:
    """Return ordered and partitioned WindowSpec, defaulting to whole df.

    Particularly useful when you don't know if the variable being used for
    partition_cols will contain values or not in advance.

    Parameters
    ----------
    partition_cols
        If present the columns to partition a spark dataframe on.
    order_cols
        If present the columns to order a spark dataframe on (where order in
        sequence is order that orderBy is applied).

    Returns
    -------
    WindowSpec
        The WindowSpec object to be applied.

    Usage
    -----
    window_spec = get_window_spec(...)

    F.sum(values).over(window_spec)
    """
    if partition_cols and order_cols:
        window_spec = Window.partitionBy(partition_cols).orderBy(order_cols)

    elif partition_cols:
        window_spec = Window.partitionBy(partition_cols)

    elif order_cols:
        window_spec = Window.orderBy(order_cols)

    else:
        window_spec = Window.rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing,
        )

    return window_spec


def rank_numeric(
    numeric: Union[str, Sequence[str]],
    group: Union[str, Sequence[str]],
    ascending: bool = False,
) -> SparkCol:
    """Rank a numeric and assign a unique value to each row.

    The `F.row_number()` method has been selected as a method to rank as
    gives a unique number to each row. Other methods such as `F.rank()`
    and `F.dense_rank()` do not assign unique values per row.

    Parameters
    ----------
    numeric
        The column name or list of column names containing values which will
        be ranked.
    group
        The grouping levels to rank the numeric column or columns over.
    ascending
        Dictates whether high or low values are ranked as the top value.

    Returns
    -------
    SparkCol
        Contains a rank for the row in its grouping level.
    """
    if ascending:
        # Defaults to ascending.
        window = Window.partitionBy(group).orderBy(numeric)
    else:
        if type(numeric) is list:
            message = f"""
            The function parameter numeric must be a string when using
            F.desc(numeric).
            Currently numeric={numeric}.
            """
            logger.error(message)
            raise ValueError(message)
        window = Window.partitionBy(group).orderBy(F.desc(numeric))

    return F.row_number().over(window)


def calc_median_price(
    groups: Union[str, Sequence[str]],
    price_col: str = 'price',
) -> SparkCol:
    """Calculate the median price per grouping level.

    Parameters
    ----------
    groups
        The grouping levels for calculating the average price.
    price_col
        Column name containing the product prices.

    Returns
    -------
    SparkCol
        A single entry for each grouping level, and its median price.
    """
    # Note median in [1,2,3,4] would return as 2 using below.
    median = f'percentile_approx({price_col}, 0.5)'

    return F.expr(median).over(Window.partitionBy(groups))


def convert_cols_to_struct_col(
    df: SparkDF,
    struct_col_name: str,
    struct_cols: Optional[Sequence[str]],
    no_struct_col_type: T.DataTypeSingleton = T.BooleanType(),
    no_struct_col_value: Any = None,
) -> SparkDF:
    """Convert specified selection of columns to a single struct column.

    As BigQuery tables do not take to having an empty struct column appended to
    them, this function will create a placeholder column to put into the struct
    column if no column names to combine are passed.

    Parameters
    ----------
    df
        The input dataframe that contains the columns for combining.
    struct_col_name
        The name of the resulting struct column.
    struct_cols
        A sequence of columns present in df for combining.
    no_struct_col_type
        If no struct_cols are present, this is the type that the dummy column
        to place in the struct will be, default = BooleanType.
    no_struct_col_value
        If no struct_cols are present, this is the value that will be used in
        the dummy column, default = None.

    Returns
    -------
        The input dataframe with the specified struct_cols dropped and replaced
        with a single struct type column containing those columns.

    Raises
    ------
    ValueError
        If not all the specified struct_cols are present in df.
    """
    if struct_cols and not all(col in df.columns for col in struct_cols):
        message = f"""
        Cannot create struct columns due to column mismatch.

        Want to create struct column from columns: {struct_cols}

        But dataframe has columns {df.columns}
        """
        logger.error(message)
        raise ValueError(message)

    if not struct_cols:
        df = df.withColumn(
            f'no_{struct_col_name}',
            F.lit(no_struct_col_value).cast(no_struct_col_type),
        )
        struct_cols = [f'no_{struct_col_name}']

    return (
        df
        .withColumn(struct_col_name, F.struct(*struct_cols))
        .drop(*struct_cols)
    )


def select_first_obs_appearing_in_group(
    df: SparkDF,
    group: Sequence[str],
    date_col: str,
    ascending: bool,
) -> SparkDF:
    """Rank and select observation in group based on earliest or latest date.

    Given that there can be multiple observations per group, select
    observation that appears first or last (depending on whether ascending is
    set to True or False, respectively).

    Parameters
    ----------
    df
        The input dataframe that contains the group and date_col.
    group
        The grouping levels required to find the observation that appears first
        or last (depending on whether ascending is set to True or False,
        respectively)
    date_col
        Column name containing the dates of each observation.
    ascending
        Dictates whether first or last observation within a grouping is
        selected (depending on whether ascending is set to True or False,
        respectively).

    Returns
    -------
    SparkDF
        The input dataframe that contains each observation per group that
        appeared first or last (depending on whether ascending is set to
        True or False, respectively) according to date_col.
    """
    rank_by_date = rank_numeric(
        numeric=date_col,
        group=group,
        ascending=ascending,
    )
    return (
        df
        .withColumn('rank', rank_by_date)
        .filter(F.col('rank') == 1)
        .drop('rank')
    )
