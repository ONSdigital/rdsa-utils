"""A selection of helper functions for building in pyspark."""

import functools
import itertools
import logging
import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import pandas as pd
from pyspark.sql import Column as SparkCol
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession, Window, WindowSpec
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rdsa_utils.cdp.io.input import extract_database_name
from rdsa_utils.logging import log_spark_df_schema

logger = logging.getLogger(__name__)


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
    as it will set the schema for that column to not allow missing values.

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
    var_name: str = "variable",
    value_name: str = "value",
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
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    # We only want to select certain columns
    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
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
                    (
                        _convert_to_spark_col(arg)
                        if varnames[i] not in exclude
                        else arg
                    )  # noqa: E501
                    for i, arg in enumerate(args)
                ]
            if kwargs:
                kwargs = {
                    k: (
                        _convert_to_spark_col(kwarg) if k not in exclude else kwarg
                    )  # noqa: E501
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
        F.col(col_name).alias(mapper.get(col_name, col_name)) for col_name in df.columns
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
    price_col: str = "price",
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
    median = f"percentile_approx({price_col}, 0.5)"

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
            f"no_{struct_col_name}",
            F.lit(no_struct_col_value).cast(no_struct_col_type),
        )
        struct_cols = [f"no_{struct_col_name}"]

    return df.withColumn(struct_col_name, F.struct(*struct_cols)).drop(*struct_cols)


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
    return df.withColumn("rank", rank_by_date).filter(F.col("rank") == 1).drop("rank")


@log_spark_df_schema
def convert_struc_col_to_columns(
    df: SparkDF,
    convert_nested_structs: bool = False,
) -> SparkDF:
    """Flatten struct columns in pyspark dataframe to individual columns.

    Parameters
    ----------
    df
        Dataframe that may or may not contain struct type columns.
    convert_nested_structs
        If true, function will recursively call until no structs are left.
        Inversely, when false, only top level structs are flattened; if these
        contain subsequent structs they would remain.

    Returns
    -------
        The input dataframe but with any struct type columns dropped, and in
        its place the individual fields within the struct column as individual
        columns.
    """
    struct_cols = []
    for field in df.schema.fields:
        if type(field.dataType) == T.StructType:
            struct_cols.append(field.name)

    df = df.select(
        # Select all columns in df not identified as being struct type.
        *[col for col in df.columns if col not in struct_cols],
        # All columns identified as being struct type, but expand the struct
        # to individual columns using .* notation.
        *[f"{col}.*" for col in struct_cols],
    )

    if convert_nested_structs and any(
        isinstance(field.dataType, T.StructType) for field in df.schema.fields
    ):
        df = convert_struc_col_to_columns(df=df)

    return df


def cut_lineage(df: SparkDF) -> SparkDF:
    """Convert the SparkDF to a Java RDD and back again.

    This function is helpful in instances where Catalyst optimiser is causing
    memory errors or problems, as it only tries to optimise till the conversion
    point.

    Note: This uses internal members and may break between versions.

    Parameters
    ----------
    df
        SparkDF to convert.

    Returns
    -------
    SparkDF
        New SparkDF created from Java RDD.

    Raises
    ------
    Exception
        If any error occurs during the lineage cutting process,
        particularly during conversion between SparkDF and Java RDD
        or accessing internal members.

    Examples
    --------
    >>> df = rdd.toDF()
    >>> new_df = cut_lineage(df)
    >>> new_df.count()
    3
    """
    try:
        logger.info("Converting SparkDF to Java RDD.")

        jrdd = df._jdf.toJavaRDD()
        jschema = df._jdf.schema()
        jrdd.cache()

        # Check for sparkSession attribute (introduced in Spark 3.3.0)
        if hasattr(df, "sparkSession"):
            spark = df.sparkSession
        else:
            # Fallback for Spark 3.2.3
            spark = df.sql_ctx.sparkSession

        new_java_df = spark._jsparkSession.createDataFrame(jrdd, jschema)
        new_df = SparkDF(new_java_df, spark)
        return new_df
    except Exception as e:
        logger.error(f"An error occurred during the lineage cutting process: {e}")
        raise


def find_spark_dataframes(
    locals_dict: Dict[str, Union[SparkDF, Dict]],
) -> Dict[str, Union[SparkDF, Dict]]:
    """Extract SparkDF's objects from a given dictionary.

    This function scans the dictionary and returns another containing only
    entries where the value is a SparkDF. It also handles dictionaries within
    the input, including them in the output if their first item is a SparkDF.

    Designed to be used with locals() in Python, allowing extraction of
    all SparkDF variables in a function's local scope.

    Parameters
    ----------
    locals_dict
        A dictionary usually returned by locals(), with variable names
        as keys and their corresponding objects as values.

    Returns
    -------
    Dict
        A dictionary with entries from locals_dict where the value is a
        SparkDF or a dictionary with a SparkDF as its first item.

    Examples
    --------
    >>> dfs = find_spark_dataframes(locals())
    """
    frames = {}

    for key, value in locals_dict.items():
        if key in ["_", "__", "___"]:
            continue

        if isinstance(value, SparkDF):
            frames[key] = value
            logger.info(f"SparkDF found: {key}")
        elif (
            isinstance(value, dict)
            and value
            and isinstance(next(iter(value.values())), SparkDF)
        ):
            frames[key] = value
            logger.info(f"Dictionary of SparkDFs found: {key}")
        else:
            logger.debug(
                f"Skipping non-SparkDF item: {key}, Type: {type(value)}",
            )

    return frames


def create_spark_session(
    app_name: Optional[str] = None,
    size: Optional[Literal["small", "medium", "large", "extra-large"]] = None,
    extra_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """Create a PySpark Session based on the specified size.

    This function creates a PySpark session with different configurations
    based on the size specified.

    The size can be 'default', 'small', 'medium', 'large', or 'extra-large'.
    Extra Spark configurations can be passed as a dictionary.
    If no size is given, then a basic Spark session is spun up.

    Parameters
    ----------
    app_name
        The spark session app name.
    size
        The size of the spark session to be created. It can be 'default',
        'small', 'medium', 'large', or 'extra-large'.
    extra_configs
        Mapping of additional spark session config settings and the desired
        value for it. Will override any default settings.

    Returns
    -------
    SparkSession
        The created PySpark session.

    Raises
    ------
    ValueError
        If the specified 'size' parameter is not one of the valid options:
        'small', 'medium', 'large', or 'extra-large'.
    Exception
        If any other error occurs during the Spark session creation process.

    Examples
    --------
    >>> spark = create_spark_session('medium', {'spark.ui.enabled': 'false'})

    Session Details:
    ---------------
    'small':
        This is the smallest session that will realistically be used. It uses
        only 1g of memory and 3 executors, and only 1 core. The number of
        partitions are limited to 12, which can improve performance with
        smaller data. It's recommended for simple data exploration of small
        survey data or for training and demonstrations when several people
        need to run Spark sessions simultaneously.
    'medium':
        A standard session used for analysing survey or synthetic datasets.
        Also used for some Production pipelines based on survey and/or smaller
        administrative data.It uses 6g of memory and 3 executors, and 3 cores.
        The number of partitions are limited to 18, which can improve
        performance with smaller data.
    'large':
        Session designed for running Production pipelines on large
        administrative data, rather than just survey data. It uses 10g of
        memory and 5 executors, 1g of memory overhead, and 5 cores. It uses the
        default number of 200 partitions.
    'extra-large':
        Used for the most complex pipelines, with huge administrative
        data sources and complex calculations. It uses 20g of memory and
        12 executors, 2g of memory overhead, and 5 cores. It uses 240
        partitions; not significantly higher than the default of 200,
        but it is best for these to be a multiple of cores and executors.

    References
    ----------
    The session sizes and their details are taken directly
    from the following resource:
    "https://best-practice-and-impact.github.io/ons-spark/spark-overview/example-spark-sessions.html"
    """
    try:
        if size:
            size = size.lower()
            valid_sizes = ["small", "medium", "large", "extra-large"]
            if size not in valid_sizes:
                msg = f"Invalid '{size=}'. If specified must be one of {valid_sizes}."
                raise ValueError(msg)

        logger.info(
            (
                f"Creating a '{size}' Spark session..."
                if size
                else "Creating a basic Spark session..."
            ),
        )

        if app_name:
            builder = SparkSession.builder.appName(f"{app_name}")
        else:
            builder = SparkSession.builder

        # fmt: off
        if size == "small":
            builder = (
                builder.config("spark.executor.memory", "1g")
                .config("spark.executor.cores", 1)
                .config("spark.dynamicAllocation.maxExecutors", 3)
                .config("spark.sql.shuffle.partitions", 12)
            )
        elif size == "medium":
            builder = (
                builder.config("spark.executor.memory", "6g")
                .config("spark.executor.cores", 3)
                .config("spark.dynamicAllocation.maxExecutors", 3)
                .config("spark.sql.shuffle.partitions", 18)
            )
        elif size == "large":
            builder = (
                builder.config("spark.executor.memory", "10g")
                .config("spark.yarn.executor.memoryOverhead", "1g")
                .config("spark.executor.cores", 5)
                .config("spark.dynamicAllocation.maxExecutors", 5)
                .config("spark.sql.shuffle.partitions", 200)
            )
        elif size == "extra-large":
            builder = (
                builder.config("spark.executor.memory", "20g")
                .config("spark.yarn.executor.memoryOverhead", "2g")
                .config("spark.executor.cores", 5)
                .config("spark.dynamicAllocation.maxExecutors", 12)
                .config("spark.sql.shuffle.partitions", 240)
            )

        # Common configurations for all sizes
        builder = (
            # Dynamic Allocation
            builder.config("spark.dynamicAllocation.enabled", "true")
             .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
             # Adaptive Query Execution
             .config("spark.sql.adaptive.enabled", "true")
             # General
             .config("spark.ui.showConsoleProgress", "false")
        ).enableHiveSupport()
        # fmt: on

        # Apply extra configurations
        if extra_configs:
            for key, value in extra_configs.items():
                builder = builder.config(key, value)

        logger.info("Spark session created successfully!")
        return builder.getOrCreate()
    except Exception as e:
        logger.error(f"An error occurred while creating the Spark session: {e}")
        raise


def load_csv(
    spark: SparkSession,
    filepath: str,
    keep_columns: Optional[List[str]] = None,
    rename_columns: Optional[Dict[str, str]] = None,
    drop_columns: Optional[List[str]] = None,
    **kwargs,
) -> SparkDF:
    """Load a CSV file into a PySpark DataFrame.

    spark
        Active SparkSession.
    filepath
        The full path and filename of the CSV file to load.
    keep_columns
        A list of column names to keep in the DataFrame, dropping all others.
        Default value is None.
    rename_columns
        A dictionary to rename columns where keys are existing column
        names and values are new column names.
        Default value is None.
    drop_columns
        A list of column names to drop from the DataFrame.
        Default value is None.
    kwargs
        Additional keyword arguments to pass to the `spark.read.csv` method.

    Returns
    -------
    SparkDF
        PySpark DataFrame containing the data from the CSV file.

    Notes
    -----
    Transformation order:
    1. Columns are kept according to `keep_columns`.
    2. Columns are dropped according to `drop_columns`.
    3. Columns are renamed according to `rename_columns`.

    Raises
    ------
    Exception
        If there is an error loading the file.
    ValueError
        If a column specified in rename_columns, drop_columns, or
        keep_columns is not found in the DataFrame.

    Examples
    --------
    Load a CSV file with multiline and rename columns:

    >>> df = load_csv(
            spark,
            "/path/to/file.csv",
            multiLine=True,
            rename_columns={"old_name": "new_name"}
        )

    Load a CSV file with a specific encoding:

    >>> df = load_csv(spark, "/path/to/file.csv", encoding="ISO-8859-1")

    Load a CSV file and keep only specific columns:

    >>> df = load_csv(spark, "/path/to/file.csv", keep_columns=["col1", "col2"])

    Load a CSV file and drop specific columns:

    >>> df = load_csv(spark, "/path/to/file.csv", drop_columns=["col1", "col2"])

    Load a CSV file with custom delimiter and multiline:

    >>> df = load_csv(spark, "/path/to/file.csv", sep=";", multiLine=True)
    """
    try:
        df = spark.read.csv(filepath, header=True, **kwargs)
        logger.info(
            (
                f"Loaded CSV file: {filepath}. "
                f"Keep columns: {keep_columns}, Drop columns: {drop_columns}, "
                f"Rename columns: {rename_columns}."
                + (f" Additional read options: {kwargs}." if kwargs else "")
            ),
        )
    except Exception as e:
        error_message = f"Error loading file {filepath}: {e}"
        logger.error(error_message)
        raise Exception(error_message) from e

    columns = [str(col) for col in df.columns]

    # When multi_line is used it adds \r at the end of the final column
    if kwargs.get("multiLine", False):
        columns[-1] = columns[-1].replace("\r", "")
        df = df.withColumnRenamed(df.columns[-1], columns[-1])

    # Apply column transformations: keep, drop, rename
    if keep_columns:
        missing_columns = [col for col in keep_columns if col not in columns]
        if missing_columns:
            error_message = (
                f"Columns {missing_columns} not found in DataFrame and cannot be kept"
            )
            logger.error(error_message)
            raise ValueError(error_message)
        df = df.select(*keep_columns)

    if drop_columns:
        for col in drop_columns:
            if col in columns:
                df = df.drop(col)
            else:
                error_message = (
                    f"Column '{col}' not found in DataFrame and cannot be dropped"
                )
                logger.error(error_message)
                raise ValueError(error_message)

    if rename_columns:
        for old_name, new_name in rename_columns.items():
            if old_name in columns:
                df = df.withColumnRenamed(old_name, new_name)
            else:
                error_message = (
                    f"Column '{old_name}' not found in DataFrame and "
                    f"cannot be renamed to '{new_name}'"
                )
                logger.error(error_message)
                raise ValueError(error_message)

    return df


def truncate_external_hive_table(spark: SparkSession, table_identifier: str) -> None:
    """Truncate an External Hive table stored on S3 or HDFS.

    Parameters
    ----------
    spark
        Active SparkSession.
    table_identifier
        The name of the Hive table to truncate. This can either be in the format
        '<database>.<table>' or simply '<table>' if the current Spark session
        has a database set.

    Returns
    -------
    None
        This function does not return any value. It performs an action of
        truncating the table.

    Raises
    ------
    ValueError
        If the table name is incorrectly formatted, the database is not provided
        when required, or if the table does not exist.
    AnalysisException
        If there is an issue with partition operations or SQL queries.
    Exception
        If there is a general failure during the truncation process.

    Examples
    --------
    Truncate a Hive table named 'my_database.my_table':

    >>> truncate_external_hive_table(spark, 'my_database.my_table')

    Or, if the current Spark session already has a database set:

    >>> spark.catalog.setCurrentDatabase('my_database')
    >>> truncate_external_hive_table(spark, 'my_table')
    """
    try:
        logger.info(f"Attempting to truncate the table '{table_identifier}'")

        # Extract database and table name, even if only the table name is provided
        db_name, table_name = extract_database_name(spark, table_identifier)

        # Set the current database if a database was specified
        if db_name:
            spark.catalog.setCurrentDatabase(db_name)

        # Check if the table exists before proceeding
        if not spark.catalog.tableExists(table_name, db_name):
            error_msg = f"Table '{db_name}.{table_name}' does not exist."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get the list of partitions
        try:
            partitions = spark.sql(f"SHOW PARTITIONS {db_name}.{table_name}").collect()
        except Exception as e:
            logger.warning(
                f"Unable to retrieve partitions for '{db_name}.{table_name}': {e}",
            )
            partitions = []

        if partitions:
            logger.info(
                f"Table '{table_identifier}' is partitioned. Dropping all partitions.",
            )

            # Drop each partition
            for partition in partitions:
                partition_spec = partition[
                    0
                ]  # e.g., partition is in format 'year=2023', etc.
                spark.sql(
                    f"ALTER TABLE {db_name}.{table_name} "
                    f"DROP IF EXISTS PARTITION ({partition_spec})",
                )

        else:
            logger.info(
                f"Table '{table_identifier}' has no partitions or is not partitioned.",
            )

            # Overwrite with an empty DataFrame
            original_df = spark.table(f"{db_name}.{table_name}")
            schema: T.StructType = original_df.schema
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")

        logger.info(f"Table '{table_identifier}' successfully truncated.")

    except Exception as e:
        logger.error(
            f"An error occurred while truncating the table '{table_identifier}': {e}",
        )
        raise


def cache_time_df(df: SparkDF) -> None:
    """Cache a PySpark DataFrame and print the time taken to cache and count it.

    Parameters
    ----------
    df
        The PySpark DataFrame to cache.

    Returns
    -------
    None
    """
    if not isinstance(df, SparkDF):
        msg = "Input must be a PySpark DataFrame."
        raise TypeError(msg)

    start_time = time.time()
    df.cache().count()
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    logger.info(f"Cached in {elapsed_time} seconds")


def count_nulls(
    df: SparkDF,
    subset_cols: Optional[Union[List[str], str]] = None,
) -> pd.DataFrame:
    """Count the number of null values in the specified columns of a SparkDF.

    Parameters
    ----------
    df
        The PySpark DataFrame to analyze.
    subset_cols
        List of column names or a single column name as a string to count
        null values for. If not provided, counts are calculated for all
        columns.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame with the count of null values per column.
    """
    if not isinstance(df, SparkDF):
        msg = "Input must be a PySpark DataFrame."
        raise TypeError(msg)
    if isinstance(subset_cols, str):
        subset_cols = [subset_cols]
    if subset_cols is not None:
        if not isinstance(subset_cols, list):
            msg = "subset_cols must be a list, a string, or None."
            raise TypeError(msg)
        if not all(isinstance(col, str) for col in subset_cols):
            msg = "All elements of subset_cols must be strings."
            raise TypeError(msg)

    cols = subset_cols if subset_cols else df.columns
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols],
    ).toPandas()
    return null_counts


def aggregate_col(df: SparkDF, col: str, operation: str) -> float:
    """Aggregate (sum, max, min, or mean) a numeric PySpark column.

    Parameters
    ----------
    df
        The PySpark DataFrame containing the column.
    col
        The name of the numeric column to aggregate.
    operation
        The type of aggregation to perform. Must be one of 'sum', 'max',
        'min', or 'mean'.

    Returns
    -------
    float
        The result of the specified aggregation on the column.
    """
    if not isinstance(df, SparkDF):
        msg = "Input df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(col, str):
        msg = "Column name must be a string."
        raise TypeError(msg)
    valid_operations = ["sum", "max", "min", "mean"]
    if operation not in valid_operations:
        msg = f"`operation` must be one of {valid_operations}."
        raise ValueError(msg)

    result = df.agg({col: operation}).collect()[0][0]
    logger.info(f"{operation.capitalize()} of values in {col}: {result}")
    return result


def get_unique(
    df: SparkDF,
    col: str,
    remove_null: bool = False,
    verbose: bool = True,
) -> List[Optional[Union[str, int, float]]]:
    """Return a list of unique values in a PySpark DataFrame column.

    Parameters
    ----------
    df
        The PySpark DataFrame containing the column.
    col
        The name of the column to analyze.
    remove_null
        Whether to remove null values from output. Default is False.
    verbose
        Whether to log the number of unique values. Default is True.

    Returns
    -------
    List
        A list of unique values from the specified column.
    """
    if not isinstance(df, SparkDF):
        msg = "Input df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(col, str):
        msg = "Column name must be a string."
        raise TypeError(msg)
    if not isinstance(remove_null, bool):
        msg = "remove_null must be a boolean."
        raise TypeError(msg)
    if not isinstance(verbose, bool):
        msg = "verbose must be a boolean."
        raise TypeError(msg)

    unique_vals = df.select(col).distinct().rdd.map(lambda r: r[0]).collect()
    if remove_null:
        unique_vals = [c for c in unique_vals if c is not None]
    unique_vals = sorted(unique_vals, key=lambda x: (x is None, x))
    if verbose:
        logger.info(f"{len(unique_vals)} unique values in {col}")
    return unique_vals


def drop_duplicates_reproducible(
    df: SparkDF,
    col: str,
    id_col: Optional[str] = None,
) -> SparkDF:
    """Remove duplicates from a PySpark DataFrame in a repeatable manner.

    Parameters
    ----------
    df
        The PySpark DataFrame.
    col
        The column to partition by for removing duplicates.
    id_col
        The column to use for ordering within each partition. If None, a
        unique ID column is generated.

    Returns
    -------
    SparkDF
        The SparkDF with duplicates removed.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(col, str):
        msg = "col must be a string."
        raise TypeError(msg)
    if col not in df.columns:
        msg = f"{col} does not exist in the SparkDF."
        raise ValueError(msg)
    if id_col is not None:
        if not isinstance(id_col, str):
            msg = "id_col must be a string or None."
            raise TypeError(msg)
        if id_col not in df.columns:
            msg = f"{id_col} not in the SparkDF."
            raise ValueError(msg)

    if id_col is None:
        df = df.withColumn("dup_id", F.monotonically_increasing_id())
        id_col = "dup_id"

    window_spec = Window.partitionBy(col).orderBy(id_col)
    df = df.withColumn("rank", F.rank().over(window_spec))
    df = df.filter(F.col("rank") == 1)
    df = df.drop("dup_id", "rank")
    return df


def apply_col_func(
    df: SparkDF,
    cols: List[str],
    func: Callable[[SparkDF, str], SparkDF],
) -> SparkDF:
    """Apply a function to a list of columns in a PySpark DataFrame.

    Parameters
    ----------
    df
        The PySpark DataFrame.
    cols
        List of column names to apply the function to.
    func
        The function to apply, which should accept two arguments: (df, col).

    Returns
    -------
    SparkDF
        The SparkDF after applying the function to each column.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(cols, list):
        msg = "cols must be a list of strings."
        raise TypeError(msg)
    if not all(isinstance(col, str) for col in cols):
        msg = "All elements in cols must be strings."
        raise TypeError(msg)
    if not all(col in df.columns for col in cols):
        msg = "All column names in cols must exist in the SparkDF."
        raise ValueError(msg)
    if not callable(func):
        msg = "func must be a callable function."
        raise TypeError(msg)

    for col in cols:
        df = func(df, col)
    return df


def pyspark_random_uniform(
    df: SparkDF,
    output_colname: str,
    lower_bound: float = 0,
    upper_bound: float = 1,
    seed: Optional[int] = None,
) -> SparkDF:
    """Mimic numpy.random.uniform for PySpark.

    Parameters
    ----------
    df
        The PySpark DataFrame to which the column will be added.
    output_colname
        The name of the new column to be created.
    lower_bound
        The lower bound of the uniform distribution. Defaults to 0.
    upper_bound
        The upper bound of the uniform distribution. Defaults to 1.
    seed
        Seed for random number generation. Defaults to None for
        non-deterministic results.

    Returns
    -------
    SparkDF
        The SparkDF with the new column added.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(output_colname, str):
        msg = "output_colname must be a string."
        raise TypeError(msg)
    if not isinstance(lower_bound, (int, float)):
        msg = "lower_bound must be a number."
        raise TypeError(msg)
    if not isinstance(upper_bound, (int, float)):
        msg = "upper_bound must be a number."
        raise TypeError(msg)
    if not lower_bound < upper_bound:
        msg = "lower_bound must be less than upper_bound."
        raise ValueError(msg)

    return df.withColumn(
        output_colname,
        F.rand(seed) * (upper_bound - lower_bound) + lower_bound,
    )


def cumulative_array(
    df: SparkDF,
    array_col: str,
    output_colname: str,
) -> SparkDF:
    """Convert a PySpark array column to a cumulative array column.

    Parameters
    ----------
    df
        The PySpark DataFrame containing the array column.
    array_col
        The name of the array column to convert.
    output_colname
        The name of the new column to store the cumulative array.

    Returns
    -------
    SparkDF
        The SparkDF with the cumulative array column added.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(array_col, str):
        msg = "array_col must be a string."
        raise TypeError(msg)
    if not isinstance(output_colname, str):
        msg = "output_colname must be a string."
        raise TypeError(msg)
    if array_col not in df.columns:
        msg = f"{array_col} not in SparkDF columns."
        raise ValueError(msg)

    return df.withColumn(
        output_colname,
        F.expr(
            f"""transform({array_col}, (x, i) ->
            aggregate(slice({array_col}, 1, i), 0D,
            (acc, y) -> acc + y) + x)""",
        ),
    )


def union_mismatched_dfs(df1: SparkDF, df2: SparkDF) -> SparkDF:
    """Perform a union between PySpark DataFrames with mismatched column names.

    Parameters
    ----------
    df1
        The first PySpark DataFrame.
    df2
        The second PySpark DataFrame.

    Returns
    -------
    SparkDF
        A SparkDF resulting from the union of df1 and df2, with missing
        columns filled with null values.
    """
    if not isinstance(df1, SparkDF):
        msg = "df1 must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(df2, SparkDF):
        msg = "df2 must be a PySpark DataFrame."
        raise TypeError(msg)

    diff1 = [c for c in df2.columns if c not in df1.columns]
    diff2 = [c for c in df1.columns if c not in df2.columns]

    df1_expanded = df1.select("*", *[F.lit(None).alias(c) for c in diff1])
    df2_expanded = df2.select("*", *[F.lit(None).alias(c) for c in diff2])

    return df1_expanded.unionByName(df2_expanded)


def sum_columns(
    df: SparkDF,
    cols_to_sum: List[str],
    output_col: str,
) -> SparkDF:
    """Calculate row-wise sum of specified PySpark columns.

    Parameters
    ----------
    df
        The PySpark DataFrame to modify.
    cols_to_sum
        List of column names to sum together.
    output_col
        The name of the new column to create with the sum.

    Returns
    -------
    SparkDF
        The SparkDF with the new column added.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(cols_to_sum, list):
        msg = "cols_to_sum must be a list."
        raise TypeError(msg)
    if not all(isinstance(col, str) for col in cols_to_sum):
        msg = "All elements in cols_to_sum must be strings."
        raise TypeError(msg)
    if not isinstance(output_col, str):
        msg = "output_col must be a string."
        raise TypeError(msg)

    cols_to_sum = [F.col(col) for col in cols_to_sum]
    df = df.withColumn(
        output_col,
        functools.reduce(lambda col1, col2: col1 + col2, cols_to_sum),
    )
    return df


def set_nulls(
    df: SparkDF,
    column: str,
    values: Union[str, List[str]],
) -> SparkDF:
    """Replace specified values with nulls in given column of PySpark df.

    Parameters
    ----------
    df
        The PySpark DataFrame to modify.
    column
        The name of the column in which to replace values.
    values
        The value(s) to replace with nulls.

    Returns
    -------
    SparkDF
        The SparkDF with specified values replaced by nulls.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(column, str):
        msg = "column must be a string."
        raise TypeError(msg)
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        msg = "values must be a list of strings or a string."
        raise TypeError(msg)
    if not all(isinstance(value, str) for value in values):
        msg = "All elements in values must be strings."
        raise TypeError(msg)

    if isinstance(values, str):
        values = [values]
    for value in values:
        df = df.withColumn(
            column,
            F.when(F.col(column) != value, F.col(column)).otherwise(F.lit(None)),
        )
    return df


def union_multi_dfs(df_list: List[SparkDF]) -> SparkDF:
    """Perform a union on all SparkDFs in the provided list.

    Note
    ----
    All SparkDFs must have the same columns.

    Parameters
    ----------
    df_list
        List of PySpark DataFrames to union.

    Returns
    -------
    SparkDF
        A SparkDF that is the result of the union of all SparkDFs in the list.
    """
    if not isinstance(df_list, list):
        msg = "df_list must be a list."
        raise TypeError(msg)
    if not len(df_list) > 0:
        msg = "df_list must not be empty"
        raise ValueError(msg)
    if not all(isinstance(df, SparkDF) for df in df_list):
        msg = "All elements in df_list must be PySpark DataFrames."
        raise TypeError(msg)

    combined_df = functools.reduce(lambda df1, df2: df1.union(df2), df_list)
    return combined_df


def join_multi_dfs(
    df_list: List[SparkDF],
    on: Union[str, List[str]],
    how: str,
) -> SparkDF:
    """Join multiple Spark SparkDFs together.

    Parameters
    ----------
    df_list
        List of Spark SparkDFs to join.
    on
        Column(s) on which to join the SparkDFs.
    how
        Type of join to perform (e.g., 'inner', 'outer', 'left', 'right').

    Returns
    -------
    SparkDF
        A SparkDF that is the result of joining all SparkDFs in the list.
    """
    if not isinstance(df_list, list):
        msg = "df_list must be a list of SparkDFs"
        raise TypeError(msg)
    if not all(isinstance(df, SparkDF) for df in df_list):
        msg = "All elements in df_list must be SparkDFs"
        raise TypeError(msg)
    if not isinstance(on, (str, list)):
        msg = "'on' must be a string or a list of strings"
        raise TypeError(msg)
    if isinstance(on, list) and not all(isinstance(col, str) for col in on):
        msg = "All elements in 'on' must be strings"
        raise ValueError(msg)
    valid_join_types = ["inner", "outer", "left", "right"]
    if how not in valid_join_types:
        msg = f"'how' must be one of {valid_join_types}"
        raise ValueError(msg)

    joined_df = functools.reduce(lambda df1, df2: df1.join(df2, on, how), df_list)
    return joined_df


def map_column_values(
    df: SparkDF,
    dict_: Dict[str, str],
    input_col: str,
    output_col: Union[str, None] = None,
) -> SparkDF:
    """Map PySpark column to dictionary keys.

    Parameters
    ----------
    df
        The PySpark DataFrame to modify.
    dict_
        Dictionary for mapping values in input_col to new values.
    input_col
        The name of the column to replace values in.
    output_col
        The name of the new column with replaced values. Defaults to
        input_col if not provided.

    Returns
    -------
    SparkDF
        The SparkDF with the new column added.
    """
    if not isinstance(df, SparkDF):
        msg = "df must be a PySpark DataFrame."
        raise TypeError(msg)
    if not isinstance(dict_, dict):
        msg = "dict_ must be a dictionary."
        raise TypeError(msg)
    if not isinstance(input_col, str):
        msg = "input_col must be a string."
        raise TypeError(msg)
    if output_col is not None:
        if not isinstance(output_col, str):
            msg = "output_col must be a string."
            raise TypeError(msg)

    if output_col is None:
        output_col = input_col

    mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*dict_.items())])

    df = df.withColumn(
        output_col,
        F.coalesce(mapping_expr.getItem(F.col(input_col)), F.col(input_col)),
    )
    return df


def smart_coalesce(df: SparkDF, target_file_size_mb: int = 512) -> SparkDF:
    """Coalesce a Spark DataFrame to an appropriate number of partitions.

    Coalesces a Spark DataFrame to an appropriate number of partitions based on its
    estimated size using Spark's Catalyst optimiser and a user-defined target file size.

    This function helps to reduce the number of output files written when saving a
    DataFrame to storage systems such as Hive or Amazon S3 by adjusting the number of
    partitions using `.coalesce()`. It is especially useful for avoiding the
    "small files problem", which can negatively affect performance,
    metadata management, and query planning.

    It leverages Spark Catalyst's query plan statistics to get a logical estimate
    of the DataFrame's size in bytes without triggering a full job or action.
    Based on the provided `target_file_size_mb`, it calculates how many output files
    are needed and reduces the number of partitions accordingly.

    Parameters
    ----------
    df
        The input Spark DataFrame that will be written to storage.
    target_file_size_mb
        The desired maximum size of each output file in megabytes. This controls the
        number of output files by estimating how many are needed to approximately
        match the total data volume.
        Default is 512 MB.

    Returns
    -------
    SparkDF
        A Spark DataFrame with a reduced number of partitions, ready to be written
        to disk using `.write()`. The number of partitions is chosen to produce
        output files close to the target size.

    Notes
    -----
    - This function uses Spark Catalyst's logical plan statistics. These may be
      outdated or unavailable if statistics haven't been collected
      (e.g., ANALYZE TABLE not run).
    - If the estimated size is zero or unavailable, it defaults to a single partition.
    - This function uses `.coalesce()` which avoids a shuffle but can cause skew if the
      data is unevenly distributed. For very large datasets, consider using
      `repartition()` instead.
    - This function is best used as a final optimisation before writing output files,
      especially to S3, Hive, or HDFS.

    Why Small Files Are a Problem
    -----------------------------
    Writing many small files (e.g., thousands of files per partition)
    negatively impacts:

    1. Hive Metastore:
       - Hive must track every individual file in the metastore.
       - Too many files lead to slow table listings, metadata queries,
         and planning time.

    2. Spark Performance:
       - During reads, Spark spawns a task per file.
       - Thousands of tiny files = thousands of tasks =
         job scheduling overhead + slow query startup.

    3. S3 Performance:
       - S3 is object storage, not a filesystem. Each file written = one PUT request.
       - Too many files increase write latency and cost.
       - During reads, many GET requests slow down performance.

    Examples
    --------
    Reduce number of output files for a moderate-sized DataFrame:
    >>> coalesced_df = smart_coalesce(df, target_file_size_mb=200)
    >>> coalesced_df.write.mode("overwrite").saveAsTable("my_optimised_table")
    """
    # Get estimated size from Catalyst (in bytes)
    estimated_size_bytes = (
        df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    )

    # Convert target file size to bytes
    target_bytes = target_file_size_mb * 1024 * 1024

    # Determine number of output files
    num_files = max(1, estimated_size_bytes // target_bytes)

    estimated_size_gb = estimated_size_bytes / (1024**3)

    logger.info(
        f"Estimated logical size: {estimated_size_gb:.2f} GB, "
        f"target file size: {target_file_size_mb} MB, "
        f"coalescing to {num_files} partitions.",
    )

    return df.coalesce(num_files)


def filter_out_values(
    df: SparkDF,
    column: str,
    values_to_exclude: List[Union[str, int, float]],
    keep_nulls: bool = True,
) -> SparkDF:
    """Exclude rows whose column value appears in the exclusion list.

    Parameters
    ----------
    df
        Input DataFrame.
    column
        Name of the column to filter on.
    values_to_exclude
        List of values to remove.
    keep_nulls
        Whether to preserve NULL values in the column.

    Raises
    ------
    ValueError
        If `values_to_exclude` is empty.
        If `column` is not found in `df`.

    Returns
    -------
    DataFrame
        Filtered DataFrame with specified values excluded.

    Notes
    -----
    - `isin` performs exact matching. For reliable filtering of floating-point data,
      prefer defining the column as DoubleType.
    - FloatType columns may suffer from binary precision issues,
      causing literal comparisons to fail unexpectedly.
    - If you must filter on FloatType with approximate values, consider:
      1. Rounding the column to a fixed precision:
         ```python
         df = df.withColumn("col", F.round(F.col("col"), 2))
         filter_out_values(df, "col", [1.23, 4.56])
         ```
      2. Filtering by range to capture an approximate match:
         ```python
         df.filter(~((F.col("col") >= 1.229) & (F.col("col") <= 1.231)))
         ```

    Examples
    --------
    # Keep nulls (default)
    >>> data = [(1, "apple"), (2, None), (3, "banana")]
    >>> df = spark.createDataFrame(data, ["id", "fruit"])
    >>> filter_out_values(df, "fruit", ["apple"]).show()
    +---+------+
    | id| fruit|
    +---+------+
    |  2|  null|
    |  3|banana|
    +---+------+

    # Exclude nulls
    >>> filter_out_values(df, "fruit", ["apple"], keep_nulls=False).show()
    +---+------+
    | id| fruit|
    +---+------+
    |  3|banana|
    +---+------+
    """
    if not values_to_exclude:
        error_msg = (
            f"`values_to_exclude` for column='{column}' "
            "must contain at least one value."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    if column not in df.columns:
        error_msg = f"Column '{column}' not found in DataFrame."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        f"filter_out_values function called with column='{column}', "
        f"values_to_exclude={values_to_exclude}, keep_nulls={keep_nulls}",
    )

    col_expr = F.col(column)
    base_condition = ~col_expr.isin(values_to_exclude)
    if keep_nulls:
        condition = base_condition | col_expr.isNull()
    else:
        condition = base_condition

    return df.filter(condition)
