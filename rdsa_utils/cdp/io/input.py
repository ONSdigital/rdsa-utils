"""Read inputs on CDP."""

import logging
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from rdsa_utils.exceptions import DataframeEmptyError

logger = logging.getLogger(__name__)


def get_current_database(spark: SparkSession) -> str:
    """Retrieve the current database from the active SparkSession."""
    return spark.sql("SELECT current_database()").collect()[0]["current_database()"]


def get_tables_in_database(spark: SparkSession, database_name: str) -> List[str]:
    """Get a list of tables in a given database.

    Parameters
    ----------
    spark
        Active SparkSession.
    database_name
        The name of the database from which to list tables.

    Returns
    -------
    List[str]
        A list of table names in the specified database.

    Raises
    ------
    ValueError
        If there is an error fetching tables from the specified database.

    Examples
    --------
    >>> tables = get_tables_in_database(spark, "default")
    >>> print(tables)
    ['table1', 'table2', 'table3']
    """
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {database_name}")
        tables = [row["tableName"] for row in tables_df.collect()]
        return tables
    except Exception as e:
        error_msg = f"Error fetching tables from database {database_name}: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e


def extract_database_name(
    spark: SparkSession,
    long_table_name: str,
) -> Tuple[str, str]:
    """Extract the database component and table name from a compound table name.

    This function can handle multiple scenarios:

    1. For GCP's naming format '<project>.<database>.<table>',
       the function will return the database and table name.

    2. If the name is formatted as 'db_name.table_name', the function will
       extract and return the database and table names.

    3. If the long_table_name contains only the table name (e.g., 'table_name'),
       the function will use the current database of the SparkSession.

    4. For any other incorrectly formatted names, the function will raise
       a ValueError.

    Parameters
    ----------
    spark
        Active SparkSession.
    long_table_name
        Full name of the table, which can include the GCP project
        and/or database name.

    Returns
    -------
    Tuple[str, str]
        A tuple containing the name of the database and the table name.

    Raises
    ------
    ValueError
        If the table name doesn't match any of the expected formats.
    """
    parts = long_table_name.split(".")

    if len(parts) == 3:  # GCP format: project.database.table
        _, db_name, table_name = parts

    elif len(parts) == 2:  # Common format: database.table
        db_name, table_name = parts

    elif len(parts) == 1:  # Only table name is given
        db_name = get_current_database(spark)
        table_name = parts[0]

    else:
        error_msg = (
            f"Table name {long_table_name} is incorrectly formatted. "
            "Expected formats: <project>.<database>.<table>, "
            "<database>.<table>, or <table>"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        f"Extracted database name: {db_name}, table name: "
        f"{table_name} from {long_table_name}",
    )
    return db_name, table_name


def load_and_validate_table(
    spark: SparkSession,
    table_name: str,
    skip_validation: bool = False,
    err_msg: str = None,
    filter_cond: str = None,
    keep_columns: Optional[List[str]] = None,
    rename_columns: Optional[Dict[str, str]] = None,
    drop_columns: Optional[List[str]] = None,
) -> SparkDF:
    """Load a table, apply transformations, and validate if it is not empty.

    Parameters
    ----------
    spark
        Active SparkSession.
    table_name
        Name of the table to load.
    skip_validation
        If True, skips validation step, by default False.
    err_msg
        Error message to return if table is empty, by default None.
    filter_cond
        Condition to apply to SparkDF once read, by default None.
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

    Returns
    -------
    SparkDF
        Loaded SparkDF if validated, subject to options above.

    Raises
    ------
    PermissionError
        If there's an issue accessing the table or if the table
        does not exist in the specified database.
    ValueError
        If the table is empty after loading, becomes empty after applying
        a filter condition, or if columns specified in keep_columns,
        drop_columns, or rename_columns do not exist in the DataFrame.

    Notes
    -----
    Transformation order:
    1. Columns are kept according to `keep_columns`.
    2. Columns are dropped according to `drop_columns`.
    3. Columns are renamed according to `rename_columns`.

    Examples
    --------
    Load a table, apply a filter, and validate it:

    >>> df = load_and_validate_table(
            spark=spark,
            table_name="my_table",
            filter_cond="age > 21"
        )

    Load a table and keep only specific columns:

    >>> df = load_and_validate_table(
            spark=spark,
            table_name="my_table",
            keep_columns=["name", "age", "city"]
        )

    Load a table, drop specific columns, and rename a column:

    >>> df = load_and_validate_table(
            spark=spark,
            table_name="my_table",
            drop_columns=["extra_column"],
            rename_columns={"name": "full_name"}
        )

    Load a table, skip validation, and apply all transformations:

    >>> df = load_and_validate_table(
            spark=spark,
            table_name="my_table",
            skip_validation=True,
            keep_columns=["name", "age", "city"],
            drop_columns=["extra_column"],
            rename_columns={"name": "full_name"},
            filter_cond="age > 21"
        )
    """
    try:
        df = spark.read.table(table_name)
        logger.info(f"Successfully loaded table {table_name}.")
    except Exception as e:
        db_name, _ = extract_database_name(spark, table_name)
        db_err = (
            f"Error accessing {table_name} in the {db_name} database. "
            "Check you have access to the database and that "
            "the table name is correct."
        )
        logger.error(db_err)
        raise PermissionError(db_err) from e

    columns = [str(col) for col in df.columns]

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

    # Validate the table if skip_validation is not True
    if not skip_validation:
        if df.rdd.isEmpty():
            err_msg = err_msg or f"Table {table_name} is empty."
            raise DataframeEmptyError(err_msg)

    # Apply the filter condition if provided
    if filter_cond:
        df = df.filter(filter_cond)
        if not skip_validation and df.rdd.isEmpty():
            err_msg = (
                err_msg
                or f"Table {table_name} is empty after applying "
                f"filter condition [{filter_cond}]."
            )
            raise DataframeEmptyError(err_msg)

    logger.info(
        (
            f"Loaded and validated table {table_name}. "
            f"Filter condition applied: {filter_cond}. "
            f"Keep columns: {keep_columns}, Drop columns: {drop_columns}, "
            f"Rename columns: {rename_columns}."
        ),
    )

    return df
