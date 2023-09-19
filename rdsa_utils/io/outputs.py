"""Functions that primarily deal with writing or saving data."""
import logging
from typing import Union

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from rdsa_utils.helpers.helpers_spark import assert_df_is_not_empty
from rdsa_utils.io.inputs import load_and_validate_table

logger = logging.getLogger(__name__)


def insert_df_to_hive_table(
    spark: SparkSession,
    df: SparkDF,
    table_name: str,
    overwrite: bool = False,
    fill_missing_cols: bool = False,
) -> None:
    """Write the SparkDF contents to a Hive table.

    This function writes data from a SparkDF into a Hive table, allowing
    optional handling of missing columns. The table's column order is ensured to
    match that of the DataFrame.

    Parameters
    ----------
    spark
        Active SparkSession.
    df
        SparkDF containing data to be written.
    table_name
        Name of the Hive table to write data into.
    overwrite
        If True, existing data in the table will be overwritten,
        by default False.
    fill_missing_cols
        If True, missing columns will be filled with nulls, by default False.
    """
    logger.info(f'Preparing to write data to {table_name}.')

    # Validate SparkDF before writing
    assert_df_is_not_empty(df, f'Cannot write an empty SparkDF to {table_name}')

    try:
        table_columns = spark.read.table(table_name).columns
    except AnalysisException:
        logger.error(
            (
                f'Error reading table {table_name}. '
                f'Make sure the table exists and you have access to it.'
            ),
        )

        raise

    if fill_missing_cols:
        missing_columns = list(set(table_columns) - set(df.columns))

        for col in missing_columns:
            df = df.withColumn(col, F.lit(None))
    else:
        # Validate schema before writing
        if set(table_columns) != set(df.columns):
            msg = (
                f"SparkDF schema does not match table {table_name} "
                f"schema and 'fill_missing_cols' is False."
            )
            raise ValueError(msg)

    df = df.select(table_columns)

    try:
        df.write.insertInto(table_name, overwrite)
        logger.info(f'Successfully wrote data to {table_name}.')
    except Exception:
        logger.error(f'Error writing data to {table_name}.')
        raise


def write_and_read_hive_table(
    spark: SparkSession,
    df: SparkDF,
    table_name: str,
    database: str,
    filter_id: Union[int, str],
    filter_col: str = 'run_id',
    fill_missing_cols: bool = False,
) -> SparkDF:
    """Write a SparkDF to an existing Hive table and then read it back.

    Parameters
    ----------
    spark
        Active SparkSession.
    df
        The SparkDF to be written to the Hive table.
    table_name
        The name of the Hive table to write to and read from.
    database
        The Hive database name.
    filter_id : Union[int, str]
        The identifier to filter on when reading data back from the Hive table.
    filter_col
        The column name to use for filtering data when reading back from
        the Hive table, by default 'run_id'.
    fill_missing_cols
        If True, missing columns in the DataFrame will be filled with nulls
        when writing to the Hive table, by default False.

    Returns
    -------
    SparkDF
        The DataFrame read from the Hive table.

    Notes
    -----
    This function assumes the Hive table already exists. The DataFrame `df`
    should have the same schema as the Hive table for the write to succeed.

    The function allows for more effective memory management when dealing
    with large PySpark DataFrames by leveraging Hive's on-disk storage.

    Predicate pushdown is used when reading the data back into a PySpark
    DataFrame, minimizing the memory usage and optimizing the read
    operation.

    As part of the design, there is always a column called filter_col in the
    DataFrame and Hive table to track pipeline runs.

    The Hive table contains all the runs, and we only read back the run that we
    just wrote to the Hive Table using the `filter_id` parameter. If no
    `filter_col` is specified, 'run_id' is used as default.
    """
    try:
        # Check for existence of the Hive table
        if not spark.catalog.tableExists(database, table_name):
            msg = (
                f'The specified Hive table {database}.'
                f'{table_name} does not exist.'
            )
            raise ValueError(
                msg,
            )

        # Ensure the filter_col exists in DataFrame
        if filter_col not in df.columns:
            msg = (
                "The provided DataFrame doesn't contain the "
                f"specified filter column: {filter_col}"
            )
            raise ValueError(
                msg,
            )

        # Write DataFrame to Hive using the helper function
        insert_df_to_hive_table(
            spark,
            df,
            f'{database}.{table_name}',
            fill_missing_cols=fill_missing_cols,
        )

        # Read DataFrame back from Hive with filter condition
        df_read = load_and_validate_table(
            spark,
            f'{database}.{table_name}',
            skip_validation=False,
            err_msg=None,
            filter_cond=f"{filter_col} = '{filter_id}'",
        )
        return df_read

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        raise
