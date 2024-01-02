"""Write outputs on CDSW."""
import logging
from typing import Union

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from py4j.java_gateway import java_import

from rdsa_utils.exceptions import (
    ColumnNotInDataframeError,
    DataframeEmptyError,
    TableNotFoundError,
)
from rdsa_utils.helpers.pyspark import is_df_empty
from rdsa_utils.cdsw.io.input import load_and_validate_table

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

    Raises
    ------
    AnalysisException
        If there's an error reading the table. This can occur if the table
        doesn't exist or if there's no access to it.
    ValueError
        If the SparkDF schema does not match the Hive table schema and
        'fill_missing_cols' is set to False.
    Exception
        For other general exceptions when writing data to the table.
    """
    logger.info(f'Preparing to write data to {table_name}.')

    # Validate SparkDF before writing
    if is_df_empty(df):
        msg = f'Cannot write an empty SparkDF to {table_name}'
        raise DataframeEmptyError(
            msg,
        )

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

    Raises
    ------
    ValueError
        If the specified Hive table does not exist in the given database or
        if the provided DataFrame doesn't contain the specified filter column.
    Exception
        For general exceptions encountered during execution.

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
            raise TableNotFoundError(
                msg,
            )

        # Ensure the filter_col exists in DataFrame
        if filter_col not in df.columns:
            msg = (
                "The provided DataFrame doesn't contain the "
                f"specified filter column: {filter_col}"
            )
            raise ColumnNotInDataframeError(
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


def save_csv_to_hdfs(
    spark: SparkSession,
    df: SparkDF,
    file_name: str,
    file_path: str,
    overwrite: bool = True,
    coalesce_data: bool = True,
) -> None:
    """Save DataFrame as CSV on HDFS, with optional coalescing.

    This function saves a PySpark DataFrame to HDFS in CSV format.
    Coalescing the DataFrame into a single partition is optional.

    Without coalescing, the DataFrame is saved in multiple parts, and
    these parts are merged into a single file. However, this does not
    guarantee the order of rows as in the original DataFrame.

    Parameters
    ----------
    spark
        Active SparkSession instance.
    df
        PySpark DataFrame to save.
    file_name
        The name of the CSV file. The file name must include
        the ".csv" extension.
    file_path
        The path in HDFS where the CSV file should be saved.
    overwrite
        If True, any existing file with the same name in the
        given path will be overwritten.
        If False, an exception will be raised if a file with
        the same name already exists.
    coalesce_data
        If True, coalesces the DataFrame into a single partition
        before saving. This preserves the order of rows but may
        impact performance for large DataFrames.

    Raises
    ------
    ValueError
        If the provided file_name doesn't end with ".csv".
    IOError
        If `overwrite` is False and the file already exists.

    Examples
    --------
    >>> save_to_hdfs_csv(df, "example.csv", "/user/hadoop/data/")
    """
    if not file_name.endswith('.csv'):
        msg = "The file_name must end with '.csv' extension."
        raise ValueError(msg)

    destination_path = f"{file_path.rstrip('/')}/{file_name}"

    # Access Hadoop FileSystem
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'org.apache.hadoop.io.IOUtils')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration(),
    )

    if not overwrite and fs.exists(spark._jvm.Path(destination_path)):
        msg = (
            f"File '{destination_path}' already exists "
            "and overwrite is set to False."
        )
        raise IOError(
            msg,
        )

    logger.info(f'Saving DataFrame to {file_name} in HDFS at {file_path}')

    # Coalesce the DataFrame to a single partition if required
    if coalesce_data:
        df = df.coalesce(1)

    # Temporary directory for saving the data
    temp_path = f"{file_path.rstrip('/')}/{file_name}_temp"

    # Save the DataFrame to HDFS in CSV format
    df.write.csv(temp_path, header=True, mode='overwrite')
    logger.info(f'DataFrame saved temporarily at {temp_path}')

    # HDFS Path objects for source and destination
    destination_file_path = spark._jvm.Path(destination_path)
    temp_dir_path = spark._jvm.Path(temp_path)

    # Open the merged file for writing
    merged_file_path = spark._jvm.Path(temp_path + '/merged.csv')
    merged_file = fs.create(merged_file_path)

    try:
        first_file = True
        for file_status in fs.listStatus(temp_dir_path):
            file_name = file_status.getPath().getName()
            if file_name.startswith('part-'):
                part_file = fs.open(file_status.getPath())
                try:
                    # Skip header for non-first files
                    if not first_file:
                        part_file.readLine()
                    else:
                        first_file = False
                    spark._jvm.org.apache.hadoop.io.IOUtils.copyBytes(
                        part_file,
                        merged_file,
                        spark._jsc.hadoopConfiguration(),
                        False,
                    )
                finally:
                    part_file.close()
    finally:
        merged_file.close()

    # Check if the destination file already exists and delete if necessary
    if fs.exists(destination_file_path):
        if not fs.delete(destination_file_path, False):
            logger.error(
                f'Failed to delete existing file at {destination_path}',
            )
            msg = f'Could not delete existing file at {destination_path}'
            raise IOError(
                msg,
            )

    # Rename merged file to final destination
    if not fs.rename(merged_file_path, destination_file_path):
        logger.error(f'Failed to rename file to {destination_path}')
        msg = f'Could not rename file to {destination_path}'
        raise IOError(msg)
    else:
        logger.info(f'Renamed the file to {file_name}')

    # Clean up temporary directory
    fs.delete(spark._jvm.Path(temp_path), True)
    logger.info(f'Temporary directory {temp_path} deleted')
