"""Write outputs on CDP."""

import logging
import uuid
from typing import Union

import boto3
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from rdsa_utils.cdp.helpers.hdfs_utils import delete_path, file_exists, rename
from rdsa_utils.cdp.helpers.s3_utils import (
    copy_file,
    delete_folder,
    list_files,
    remove_leading_slash,
    validate_bucket_name,
    validate_s3_file_path,
)
from rdsa_utils.cdp.io.input import load_and_validate_table
from rdsa_utils.exceptions import (
    ColumnNotInDataframeError,
    DataframeEmptyError,
    TableNotFoundError,
)
from rdsa_utils.helpers.pyspark import is_df_empty

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
    logger.info(f"Preparing to write data to {table_name}.")

    # Validate SparkDF before writing
    if is_df_empty(df):
        msg = f"Cannot write an empty SparkDF to {table_name}"
        raise DataframeEmptyError(
            msg,
        )

    try:
        table_columns = spark.read.table(table_name).columns
    except AnalysisException:
        logger.error(
            (
                f"Error reading table {table_name}. "
                f"Make sure the table exists and you have access to it."
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
        logger.info(f"Successfully wrote data to {table_name}.")
    except Exception:
        logger.error(f"Error writing data to {table_name}.")
        raise


def write_and_read_hive_table(
    spark: SparkSession,
    df: SparkDF,
    table_name: str,
    database: str,
    filter_id: Union[int, str],
    filter_col: str = "run_id",
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
    filter_id
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
            msg = f"The specified Hive table {database}.{table_name} does not exist."
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
            f"{database}.{table_name}",
            fill_missing_cols=fill_missing_cols,
        )

        # Read DataFrame back from Hive with filter condition
        df_read = load_and_validate_table(
            spark,
            f"{database}.{table_name}",
            skip_validation=False,
            err_msg=None,
            filter_cond=f"{filter_col} = '{filter_id}'",
        )
        return df_read

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


def save_csv_to_hdfs(
    df: SparkDF,
    file_name: str,
    file_path: str,
    overwrite: bool = True,
) -> None:
    """Save DataFrame as CSV on HDFS, coalescing to a single partition.

    This function saves a PySpark DataFrame to HDFS in CSV format. By
    coalescing the DataFrame into a single partition before saving, it
    accomplishes two main objectives:

    1. Single Part File: The output is a single CSV file rather than
       multiple part files. This method reduces complexity and
       cuts through the clutter of multi-part files, offering users
       and systems a more cohesive and hassle-free experience.

    2. Preserving Row Order: Coalescing into a single partition maintains
       the order of rows as they appear in the DataFrame. This is essential
       when the row order matters for subsequent processing or analysis.
       It's important to note, however, that coalescing can have performance
       implications for very large DataFrames by concentrating
       all data processing on a single node.

    Parameters
    ----------
    df
        PySpark DataFrame to be saved.
    file_name
        Name of the CSV file. Must include the ".csv" extension.
    file_path
        HDFS path where the CSV file should be saved.
    overwrite
        If True, overwrite any existing file with the same name. If False
        and the file exists, the function will raise an error.

    Raises
    ------
    ValueError
        If the file_name does not end with ".csv".
    IOError
        If overwrite is False and the target file already exists.

    Examples
    --------
    Saving to an S3 bucket using the `s3a://` scheme:

    ```python
    # Assume `df` is a pre-defined PySpark DataFrame
    file_name = "data_output.csv"
    file_path = "s3a://my-bucket/data_folder/"
    save_csv_to_hdfs(df, file_name, file_path, overwrite=True)
    ```

    Saving to a normal HDFS path:

    ```python
    # Assume `df` is a pre-defined PySpark DataFrame
    file_name = "data_output.csv"
    file_path = "/user/hdfs/data_folder/"
    save_csv_to_hdfs(df, file_name, file_path, overwrite=True)
    ```
    """
    if not file_name.endswith(".csv"):
        error_msg = "The file_name must end with '.csv' extension."
        raise ValueError(error_msg)

    destination_path = f"{file_path.rstrip('/')}/{file_name}"

    if not overwrite and file_exists(destination_path):
        error_msg = (
            f"File '{destination_path}' already exists "
            "and overwrite is set to False."
        )
        raise IOError(error_msg)

    logger.info(f"Saving DataFrame to {file_name} in HDFS at {file_path}")

    # Coalesce the DataFrame to a single partition
    df = df.coalesce(1)

    # Temporary path for saving the single part file
    temp_path = f"{file_path.rstrip('/')}/temp_{file_name}"

    # Save the DataFrame to HDFS in CSV format in a temporary directory
    df.write.csv(temp_path, header=True, mode="overwrite")

    # Identify the part file: pattern matching for the single part file
    part_file = f"{temp_path}/part-00000*.csv"

    # Rename the part file to the final file name
    if not rename(part_file, destination_path, overwrite):
        error_msg = f"Failed to rename the part file to '{destination_path}'"
        raise IOError(error_msg)

    logger.info(f"DataFrame successfully saved to {destination_path}")

    # Clean up the temporary directory
    delete_path(temp_path)
    logger.info(f"Temporary directory {temp_path} deleted")


def save_csv_to_s3(
    df: SparkDF,
    bucket_name: str,
    file_name: str,
    file_path: str,
    s3_client: boto3.client,
    overwrite: bool = True,
) -> None:
    """Save DataFrame as CSV on S3, coalescing to a single partition.

    This function saves a PySpark DataFrame to S3 in CSV format. By
    coalescing the DataFrame into a single partition before saving, it
    accomplishes two main objectives:

    1. Single Part File: The output is a single CSV file rather than
       multiple part files. This method reduces complexity and
       cuts through the clutter of multi-part files, offering users
       and systems a more cohesive and hassle-free experience.

    2. Preserving Row Order: Coalescing into a single partition maintains
       the order of rows as they appear in the DataFrame. This is essential
       when the row order matters for subsequent processing or analysis.
       It's important to note, however, that coalescing can have performance
       implications for very large DataFrames by concentrating
       all data processing on a single node.

    Parameters
    ----------
    df
        PySpark DataFrame to be saved.
    bucket_name
        The name of the S3 bucket where the CSV file should be saved.
    file_name
        Name of the CSV file. Must include the ".csv" extension.
    file_path
        S3 path where the CSV file should be saved.
    s3_client
        The boto3 S3 client instance.
    overwrite
        If True, overwrite any existing file with the same name. If False
        and the file exists, the function will raise an error.

    Raises
    ------
    ValueError
        If the file_name does not end with ".csv".
    InvalidBucketNameError
        If the bucket name does not meet AWS specifications.
    InvalidS3FilePathError
        If the file_path contains an S3 URI scheme like 's3://' or 's3a://'.
    IOError
        If overwrite is False and the target file already exists.

    Examples
    --------
    Saving to an S3 bucket:

    ```python
    # Assume `df` is a pre-defined PySpark DataFrame
    file_name = "data_output.csv"
    file_path = "data_folder/"
    s3_client = boto3.client('s3')
    save_csv_to_s3(
        df,
        'my-bucket',
        file_name,
        file_path,
        s3_client,
        overwrite=True
    )
    ```
    """
    bucket_name = validate_bucket_name(bucket_name)
    file_path = validate_s3_file_path(file_path, allow_s3_scheme=False)
    file_path = remove_leading_slash(file_path)

    if not file_name.endswith(".csv"):
        error_msg = "The file_name must end with '.csv' extension."
        raise ValueError(error_msg)

    destination_path = f"{file_path.rstrip('/')}/{file_name}"

    if not overwrite and file_exists(s3_client, bucket_name, destination_path):
        error_msg = (
            f"File '{destination_path}' already exists "
            "and overwrite is set to False."
        )
        raise IOError(error_msg)

    logger.info(
        f"Saving DataFrame to {file_name} in S3 at s3://{bucket_name}/{file_path}",
    )

    # Coalesce the DataFrame to a single partition
    df = df.coalesce(1)

    # Temporary S3 path for saving the single part file
    temp_path = f"{file_path.rstrip('/')}/temp_{uuid.uuid4().hex}_{file_name}"

    # Save the DataFrame to S3 in CSV format in a temporary directory
    df.write.csv(
        f"s3a://{bucket_name}/{temp_path}",
        header=True,
        mode="overwrite",
    )

    # Identify the part file using the list_files helper function
    part_file_prefix = f"{temp_path}/part-00000"
    part_files = list_files(s3_client, bucket_name, part_file_prefix)
    if not part_files:
        error_msg = "No part files found in the temporary directory."
        raise IOError(error_msg)

    # Get the first part file from the list
    # Since the DataFrame is coalesced to a single partition, there should
    # only be one part file
    part_file_key = part_files[0]

    # Rename the part file to the final file name
    if not copy_file(
        s3_client,
        bucket_name,
        part_file_key,
        bucket_name,
        destination_path,
        overwrite,
    ):
        error_msg = f"Failed to rename the part file to '{destination_path}'"
        raise IOError(error_msg)

    logger.info(
        f"DataFrame successfully saved to s3://{bucket_name}/{destination_path}",
    )

    # Clean up the temporary directory
    delete_folder(s3_client, bucket_name, temp_path)
