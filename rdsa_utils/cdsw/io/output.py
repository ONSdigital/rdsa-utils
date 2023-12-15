"""Write outputs to HDFS on CDSW."""
import logging

from py4j.java_gateway import java_import
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

def save_csv_to_hdfs(
    spark: SparkSession,
    df: DataFrame,
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
    spark : SparkSession
        Active SparkSession instance.
    df : DataFrame
        The PySpark DataFrame to save.
    file_name : str
        The name of the CSV file. The file name must include
        the ".csv" extension.
    file_path : str
        The path in HDFS where the CSV file should be saved.
    overwrite : bool, default True
        If True, any existing file with the same name in the
        given path will be overwritten.
        If False, an exception will be raised if a file with
        the same name already exists.
    coalesce_data : bool, default True
        If True, coalesces the DataFrame into a single partition
        before saving. This preserves the order of rows but may
        impact performance for large DataFrames.

    Returns
    -------
    None

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
