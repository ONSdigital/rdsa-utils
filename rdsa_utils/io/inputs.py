"""Functions that primarily deal with loading or reading data."""
import logging

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from rdsa_utils.helpers.helpers_spark import extract_database_name

logger = logging.getLogger(__name__)


def load_and_validate_table(
    spark: SparkSession,
    table_name: str,
    skip_validation: bool = False,
    err_msg: str = None,
    filter_cond: str = None,
) -> SparkDF:
    """Load a table and validate if it is not empty after applying a filter.

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

    Returns
    -------
    SparkDF
        Loaded SparkDF if validated, subject to options above.

    Raises
    ------
    PermissionError
        If accessing the table fails.
    """
    try:
        df = spark.read.table(table_name)
        logger.info(f'Successfully loaded table {table_name}.')
    except Exception as e:
        db_name, _ = extract_database_name(spark, table_name)
        db_err = (
            f'Error accessing {table_name} in the {db_name} database. '
            'Check you have access to the database and that '
            'the table name is correct.'
        )
        logger.error(db_err)
        raise PermissionError(db_err) from e

    if not skip_validation:
        if df.rdd.isEmpty():
            err_msg = err_msg or f'Table {table_name} is empty.'
            raise ValueError(err_msg)

    if filter_cond:
        df = df.filter(filter_cond)
        if not skip_validation and df.rdd.isEmpty():
            err_msg = (
                err_msg
                or f'Table {table_name} is empty after applying '
                f'filter condition [{filter_cond}].'
            )
            raise ValueError(err_msg)

    logger.info(
        (
            f'Loaded and validated table {table_name}. '
            f'Filter condition applied: {filter_cond}'
        ),
    )

    return df
