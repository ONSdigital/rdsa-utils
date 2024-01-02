"""Read inputs on CDSW."""
import logging
from typing import Tuple

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from rdsa_utils.exceptions import DataframeEmptyError

logger = logging.getLogger(__name__)

def get_current_database(spark: SparkSession) -> str:
    """Retrieve the current database from the active SparkSession."""
    return spark.sql('SELECT current_database()').collect()[0][
        'current_database()'
    ]


def extract_database_name(
    spark: SparkSession, long_table_name: str,
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
    spark : SparkSession
        Active SparkSession.
    long_table_name : str
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
    parts = long_table_name.split('.')

    if len(parts) == 3:  # GCP format: project.database.table
        _, db_name, table_name = parts

    elif len(parts) == 2:  # Common format: database.table
        db_name, table_name = parts

    elif len(parts) == 1:  # Only table name is given
        db_name = get_current_database(spark)
        table_name = parts[0]

    else:
        error_msg = (
            f'Table name {long_table_name} is incorrectly formatted. '
            'Expected formats: <project>.<database>.<table>, '
            '<database>.<table>, or <table>'
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        f'Extracted database name: {db_name}, table name: '
        f'{table_name} from {long_table_name}',
    )
    return db_name, table_name


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
        If there's an issue accessing the table or if the table
        does not exist in the specified database.
    ValueError
        If the table is empty after loading, or if it becomes
        empty after applying a filter condition.
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
            raise DataframeEmptyError(err_msg)

    if filter_cond:
        df = df.filter(filter_cond)
        if not skip_validation and df.rdd.isEmpty():
            err_msg = (
                err_msg
                or f'Table {table_name} is empty after applying '
                f'filter condition [{filter_cond}].'
            )
            raise DataframeEmptyError(err_msg)

    logger.info(
        (
            f'Loaded and validated table {table_name}. '
            f'Filter condition applied: {filter_cond}'
        ),
    )

    return df
