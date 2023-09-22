"""Module containing generic input functionality code."""
import json
import logging
from pathlib import Path

import tomli
import yaml
from cloudpathlib import CloudPath
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from rdsa_utils.exceptions import DataframeEmptyError
from rdsa_utils.helpers.pyspark import extract_database_name
from rdsa_utils.typing import Config

logger = logging.getLogger(__name__)


def parse_json(data: str) -> Config:
    """Parse JSON formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard JSON-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.

    Raises
    ------
    json.decoder.JSONDecodeError
        If the string format of config_overrides cannot be decoded by
        json.loads (i.e. converted to a dictionary).
    """
    # Attempt to convert string to dictionary using json module. If this cannot
    # be done, capture error and log a useful description on what needs to be
    # changed before raising the error
    try:
        return json.loads(data)

    except json.decoder.JSONDecodeError:
        msg = """
        Cannot convert config_overrides parameter to a dictionary.

        Ensure that argument input is of form:

        '{"var1": "value1", "var2": {"var3": 1.1}, "var4": [1, 2, 3], ... }'

        where single quote is used around entire entry and double quotes are
        used for any string values within the argument.
        """
        logger.error(msg)
        raise json.decoder.JSONDecodeError(msg)  # noqa: B904


def parse_toml(data: str) -> Config:
    """Parse TOML formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard TOML-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.
    """
    return tomli.loads(data)


def parse_yaml(data: str) -> Config:
    """Parse YAML formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard YAML-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.
    """
    return yaml.safe_load(data)


def read_file(file: CloudPath | Path) -> str:
    """Load contents of specified file.

    Parameters
    ----------
    file
        The absolute file path of the file to be read.

    Returns
    -------
    str
        The contents of the provided file.

    Raises
    ------
    FileNotFoundError
        If the provided file does not exist.
    """
    if file.exists():
        return file.read_text()
    else:
        msg = f'{file=} cannot be found.'
        logger.error(msg)
        raise FileNotFoundError(msg)


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
