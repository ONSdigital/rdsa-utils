"""
Hive Data Dictionary Generator Extraction Module

This module allows users to create data dictionaries for Hive tables by parsing the DDL scripts and exporting the information in a structured format.

Main function:
    read_ddl_scripts: Read the DDL scripts from a given file path.
    replace_variables: Replace the f-string expressions in the DDL script with their values.
    extract_table_information_hive: Extracts table information (table name, column name, data type, constraints, description, storage type, partition columns) from Hive DDL scripts.

Helper functions:
    _extract_column_info: Extracts column information such as column name, data type, constraints, and description from a column line in a Hive DDL script.
    _extract_columns_part: Extracts the columns part of a Hive DDL 'CREATE TABLE' statement.
    _extract_database_and_table_name: Extracts the database and table names from a Hive DDL 'CREATE TABLE' statement.
    _extract_partition_columns: Extracts partition column names from a Hive DDL 'CREATE TABLE' statement.
    _extract_storage_type: Extracts the storage type from a Hive DDL 'CREATE TABLE' statement.
"""
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class TableInformation:
    """
    A data class representing table information extracted from Hive DDL scripts.

    Attributes
    ----------
    database_name : Optional[str]
        The name of the database, or None if not specified.
    table_name : str
        The name of the table.
    column_name : str
        The name of the column.
    data_type : str
        The data type of the column.
    constraints : str
        Column constraints, e.g., 'NOT NULL'.
    description : str
        The description of the column, if provided.
    storage_type : Optional[str]
        The storage type of the table, e.g., 'textfile', 'orc', or None if not specified.
    partition_columns : str
        A comma-separated string of partition column names, if any.
    """

    database_name: Optional[str]
    table_name: str
    column_name: str
    data_type: str
    constraints: str
    description: str
    storage_type: Optional[str]
    partition_columns: str


@dataclass
class RegexPatterns:
    """
    A data class containing regex patterns for extracting table information from Hive DDL scripts.

    Attributes
    ----------
    create_table : str
        A regex pattern for matching 'CREATE TABLE' statements.
    table_name : str
        A regex pattern for extracting database and table names from 'CREATE TABLE' statements.
    columns_part : str
        A regex pattern for extracting the columns part from 'CREATE TABLE' statements.
    storage_type : str
        A regex pattern for extracting the storage type from 'CREATE TABLE' statements.
    partition_columns : str
        A regex pattern for extracting partition column names from 'CREATE TABLE' statements.
    column_info : str
        A regex pattern for extracting column information (name, data type, constraints, and description) from column lines.
    """

    create_table: str = r"CREATE TABLE (?:IF NOT EXISTS )?.*?(?:;|$)"
    table_name: str = (
        r"CREATE TABLE (?:IF NOT EXISTS )?(?:`?([^\s.]+?)`?\.)?`?([^\s.]+?)`?\s*\("
    )
    columns_part: str = r"\((.*?)\)"
    storage_type: str = r"STORED AS\s+(\w+)"
    partition_columns: str = r"PARTITION(?:ED)? BY\s*\((.+)\)"
    column_info: str = (
        r"(\w+)\s+(\w+)(?:\((.*?)\))?(?:\s+(NOT NULL))?(?:\s*,?\s*--\s*(.*))?"
    )


def read_ddl_scripts(file_path: str) -> str:
    """
    Read the DDL scripts from a given file path.

    Parameters
    ----------
    file_path : str
        The file path containing the DDL scripts.

    Returns
    -------
    str
        The contents of the DDL scripts file.
    """

    with open(file_path, "r") as file:
        sql_scripts = file.read()
    return sql_scripts


def replace_variables(ddl_script: str, script_globals: dict) -> str:
    """
    Replace the f-string expressions in the DDL script with their values.

    Parameters
    ----------
    ddl_script : str
        The DDL script containing f-string expressions.
    script_globals : dict
        A dictionary of global variables from the script.

    Returns
    -------
    str
        The DDL script with f-string expressions replaced by their values.
    """
    ddl_script_replaced = ddl_script.format(**script_globals)
    return ddl_script_replaced


def _extract_database_and_table_name(
    script: str, patterns: RegexPatterns
) -> Tuple[Optional[str], str]:
    """
    Extract the database and table name from a 'CREATE TABLE' statement.

    Parameters
    ----------
    script : str
        The 'CREATE TABLE' statement.
    patterns : RegexPatterns
        An instance of the RegexPatterns data class containing regex patterns for extraction.

    Returns
    -------
    Tuple[Optional[str], str]
        A tuple containing the database name (or None if not specified) and table name.
    """
    table_match = re.search(patterns.table_name, script, re.IGNORECASE)
    if table_match:
        database_name = table_match.group(1) if table_match.group(1) else None
        table_name = table_match.group(2)
        return database_name, table_name
    return None, None


def _extract_columns_part(script: str, patterns: RegexPatterns) -> str:
    """
    Extract the columns part of a 'CREATE TABLE' statement.

    Parameters
    ----------
    script : str
        The 'CREATE TABLE' statement.
    patterns : RegexPatterns
        An instance of the RegexPatterns data class containing regex patterns for extraction.

    Returns
    -------
    str
        The columns part of the 'CREATE TABLE' statement.
    """
    return re.search(patterns.columns_part, script, re.DOTALL).group(1)


def _extract_storage_type(script: str, patterns: RegexPatterns) -> Optional[str]:
    """
    Extract the storage type from a 'CREATE TABLE' statement.

    Parameters
    ----------
    script : str
        The 'CREATE TABLE' statement.
    patterns : RegexPatterns
        An instance of the RegexPatterns data class containing regex patterns for extraction.

    Returns
    -------
    Optional[str]
        The storage type of the table, or None if not specified.
    """
    storage_type_match = re.search(patterns.storage_type, script, re.IGNORECASE)
    return storage_type_match.group(1).lower() if storage_type_match else None


def _extract_partition_columns(script: str, patterns: RegexPatterns) -> str:
    """
    Extract partition columns from a 'CREATE TABLE' statement.

    Parameters
    ----------
    script : str
        The 'CREATE TABLE' statement.
    patterns : RegexPatterns
        An instance of the RegexPatterns data class containing regex patterns for extraction.

    Returns
    -------
    str
        A comma-separated string of partition column names, or an empty string if no partition columns are specified.
    """
    partition_match = re.search(patterns.partition_columns, script, re.IGNORECASE)
    if partition_match:
        partition_columns = partition_match.group(1)
        partition_columns_list = [
            partition_col.strip().split()[0]
            for partition_col in partition_columns.split(",")
        ]
        return ", ".join(partition_columns_list)
    else:
        return ""


def _extract_column_info(column_match: re.Match) -> Tuple[str, str, str, str]:
    """
    Extract column information (name, data type, constraints, and description) from a column match object.

    Parameters
    ----------
    column_match : re.Match
        A Match object from the column line of a 'CREATE TABLE' statement.

    Returns
    -------
    Tuple[str, str, str, str]
        A tuple containing the column name, data type, constraints, and description.
    """
    column_name = column_match.group(1)
    data_type = column_match.group(2)
    constraints = column_match.group(4) if column_match.group(4) else ""
    description = column_match.group(5) if column_match.group(5) else ""
    return column_name, data_type, constraints, description


def extract_table_information_hive(sql_scripts: str) -> List[TableInformation]:
    """
    Extract table information (table name, column name, data type, constraints, description, storage type, partition columns) from the Hive DDL scripts.

    Parameters
    ----------
    sql_scripts : str
        The Hive DDL scripts as a string.

    Returns
    -------
    List[TableInformation]
        A list of TableInformation instances containing table information (database name, table name, column name, data type, constraints, description, storage type, partition columns).
    """
    patterns = RegexPatterns()
    table_scripts = re.findall(
        patterns.create_table, sql_scripts, re.DOTALL | re.IGNORECASE
    )
    table_info = []

    for script in table_scripts:
        database_name, table_name = _extract_database_and_table_name(script, patterns)
        columns_part = _extract_columns_part(script, patterns)
        storage_type = _extract_storage_type(script, patterns)
        partition_columns = _extract_partition_columns(script, patterns)

        column_lines = columns_part.strip().split("\n")

        for line in column_lines:
            column_match = re.search(patterns.column_info, line.strip(), re.IGNORECASE)

            if column_match:
                column_name, data_type, constraints, description = _extract_column_info(
                    column_match
                )

                table_info.append(
                    TableInformation(
                        database_name,
                        table_name,
                        column_name,
                        data_type,
                        constraints,
                        description,
                        storage_type,
                        partition_columns,
                    )
                )

    return table_info
