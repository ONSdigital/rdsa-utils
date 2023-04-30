"""
Data Dictionary Generator Extraction Module

This module allows users to create data dictionaries for Hive & Non-Hive tables by parsing the DDL scripts 
and exporting the information in a structured format.

Main function:
    read_ddl_scripts: Read the DDL scripts from a given file path.
    replace_variables: Replace the f-string expressions in the DDL script with their values.
    extract_data_dictionary: Extracts table information from a DDL script and returns a list of TableInformation objects. 
    extract_data_dictionaries_from_multiple_tables: Process multiple table definitions in a DDL script and return a list of TableInformation objects.

Helper functions:
    remove_multiline_comments: Remove multiline comments from a DDL script.
    extract_contents_inside_parentheses: Extract the content inside the outermost parentheses in a DDL script.
    extract_column_description: Extract the column name and its description from a line in a DDL script. 
    extract_column_descriptions: Extract column descriptions from a DDL script.
"""
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from simple_ddl_parser import DDLParser


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


def remove_multiline_comments(ddl_script: str) -> str:
    """
    Remove multiline comments from a DDL script.

    Parameters
    ----------
    ddl_script : str
        The DDL script from which multiline comments should be removed.

    Returns
    -------
    str
        The DDL script with multiline comments removed.
    """
    return re.sub(r"/\*.*?\*/", " ", ddl_script, flags=re.DOTALL)


def extract_content_inside_parentheses(ddl_script: str) -> str:
    """
    Extract the content inside the outermost parentheses in a DDL script.

    Parameters
    ----------
    ddl_script : str
        The DDL script from which to extract the content inside parentheses.

    Returns
    -------
    str
        The content inside the outermost parentheses in the DDL script.
    """
    content = ""
    parenthesis_count = 0
    start_copying = False
    for char in ddl_script:
        if char == "(":
            parenthesis_count += 1
            if (
                start_copying
            ):  # Only copy the opening parenthesis if we've already started copying
                content += char
            else:
                start_copying = True
        elif char == ")":
            parenthesis_count -= 1
            if parenthesis_count == 0:
                start_copying = False
            else:
                content += char
        elif start_copying and parenthesis_count > 0:
            content += char
    return content.strip(";")  # Remove any trailing semi-colon


def extract_column_description(line: str) -> Optional[Dict[str, str]]:
    """
    Extract the column name and its description from a line in a DDL script.

    Parameters
    ----------
    line : str
        A line in a DDL script containing a column definition and its description.

    Returns
    -------
    dict or None
        A dictionary with keys 'column_name' and 'column_description' if the line
        contains a valid column definition and description, None otherwise.
    """
    match = re.search(r"^\s*(\w+)\s+\w+.*?--(.*)", line)
    if match:
        column_name = match.group(1)
        description = match.group(2).strip()
        return {"column_name": column_name, "column_description": description}
    return None


def extract_column_descriptions(ddl_script: str) -> List[Dict[str, str]]:
    """
    Extract column descriptions from a DDL script.

    Parameters
    ----------
    ddl_script : str
        The DDL script from which to extract column descriptions.

    Returns
    -------
    list of dict
        A list of dictionaries, each containing the column name and its description
        with keys 'column_name' and 'column_description'.
    """
    ddl_script = remove_multiline_comments(ddl_script)
    content = extract_content_inside_parentheses(ddl_script)
    lines = content.split("\n")

    column_descriptions = []
    for line in lines:
        description = extract_column_description(line)
        if description:
            column_descriptions.append(description)

    return column_descriptions


def extract_data_dictionary(
    ddl_script: str,
    is_hive: bool = False,
    description_dict: Optional[List[Dict[str, str]]] = None,
) -> List[TableInformation]:
    """
    Extracts table information from a DDL script and returns a list of TableInformation objects.

    Parameters
    ----------
    ddl_script : str
        The DDL script to parse.
    is_hive : bool, optional, default: False
        If True, the DDL script is assumed to be in HiveQL format. If False, a SQL-like format is assumed.
    description_dict : list of dict, optional, default: None
        A list of dictionaries with column names as keys and descriptions as values. If not provided, the function attempts
        to extract column descriptions from the DDL script itself.

    Returns
    -------
    list of TableInformation
        A list of TableInformation objects, each representing the extracted information for a single column in the table.

    Examples
    --------
    >>> ddl_script = '''
    ... CREATE TABLE IF NOT EXISTS my_database.my_table (
    ...     id INT, -- Unique identifier for the record
    ...     name STRING, -- Full name of the person
    ...     age INT, -- Age of the person
    ...     city STRING -- City where the person lives
    ... ) ROW FORMAT DELIMITED
    ... FIELDS TERMINATED BY ','
    ... STORED AS TEXTFILE;
    ... '''
    >>> table_information_list = extract_data_dictionary(ddl_script, is_hive=True)
    """
    # Parse the DDL script using DDLParser with the appropriate output mode based on the is_hive flag
    if is_hive:
        parse_results = DDLParser(ddl_script).run(output_mode="hql")
    else:
        parse_results = DDLParser(ddl_script).run()

    # If no parse results are found, return an empty list
    if not parse_results:
        return []

    result = parse_results[0]

    # If no description dictionary is provided, extract column descriptions from the DDL script
    if description_dict is None:
        column_descriptions = extract_column_descriptions(ddl_script)
        description_dict = {
            desc["column_name"]: desc["column_description"]
            for desc in column_descriptions
        }
    else:
        description_dict = {
            desc["column_name"]: desc["column_description"] for desc in description_dict
        }

    table_information_list = []

    # Iterate through the columns in the parsed results
    for column in result["columns"]:
        # Create a list of constraints for each column
        constraints_list = []
        if column["unique"]:
            constraints_list.append("unique")
        if column["references"]:
            constraints_list.append(
                f"foreign_key({column['references']['table']}.{column['references']['column']})"
            )
        if column["check"]:
            constraints_list.append(f"check({column['check']})")
        constraints = ", ".join(constraints_list)

        # Extract partition column names
        partition_columns = ", ".join(
            [col["name"] for col in result.get("partitioned_by", [])]
        )

        # Create a TableInformation object for each column and append it to the table_information_list
        table_information = TableInformation(
            database_name=result["schema"],
            table_name=result["table_name"],
            column_name=column["name"],
            data_type=column["type"],
            constraints=constraints,
            description=description_dict.get(column["name"], None),
            storage_type=result.get("stored_as", None),
            partition_columns=partition_columns,
        )

        table_information_list.append(table_information)

    return table_information_list


def extract_data_dictionaries_from_multiple_tables(
    ddl_scripts: str, is_hive: bool = False
) -> List[TableInformation]:
    """
    Process multiple table definitions in a DDL script and return a list of TableInformation objects.

    Parameters
    ----------
    ddl_scripts : str
        The DDL script containing multiple table definitions.
    is_hive : bool, optional
        Set to True if the DDL script is in Hive syntax, by default False.

    Returns
    -------
    List[TableInformation]
        A list of TableInformation objects for all tables in the DDL script.
    """
    # Find all tables in script
    create_table_regex = r"CREATE TABLE (?:IF NOT EXISTS )?.*?(?:;|$)"  # Find Tables
    table_scripts = re.findall(
        create_table_regex, ddl_scripts, re.DOTALL | re.IGNORECASE
    )

    # Iterate through tables
    all_tables = []
    for table in table_scripts:
        column_descriptions = extract_column_descriptions(table)
        data_dic = extract_data_dictionary(
            table, is_hive=is_hive, description_dict=column_descriptions
        )
        all_tables.extend(data_dic)

    return all_tables
