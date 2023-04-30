"""
Data Dictionary Generator Writer Module

This module contains functions to create data dictionaries from extracted table information. 

These data dictionaries can be created in either Markdown or Excel format. 

Additionally, this module also provides a function to convert a Markdown file to themed HTML using a Jinja2 template.

The main functions in this module are:
    create_data_dictionary_markdown: Generates a data dictionary in Markdown format from a list of TableInformation objects.
    markdown_file_to_html_with_theme: Converts a Markdown file to themed HTML using a Jinja2 template.
    create_data_dictionary_excel: Generates a data dictionary in Excel format from a list of TableInformation objects.
"""
from dataclasses import asdict
from pathlib import Path
from typing import List

import markdown
import xlsxwriter
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader

from rdsa_utils import PACKAGE_PATH
from rdsa_utils.data_dic.extract import TableInformation


def create_data_dictionary_markdown(
    table_info: List[TableInformation],
    output_file: str,
    is_hive: bool = False,
):
    """
    Create a data dictionary as a Markdown file using the extracted table information.

    This function generates a data dictionary in the Markdown format from the extracted table information.

    It creates a separate section for each table with a list of columns, their data types, constraints, and descriptions.

    Parameters
    ----------
    table_info : List[TableInformation]
        A list of TableInformation objects containing table information
        (database name, table name, column name, data type, constraints, description, storage type, partition columns).
    output_file : str
        The output Markdown file path.
    is_hive : bool, optional
        Set to True if the DDL script is in Hive syntax, by default False.

    Returns
    -------
    None
    """
    markdown_lines = []
    current_table = None

    markdown_lines.append("# Data Dictionary")

    for table_entry in table_info:
        full_table_name = (
            f"{table_entry.database_name}.{table_entry.table_name}"
            if table_entry.database_name
            else table_entry.table_name
        )

        if full_table_name != current_table:
            if current_table is not None:
                markdown_lines.append("\n")

            current_table = full_table_name
            markdown_lines.append(
                f"## Database: {table_entry.database_name} | Table: {table_entry.table_name}\n"
            )
            if is_hive:
                markdown_lines.append(f"**Storage Type:** {table_entry.storage_type}\n")
                markdown_lines.append(
                    f"**Partition Columns:** {table_entry.partition_columns if table_entry.partition_columns else 'None'}\n"
                )
            markdown_lines.append(
                "| Column Name | Data Type | Constraints | Description |"
            )
            markdown_lines.append(
                "|-------------|-----------|-------------|-------------|"
            )

        markdown_lines.append(
            f"| {table_entry.column_name} | {table_entry.data_type} | {table_entry.constraints} | {table_entry.description} |"
        )

    with open(output_file, "w") as f:
        f.write("\n".join(markdown_lines))

    output_file_path = Path(output_file).resolve()
    print(f"Data dictionary saved to: {output_file_path}")


def markdown_file_to_html_with_theme(
    markdown_file: str, output_file: str = "data_dictionary.html"
) -> None:
    """
    Convert a markdown file to themed HTML using a Jinja2 template.

    Parameters
    ----------
    markdown_file : str
        Path to the input markdown file.
    output_file : str, optional
        Path to the output HTML file, defaults to "data_dictionary.html".

    Returns
    -------
    None
        This function writes the themed HTML to an output file.

    Raises
    ------
    FileNotFoundError
        If the input markdown file or Jinja2 template file does not exist.

    Notes
    -----
    This function reads the contents of a markdown file, converts it to HTML using
    the Python Markdown library, extracts the H1 content using BeautifulSoup,
    loads a Jinja2 template, applies the template to the HTML content,
    and saves the themed HTML to an output file.

    The function also supports the conversion of Markdown tables to HTML tables
    using the `markdown.extensions.tables` extension.

    The input and output file paths can be either relative or absolute. If a relative
    path is given, it is interpreted as relative to the current working directory.

    Examples
    --------
    >>> markdown_file_to_html_with_theme("example.md", "template.html", "output.html")
    """
    # Convert the input file paths to Path objects
    markdown_path = Path(markdown_file)
    output_path = Path(output_file)

    # Hard-Coded Theme Path
    template_path = Path(PACKAGE_PATH / "data_dic" / "theme.html")

    # Raise an error if the input files do not exist
    if not markdown_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {markdown_path}")
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Read the content of the Markdown file
    with open(markdown_path, "r") as f:
        markdown_content = f.read()

    # Convert Markdown to HTML with tables extension
    html = markdown.markdown(markdown_content, extensions=["tables"])

    # Extract the H1 content using BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    h1_text = soup.h1.string if soup.h1 else ""

    # Load the Jinja2 template
    templates_path = template_path.parent
    env = Environment(loader=FileSystemLoader(str(templates_path)))
    template = env.get_template(template_path.name)

    # Apply the template to the HTML content
    themed_html = template.render(content=html, title=h1_text)

    # Save the themed HTML to an output file
    with open(output_path, "w") as f:
        f.write(themed_html)

    output_file_path = Path(output_file).resolve()
    print(f"Data dictionary saved to: {output_file_path}")


def create_data_dictionary_excel(
    table_information: List[TableInformation],
    output_file: str,
    is_hive: bool = False,
):
    """
    Create a data dictionary Excel file from the extracted table information.

    This function generates a data dictionary in the Excel format from the extracted table information.

    It creates a worksheet named "Data Dictionary" with a row for each table entry and columns for database name, table name,
    column name, data type, constraints, description, storage type, and partition columns.

    The column widths are autofitted based on the maximum length of the data in each column.

    Parameters
    ----------
    table_information : List[TableInformation]
        A list of TableInformation objects containing table information
        (database name, table name, column name, data type, constraints, description, storage type, partition columns).
    output_file : str
        The path to the output Excel file.
    is_hive : bool, optional
        Set to True if the DDL script is in Hive syntax, by default False.

    Returns
    -------
    None
    """
    workbook = xlsxwriter.Workbook(output_file)
    worksheet = workbook.add_worksheet("Data Dictionary")

    header_format = workbook.add_format(
        {
            "bold": True,
            "bg_color": "gray",
            "border": 1,
            "align": "center",
            "text_wrap": True,
        }
    )

    headers = list(asdict(table_information[0]).keys())

    if not is_hive:
        headers = [
            col_name
            for col_name in headers
            if col_name not in ("storage_type", "partition_columns")
        ]

    # Write the headers
    for i, header in enumerate(headers):
        worksheet.write(0, i, header, header_format)

    # Initialize max_lengths with header lengths
    max_lengths = [len(header) + 2 for header in headers]

    # Write the table information and update max_lengths
    for row, table_info in enumerate(table_information, start=1):
        table_info_dict = asdict(table_info)
        if not is_hive:
            table_info_dict = {
                key: table_info_dict[key]
                for key in table_info_dict.keys()
                if key not in ["storage_type", "partition_columns"]
            }
        for col, (key, value) in enumerate(table_info_dict.items()):
            worksheet.write(row, col, value)
            max_lengths[col] = max(max_lengths[col], len(str(value)) + 2)

    # Autofit the column widths based on max_lengths
    for i, max_length in enumerate(max_lengths):
        worksheet.set_column(i, i, max_length)

    workbook.close()

    output_file_path = Path(output_file).resolve()
    print(f"Data dictionary saved to: {output_file_path}")
