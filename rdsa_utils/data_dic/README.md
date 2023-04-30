# Data Dictionary Generator Module

This Python module allows you to generate data dictionaries from DDL (Data Definition Language) scripts. It provides functions to extract table information from DDL scripts and create data dictionaries in Markdown and Excel formats. Additionally, it includes a function to convert a Markdown file to a themed HTML file using a Jinja2 template.

## Features

- Extract table information from DDL scripts (supports Hive & Non-Hive DDL Scripts).
- Create data dictionaries in Markdown and Excel formats.
- Convert Markdown files to themed HTML using a Jinja2 template.

# Usage

Here's an example of how to use the Data Dictionary module to generate a data dictionary from DDL scripts:

```python
from ddl_scripts import *

from rdsa_utils.data_dic.extract import read_ddl_scripts, replace_variables, extract_data_dictionaries_from_multiple_tables
from rdsa_utils.data_dic.write import create_data_dictionary_excel, create_data_dictionary_markdown, markdown_file_to_html_with_theme

def main():
    """
    Main function to read the DDL scripts, extract table information, and create a data dictionary in an Excel file.
    """
    ddl_file_path = "./ddl_scripts.py"
    output_file = "data_dictionary.xlsx"

    ddl_scripts = read_ddl_scripts(ddl_file_path)
    ddl_scripts_replaced = replace_variables(ddl_scripts, globals())

    table info = extract_data_dictionaries_from_multiple_tables(
        ddl_scripts=ddl_scripts_replaced, is_hive=True
    )

    create_data_dictionary_excel(table_info, output_file)
    create_data_dictionary_markdown(table_info, "data_dictionary.md")
    markdown_file_to_html_with_theme("./data_dictionary.md", "./data_dictionary.html")

if __name__ == "__main__":
    main()
```

1. Import the necessary functions from the `rdsa_utils.data_dic.extract` and 
`rdsa_utils.data_dic.write` modules; and import the DDL scripts file as a Python script with a **wildcard** `"*"`.
2. Define a `main` function to:
    - Read DDL scripts from a file using `read_ddl_scripts`.
    - Replace variables in the DDL scripts with their actual values usng `replace_variables`.
    - Extract table information from the DDL scripts using `extract_table_information_hive`
    - Create a data dictionary in Excel format using `create_data_dictionary_excel`.
    - Create a data dictionary in Markdown format using `create_data_dictionary_markdown`.
    - Convert the Markdown file to a themed HTML file using `markdown_file_to_html_with_theme`.
3. Execute the `main` function.

This example will create a data dictionary in Excel, Markdown, and HTML formats from the DDL scripts provided in the `ddl_file_path`.

## Notes
- Make sure to import the DDL scripts file as a Python script with a wildcard "*".
- The ddl_file_path should point to the DDL scripts file (e.g., ./ddl_scripts.py).