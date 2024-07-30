import logging
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
import pandas as pd
from path import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


@dataclass
class Config:
    """
    Configuration for processing CSV files as inputs and generating unit test code for with
    pandas dataframes.

    Attributes
    ----------
    csv_path : str
        Directory path where CSV files are located.
    files : List[str]
        List of CSV filenames to process.
    function_name : str
        Name of the function to be tested. This will populate the test code with given function_name
    column_type_override : Dict[str, List[str]]
        Dictionary mapping column types to lists of columns to override the inferred types.
        Currently supported type overrides are string & float.

    Examples
    --------
    ```python
    >>> config = Config(
    >>>     csv_path="D:/projects_data/randd_test_data/",
    >>>     files=["input.csv", "expected_output.csv"],
    >>>     function_name="dummy_function",
    >>>     column_type_override={'string': [], 'float': []}
    >>> )
    ```
    ```python
    >>> config = Config(
    >>>     csv_path="D:/projects_data/randd_test_data/",
    >>>     files=["input1.csv", "input2.csv", "mapper.csv", "expected_output.csv"],
    >>>     function_name="dummy_function",
    >>>     column_type_override={'string': ['names', 'references'], 'float': ['weights']}
    >>> )
    ```
    """

    csv_path: str
    files: List[str]
    function_name: str
    column_type_override: Dict[str, List[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Run validations after initialization to ensure configuration correctness."""
        self.validate()

    def validate(self) -> None:
        """
        Validate the configuration values to ensure they meet the expected criteria.

        Raises
        ------
        TypeError
            If any attribute is not of the expected type.
        ValueError
            If `csv_path` is not a valid directory or `files` contains non-CSV files.
        """
        if not isinstance(self.csv_path, str):
            error_msg = "csv_path must be a string"
            raise TypeError(error_msg)
        if not Path(self.csv_path).is_dir():
            error_msg = f"Invalid CSV path: {self.csv_path}"
            raise ValueError(error_msg)

        if not isinstance(self.files, list):
            error_msg = "files must be a list of strings"
            raise TypeError(error_msg)
        if any(
            not isinstance(file, str) or not file.endswith(".csv")
            for file in self.files
        ):
            error_msg = "All files must be CSV files and must be strings"
            raise ValueError(error_msg)

        if not isinstance(self.function_name, str):
            error_msg = "function_name must be a string"
            raise TypeError(error_msg)
        if not self.function_name.isidentifier():
            error_msg = "function_name must be a valid identifier, i.e 'create_schema' "
            raise ValueError(error_msg)

        if not isinstance(self.column_type_override, dict):
            error_msg = "column_type_override must be a dictionary"
            raise TypeError(error_msg)
        for key, value in self.column_type_override.items():
            if not isinstance(key, str):
                error_msg = "Keys in column_type_override must be strings"
                raise TypeError(error_msg)
            if not isinstance(value, list):
                error_msg = "Values in column_type_override must be lists"
                raise TypeError(error_msg)
            if any(not isinstance(col, str) for col in value):
                error_msg = "All column names in column_type_override must be strings"
                raise TypeError(error_msg)

    @property
    def class_name(self) -> str:
        """
        Convert the function name to CamelCase for use as a class name.

        Returns
        -------
        str
            The CamelCase class name derived from the function_name.
        """
        return "".join(word.capitalize() for word in self.function_name.split("_"))


def infer_column_types(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Infer the types of columns based on their values.

    This function analyzes each column in the DataFrame and determines its predominant type
    based on the values present. It categorizes columns as 'string', 'float', 'integer',
    'boolean', 'date', or 'mixed' (if no predominant type is found).

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame to analyze.

    Returns
    -------
    Dict[str, List[str]]
        A dictionary mapping column types to lists of column names. Columns with mixed types
        are classified under 'mixed'.
    """
    type_dict: Dict[str, List[str]] = {
        "string": [],
        "float": [],
        "integer": [],
        "boolean": [],
        "date": [],
        "mixed": [],
    }

    for col in df.columns:
        non_nan_values = df[col].dropna()
        if non_nan_values.empty:
            type_dict["string"].append(col)
            continue

        type_counts = {"string": 0, "float": 0, "integer": 0, "boolean": 0, "date": 0}

        for value in non_nan_values:
            if isinstance(value, str):
                type_counts["string"] += 1
                try:
                    pd.to_numeric(value)
                    if "." in value:
                        type_counts["float"] += 1
                    else:
                        type_counts["integer"] += 1
                except ValueError:
                    pass
                try:
                    pd.to_datetime(value, errors="raise")
                    type_counts["date"] += 1
                except ValueError:
                    pass
                if value.lower() in ["true", "false"]:
                    type_counts["boolean"] += 1
            elif isinstance(value, bool):
                type_counts["boolean"] += 1
            elif isinstance(value, (int, np.integer)):
                type_counts["integer"] += 1
            elif isinstance(value, (float, np.floating)):
                type_counts["float"] += 1
            elif isinstance(value, pd.Timestamp):
                type_counts["date"] += 1

        predominant_type = max(type_counts, key=type_counts.get)

        if type_counts[predominant_type] > 0:
            type_dict[predominant_type].append(col)
        else:
            type_dict["mixed"].append(col)

    return type_dict


def dataframe_to_string(df: pd.DataFrame, file: str, config: Config) -> str:
    """
    Convert a DataFrame to a formatted string representation suitable for unit tests.

    This function infers column types and formats the DataFrame accordingly. Columns are converted
    to string representations with specific formatting based on their inferred types.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame to convert.
    file : str
        The name of the file the DataFrame was read from, used for logging.
    config : Config
        Configuration object with settings that may affect the conversion.

    Returns
    -------
    str
        A string representation of the DataFrame formatted for use in unit tests.
    """
    logging.info(f"Processing DataFrame from file: {file}")

    type_dict = infer_column_types(df)

    logging.debug(f"Inferred column types: {type_dict}")

    if config.column_type_override:
        non_existent_columns = []
        for col_type, cols in config.column_type_override.items():
            for col in cols:
                if col in type_dict.get(col_type, []):
                    type_dict[col_type].remove(col)
                if col in df.columns:
                    type_dict.setdefault(col_type, []).append(col)
                else:
                    non_existent_columns.append(col)
        if non_existent_columns:
            logging.warning(
                f"The following columns to override do not exist in the DataFrame '{file}': {', '.join(non_existent_columns)}",
            )

    df = df.astype(str)

    for col in type_dict["string"]:
        df.loc[df[col] != "nan", col] = df.loc[df[col] != "nan", col].apply(
            lambda x: f'"{x}"',
        )

    for col in type_dict["float"]:
        df.loc[df[col] != "nan", col] = df.loc[df[col] != "nan", col].apply(
            lambda x: f"{x}.0" if "." not in x else x,
        )

    df = df.replace("nan", "np.nan")

    tab = " " * 4
    col_string = "".join([f'{tab}{tab}"{col}",\n' for col in df.columns])

    df["output"] = (
        f"{tab}[" + df[df.columns[:]].apply(lambda row: ", ".join(row), axis=1) + "],"
    )
    rows_string = df["output"].str.cat(sep=f"\n{tab}")

    data_string = f"""columns = [\n{col_string}{tab}]
        data = [\n{tab}{rows_string}\n{tab}]\n"""

    logging.info(f"Data string generated for file: {file}")

    return data_string


def generate_test_code(config: Config, data_strings: Dict[str, str]) -> str:
    """
    Generate a unit test code string based on configuration and data strings.

    The function creates imports, class definitions, fixture functions, and test functions
    necessary for unit testing a given function. It uses the configuration to customize
    the class name and imports.

    Parameters
    ----------
    config : Config
        Configuration object with settings for test generation.
    data_strings : Dict[str, str]
        Dictionary mapping filenames to their corresponding data strings.

    Returns
    -------
    str
        The generated unit test code as a string.
    """
    imports = (
        f"import pandas as pd\n"
        f"import numpy as np\n"
        f"import pytest\n"
        f"import {config.function_name}  # Please insert correct pathway to function import\n"
    )

    class_def = (
        f"\n\nclass {config.class_name}:\n"
        f'    """Tests for {config.function_name}."""\n'
    )

    fixture_defs = ""
    fixture_names = []

    for file, data_string in data_strings.items():
        fixture_name = file.replace(".csv", "").replace("-", "_").replace(" ", "_")
        fixture_names.append(fixture_name)
        fixture_defs += (
            f'\n    @pytest.fixture(scope="function")\n'
            f"    def {fixture_name}(self):\n"
            f'        """Data from {file}."""\n'
            f"        {data_string}\n"
            f"        return pd.DataFrame(columns=columns, data=data)\n"
        )

    test_def = (
        f'\n    def test_{config.function_name}(self, {", ".join(fixture_names)}): # Please construct your function\n'
        f'        """General tests for {config.function_name}."""\n'
        f"        output = {config.function_name}({list(data_strings.keys())[0]})\n"
        f'        assert output.equals({fixture_names[-1]}), "{config.function_name} not behaving as expected"\n'
    )

    return f"{imports}{class_def}{fixture_defs}{test_def}"


def process_dataframe(config: Config) -> None:
    """
    Process CSV files, generate unit test code, and save it to a Python (.py) file.

    This function reads CSV files specified in the configuration, converts each DataFrame
    to a string representation suitable for unit tests, and generates test code based on the
    provided configuration. It handles file reading errors and logs relevant information.

    Parameters
    ----------
    config : Config
        Configuration object containing settings such as file paths, function name to test,
        and column type overrides.

    Raises
    ------
    IOError
        If there is an error writing the test code to the output file.
    """
    file_paths = [Path(config.csv_path) / file for file in config.files]

    missing_files = [path for path in file_paths if not Path(path).is_file()]

    if missing_files:
        logging.error(f"File(s) not found: {', '.join(missing_files)}")
        return

    data_strings: Dict[str, str] = {}

    for file in config.files:
        try:
            df = pd.read_csv(Path(config.csv_path) / file)
            data_strings[file] = dataframe_to_string(df, file, config)
            logging.info(f"Successfully read and processed file: {file}")
        except pd.errors.EmptyDataError:
            logging.error(f"File is empty: {file}")
        except pd.errors.ParserError:
            logging.error(f"File could not be parsed: {file}")
        except Exception as e:
            logging.error(f"Error reading or processing file {file}: {e}")
            return

    test_code = generate_test_code(config, data_strings)

    output_path = Path(config.csv_path) / f"test_{config.function_name}.py"

    try:
        with open(output_path, "w") as text_file:
            text_file.write(test_code)
        logging.info(f"Successfully wrote output file: {output_path}")
    except IOError as e:
        logging.error(f"Error writing output file: {e}")


def main() -> None:
    """
    Initialize configuration and process CSV files for unit testing.

    This function sets up the configuration with paths, filenames, and function names,
    and then calls `process_dataframe` to handle the CSV files and generate the test code.

    Returns
    -------
    None
    """
    # Populate the config with your parameters, examples are provided in class init
    config = Config(
        csv_path="D:/projects_data/randd_test_data/",
        files=["input1.csv", "expected_output.csv", "fail_output.csv"],
        function_name="new_function",
        column_type_override={"string": ["period", "reference"], "float": ["602"]},
    )

    process_dataframe(config)


if __name__ == "__main__":
    main()
