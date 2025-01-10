"""Provides a class for validating TOML-based data schemas.

The `TOMLSchemaValidator` class helps ensure data quality and consistency
by verifying that TOML schemas adhere to a predefined structure and contain
necessary information for data validation.  It supports various checks,
including required fields, data types, length constraints, and custom
validation rules.

Author: James Westwood
"""

import datetime
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

if sys.version_info >= (3, 11):
    import tomllib as tomli
else:
    import tomli


class MissingDataTypesError(Exception):  # Define a custom exception
    """Raised when the 'datatypes' section is missing from the TOML config."""

    pass


class TOMLSchemaValidator:
    """Validates TOML-based data schemas against a predefined
    configuration.

    This class ensures data quality and consistency by verifying
    that TOML schemas adhere to a specific structure and contain
    the necessary information for data validation. It performs
    various checks, including:

    - **Required Fields:** Checks for the presence of mandatory
      fields in each column's configuration.
    - **Data Types:** Validates data types against a list of
      allowed types.
    - **Constraints:** Verifies length restrictions,
      minimum/maximum values, and permissible values.
    - **Custom Checks:** Supports user-defined validation rules
      for specialized requirements.
    - **Regular Expressions:** Validates string values against
      specified regular expression patterns.
    - **Uniqueness:** Checks for the uniqueness of values within
      a column.
    - **Date/Number Formats:** Validates date and number string
      formats for correctness.

    The validator uses a separate TOML configuration file to
    define the validation rules and supported data types,
    allowing for flexible customization.  It provides detailed
    error reporting, logging any discrepancies found during the
    validation process, and offers a go/no-go mechanism to halt
    processing if errors exceed a defined threshold.

    Attributes:
    ----------
        config (dict): The loaded validation configuration from
            the config TOML file.
        all_data_types (list): List of all supported data types
            defined in config.
        validation_functions (dict): A dictionary mapping
            validation field names to their corresponding
            validation functions.
        selected_functions (dict): Functions selected for each
            column being validated.
        string_types (list): Recognized string data types.
        numeric_types (list): Recognized numeric data types.
        datetime_types (list): Recognized datetime data types.

    """

    def __init__(self, config_file_path="config_validator_config.toml"):
        self.toml_val_logger = logging.getLogger(__name__)
        self._load_config(config_file_path)  # should create self.config
        self.selected_functions = {}

        if self.config:
            self.all_data_types = self._get_data_type_names()
        else:
            self.all_data_types = []

        self.validation_functions = {
            "description": self._validate_description,
            "nullable": self._validate_nullable,
            "data_type": self._validate_data_type,
            "length": self._validate_length,
            "min_value": self._validate_min_max,
            "max_value": self._validate_min_max,
            "possible_values": self._validate_possible_values,
            "regex_pattern": self._validate_regex_pattern,
            "unique": self._validate_unique,
            "date_format": self._validate_date_format,
            "number_str_format": self._validate_number_str_format,
            "custom_check": self._validate_custom_check,
        }

        self.string_types = ["str", "object", "StringType"]

        self.numeric_types = [
            "int",
            "float",  # Python
            "int64",
            "int32",
            "int16",
            "int8",
            "float64",
            "float32",  # Pandas/NumPy
            "IntegerType",
            "FloatType",
            "DoubleType",  # PySpark
        ]

        self.datetime_types = [
            "datetime.datetime",
            "datetime64[ns]",
            "TimestampType",
            "DateType",
        ]

    def _load_config(self, config_file_name="config_validator_config.toml"):
        """Loads the TOML config file, handling errors gracefully.

        Args:
            config_file_name (str): Name of config file (allows for easier
            testing)
        """
        try:
            config_path = os.path.join(os.path.dirname(__file__), config_file_name)
            with open(config_path, "r", encoding="utf-8") as f:
                toml_string = f.read()  # Read entire file into a string
            self.config = tomli.loads(toml_string)  # Use loads() for strings
        except FileNotFoundError:
            self.toml_val_logger.error(f"Config file '{config_file_name}' not found.")
        except tomli.TOMLDecodeError as e:
            self.toml_val_logger.error(
                f"Error decoding TOML file '{config_file_name}': {e}"
            )

    def _load_validation_schema(self, toml_path: str) -> Dict[str, Any]:
        """Loads a data validation schema from a TOML file.

        Args:
            toml_path: Path to the TOML file containing the schema.

        Returns
        -------
            A dictionary representing the validation schema, or None if an
            error occurs.

        """
        try:
            with open(toml_path, "rb") as f:
                schema = tomli.load(f)

            if not isinstance(schema, dict):
                self.toml_val_logger.error("Invalid schema: TOML must be a dictionary.")
                return None

            return schema

        except FileNotFoundError:
            self.toml_val_logger.error(f"TOML file not found at: {toml_path}")
            return None
        except tomli.TOMLDecodeError as e:
            self.toml_val_logger.error(f"Invalid TOML in {toml_path}: {e}")
            return None

    def _check_required_fields(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Checks for the presence of required fields in each column's configuration."""
        required_fields = self.config.get("required_fields", {}).get(
            "fields",
        )  # Access from self.config

        if required_fields:  # Check if required_fields is defined and not empty.
            for field in required_fields:
                if field not in col_config or col_config[field] is None:
                    col_errors.append(
                        f"Column '{col_name}' is missing required field '{field}'."
                    )
                    if field == "data_type" and "possible_values" in col_config:
                        col_errors.append(
                            f"Column '{col_name}' cannot have possible values without a data_type"
                        )
        return col_errors

    def _select_validation_functions(self, schema: Dict[str, Any]) -> None:
        """Selects validation functions and stores them in self._select_validation_functions."""
        for col_name, col_config in schema.items():
            if col_name == "data_asset":
                continue

            self.selected_functions[col_name] = []
            for field_name in self.validation_functions:
                # Correctly checks for missing values and handles "nan" string.
                if (
                    field_name in col_config
                    and col_config[field_name] is not None
                    and col_config[field_name] != "nan"
                ):
                    self.selected_functions[col_name].append(field_name)

    def _validate_description(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'description' field; checks for presence, valid string."""
        if "description" not in col_config:
            col_errors.append(f"Column '{col_name}' is missing a description.")
        elif (
            not isinstance(col_config["description"], str)
            or not col_config["description"].strip()
        ):
            col_errors.append(f"Column '{col_name}' has an invalid description.")
        elif len(col_config["description"].split()) == 0:  # Check for at least one word
            col_errors.append(
                f"Column '{col_name}' description must contain at least one word."
            )
        return col_errors

    def _validate_nullable(self, col_config, col_errors, col_name):
        """Validates the 'nullable' field against the schema."""
        if not isinstance(col_config["nullable"], bool):
            col_errors.append(f"Column '{col_name}': 'nullable' must be a boolean.")
            return col_errors  # Return early if nullable is invalid

        if not col_config["nullable"] and "possible_values" in col_config:
            invalid_values = [
                None,
                "nan",
                "None",
                "NULL",
                float("nan"),
            ]  # Removed pd.NA from this list
            for val in col_config["possible_values"]:  # Iterating to handle pd.NA
                if pd.isna(val) or val in invalid_values:
                    col_errors.append(
                        f"Column '{col_name}' is non-nullable but 'possible_values' contains null-like values."
                    )

                    return col_errors  # Return early once an error is found

        return col_errors

    def _get_data_type_names(self) -> List[str]:
        """Gets all data type names from the loaded config."""
        if "datatypes" not in self.config:
            raise MissingDataTypesError(
                "The 'datatypes' section is missing from the configuration file.",
            )

        all_type_names = []
        for cat in self.config["datatypes"]:
            all_type_names.extend(self.config["datatypes"][cat]["types"])
        return all_type_names

    def _validate_data_type(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> Dict[str, Any]:
        """Validates the 'data_type' field in the schema.

        Checks for valid data types and appropriate use of min/max value and length constraints.
        """
        data_type = col_config.get("data_type")
        if not data_type or data_type == "":
            col_errors.append(f"Column '{col_name}' is missing a data_type.")

        if data_type not in self.all_data_types:
            col_errors.append(
                f"{data_type} in column '{col_name}' is not a valid data type"
            )

        elif data_type == "category":  # possible_values must be present
            if (
                "possible_values" not in col_config
                or col_config["possible_values"] == "nan"
            ):
                col_errors.append(
                    f"Column '{col_name}' must have 'possible_values' if data_type is 'category'."
                )
        else:
            col_errors.append(
                f"Invalid data_type '{data_type}' specified for column '{col_name}'."
            )

        return col_errors

    def _validate_length(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'length' field.

        Checks that the length field exists.
        Checks that the length is an integer, a string representing an integer, or a string starting with ">" or ">=".
        Checks that the data_type field is string, object, or StringType if length is specified.
        """
        length_val = col_config["length"]
        if isinstance(length_val, str):
            if length_val.startswith(">=") or length_val.startswith(">"):
                try:
                    (
                        int(length_val[2:])
                        if length_val.startswith(">=")
                        else int(length_val[1:])
                    )
                except ValueError:
                    col_errors.append(f"{col_name}: Invalid length value: {length_val}")
            else:  # Check if the entire string is an integer
                try:
                    int(length_val)
                    col_config["length"] = int(
                        length_val,
                    )

                except ValueError:
                    col_errors.append(f"{col_name}: Invalid length value: {length_val}")

        elif not isinstance(length_val, int):
            col_errors.append(
                f"{col_name}: Invalid length value (not int or str): {length_val}",
            )

        # Check for data type compatability with the `length` field
        data_type = col_config.get("data_type")
        valid_data_types = ["str", "object", "StringType"]

        if (
            data_type not in valid_data_types
            and "length" in col_config
            and col_config["length"]
        ):
            col_errors.append(
                f"Column '{col_name}' is not a string type, it is a {data_type}. 'length' is not applicable."
            )
        return col_errors

    def _validate_min_max(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates 'min_value' and 'max_value' fields.

        Checks that both min_value and max_value are numbers if specified for numeric or datetime types.
        Checks that min_value is not greater than max_value.
        """
        data_type = col_config.get("data_type")

        if "min_value" in col_config:
            min_val = col_config["min_value"]
            if data_type in self.numeric_types and not isinstance(
                min_val, (int, float)
            ):
                col_errors.append(
                    f"Column '{col_name}' min_value must be a number for data_type '{data_type}'."
                )

        if "max_value" in col_config:
            max_val = col_config["max_value"]
            if data_type in self.numeric_types and not isinstance(
                max_val, (int, float)
            ):
                col_errors.append(
                    f"Column '{col_name}' max_value must be a number for data_type '{data_type}'."
                )

        if "min_value" in col_config and "max_value" in col_config:
            if (
                data_type in self.numeric_types
                and isinstance(col_config["min_value"], (int, float))
                and isinstance(col_config["max_value"], (int, float))
                and col_config["min_value"] > col_config["max_value"]
            ):
                col_errors.append(
                    f"Column '{col_name}' min_value cannot be greater than max_value for data_type: {data_type}"
                )
        elif data_type in self.datetime_types:  # Handle datetime comparisons
            # if a min or max time is specified, this validates that it can be parsed
            try:
                min_val = pd.to_datetime(col_config["min_value"])
                max_val = pd.to_datetime(col_config["max_value"])

                if min_val > max_val:
                    col_errors.append(
                        f"Column '{col_name}' min_value cannot be greater than max_value for data_type: {data_type}"
                    )
            except (ValueError, TypeError) as e:  # Catch time parsing errors
                col_errors.append(
                    f"Error comparing datetime values for column '{col_name}': {e}"
                )

        return col_errors

    def _validate_possible_values(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'possible_values' field.

        Checks that 'possible_values' is a list.
        Checks that all values in 'possible_values' are of the type specified in 'data_type'.
        Generates a warning if 'data_type' is not "category" and 'possible_values' is used.
        """
        if "possible_values" in col_config:
            possible_values = col_config["possible_values"]

            if not isinstance(possible_values, list):
                col_errors.append(
                    f"Column '{col_name}': 'possible_values' must be a list.",
                )

            data_type = col_config.get("data_type")
            if data_type != "category":
                self.toml_val_logger.warning(  # Use warnings.warn for non-categorical types
                    f"Column '{col_name}': Using 'possible_values' with data_type '{data_type}' "
                    f"might not be memory-efficient. Consider using 'category' data_type.",
                )

            if data_type:  # Only perform type checking if data_type is specified
                type_check_map = {
                    "int": int,
                    "float": float,
                    "str": str,
                    "bool": bool,
                    "int64": int,
                    "int32": int,
                    "int16": int,
                    "int8": int,
                    "float64": float,
                    "float32": float,
                    "object": object,
                    "bool_": bool,
                    "IntegerType": int,
                    "FloatType": float,
                    "DoubleType": float,
                    "BooleanType": bool,
                    "StringType": str,
                    "category": object,  # Category can hold various types
                }

                expected_type = type_check_map.get(data_type)

                if expected_type:  # Only proceed if a valid data type mapping is found
                    for value in possible_values:
                        if (
                            not isinstance(value, expected_type)
                            and value != "nan"
                            and value is not None
                        ):  # Exclude "nan" and None
                            col_errors.append(
                                f"Column '{col_name}': Value '{value}' in 'possible_values' is not of type '{data_type}'.",
                            )

    def _validate_regex_pattern(
        self,
        col_config: Dict[str, Any],
        errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'regex_pattern' field.

        Checks that the regex pattern is valid.
        Checks that the data_type is str, object, or StringType.
        """
        if "regex_pattern" in col_config:
            pattern = col_config["regex_pattern"]
            try:
                re.compile(pattern)  # Check if the pattern is valid regex
            except re.error:
                errors.append(
                    f"Column '{col_name}': Invalid regex pattern '{pattern}'."
                )

            data_type = col_config.get("data_type")

            if data_type not in self.string_types:
                errors.append(
                    f"Column '{col_name}': 'regex_pattern' can only be applied to string type columns."
                )
        return errors

    def _validate_unique(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'unique' field. Checks that it's a boolean."""
        if "unique" in col_config:
            if not isinstance(col_config["unique"], bool):
                col_errors.append(f"Column '{col_name}': 'unique' must be a boolean.")

        # Possibly need to check for conflicting data types
        # Like could we expect that `unique = True` and `data_type = float` is illogical?
        # TODO: Placeholder to create that code if needed

        return col_errors

    def _validate_date_format(
        self, col_config: Dict[str, Any], col_errors: List[str], col_name: str
    ) -> List[str]:  #
        """Validates the 'date_format' field.

        Checks that the provided date format can be parsed by strptime.
        Checks that data_type is a valid datetime type.

        """
        date_format = col_config["date_format"]
        data_type = col_config.get("data_type")
        datetime_types = [
            "datetime.datetime",
            "datetime64[ns]",
            "TimestampType",
            "DateType",
        ]

        # Check that data_type matched the existence of date_format
        if data_type not in datetime_types:
            col_errors.append(
                f"Column '{col_name}': 'date_format' can only be used with datetime types, not '{data_type}'."
            )
            return col_errors  # Stop further checks if the type is incorrect.

        # Check for date format useage errors
        try:
            datetime.datetime.strptime("2024-05-03", date_format)  # Use a test string.
        except ValueError:
            col_errors.append(
                f"Column '{col_name}': Invalid date format '{date_format}'."
            )
        return col_errors

    def _validate_number_str_format(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'number_str_format' field.

        Checks that the provided number format can be parsed by strptime.
        Checks that data_type is a valid numeric type.
        """
        if "number_str_format" in col_config:
            number_str_format = col_config["number_str_format"]
            data_type = col_config.get("data_type")

            if data_type not in self.numeric_types:
                col_errors.append(
                    f"""Column '{col_name}': 'number_str_format' can only be
                    used with numeric types, not '{data_type}'."""
                )
                return col_errors  # Stop further checks if type is incorrect

            try:
                if data_type in [float, "float64", "float32", "DoubleType"]:
                    test_number = 1234.56
                elif (
                    data_type == str
                    or data_type == "StringType"
                    and "." in number_str_format
                ):
                    test_number = "1234.56"
                elif data_type in [
                    "int",
                    "int64",
                    "int32",
                    "int16",
                    "int8",
                    "IntegerType",
                ]:
                    test_number = 1234
                else:
                    test_number = "1234"

                # Use the actual number_str_format from the config:
                format_string = col_config["number_str_format"]
                format_string.format(str(test_number))
            except (
                ValueError,
                TypeError,
                KeyError,
            ) as e:  # Catch all possible format errors
                col_errors.append(
                    f"Column '{col_name}': Invalid number format '{number_str_format}' - {e}"
                )

        return col_errors

    def _validate_custom_check(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validates the 'custom_check' field.

        Checks if the value is valid Python code, a callable, or a function
        name defined in data_validation.py.
        """
        if "custom_check" in col_config:
            custom_check_val = col_config["custom_check"]

            if isinstance(custom_check_val, str):
                # 1. Check if it's a valid function name in data_validation.py:
                try:
                    from rdsa_utils.rdsa_data_validator import data_validation

                    check_function = getattr(data_validation, custom_check_val)
                    if not callable(check_function):
                        col_errors.append(
                            f"Column '{col_name}': '{custom_check_val}' is not a callable in data_validation.py."
                        )
                except AttributeError:
                    # 2. Attempt to parse as Python code:
                    try:
                        compile(custom_check_val, "<string>", "exec")
                    except (SyntaxError, TypeError, ValueError) as e:
                        col_errors.append(
                            f"Column '{col_name}': Invalid Python code or function name in 'custom_check': {e}"
                        )
            elif not callable(custom_check_val):  # Handle non-string values
                col_errors.append(
                    f"Column '{col_name}': 'custom_check' must be a string or callable."
                )

        return col_errors

    def _log_errors(self, errors_dict):
        """Uses the logger to log errors from the errors dict."""
        for col_name, errors in errors_dict.items():
            if errors:
                for error in errors:
                    self.toml_val_logger.error(f"Column '{col_name}': {error}")

    def _go_no_go(self, errors_dict, stop_on_errors=True, threshold=0):
        """Takes the errors dict and stops the pipeline if the number of errors exceeds a threshold.

        Args:
            errors_dict (dict): A dictionary where keys are column names (or
            other identifiers)
                                and values are lists of error messages for
                                that column.
            stop_on_errors (bool, optional): Whether to raise an exception if
            errors are found.
            Defaults to True.
            threshold (int, optional): The maximum number of errors allowed
            before stopping the pipeline.
            Defaults to 0.

        Raises
            ValueError: If the number of errors exceeds the threshold and
            stop_on_errors is True.
        """

        total_errors = sum(
            len(errors) for errors in errors_dict.values() if errors
        )  # only if errors != None

        if total_errors > threshold and stop_on_errors:
            error_messages = []
            for col, errors in errors_dict.items():
                if not errors:
                    continue
                for error in errors:
                    error_messages.append(f"Column '{col}': {error}")
            raise ValueError(
                f"Validation failed with {total_errors} errors:\n"
                + "\n".join(error_messages)
            )
        elif total_errors > 0:
            for col, errors in errors_dict.items():
                for error in errors:
                    self.toml_val_logger.warning(f"Column '{col}': {error}")
        else:
            self.toml_val_logger.info("Validation successful. No errors found.")

    def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validates the entire schema."""
        errors = {}
        # Populate self.selected_functions
        self._select_validation_functions(schema)

        for col_name, val_func_names in self.selected_functions.items():
            errors[col_name] = []
            for func_name in val_func_names:
                val_func = self.validation_functions.get(func_name)
                if val_func:
                    errors[col_name] = val_func(
                        schema.get(col_name, {}), errors[col_name], col_name
                    )
                else:
                    self.toml_val_logger.warning(
                        f"Validation function '{func_name}' not found. Skipping."
                    )

        return errors

    def run_validation(self, toml_path: str) -> None:
        """Loads the schema, runs validation, and handles results."""
        schema = self._load_validation_schema(toml_path)

        if not schema:  # Handle empty schema gracefully
            self.toml_val_logger.error(
                "Schema is empty. Cannot proceed with validation."
            )
            return

        errors = self.validate_schema(schema)  # Call validate_schema method

        self._log_errors(errors)  # Log the errors
        self._go_no_go(errors)  # Make the go/no-go decision


if __name__ == "__main__":
    validator = TOMLSchemaValidator()  # Create an instance of the validator
    toml_file_path = (
        Path("rdsa_utils") / "rdsa_data_validator" / "example_dataframe_schema.toml"
    )

    validator.run_validation(str(toml_file_path))
