"""Provides a class for validating TOML-based data schemas.

The `TOMLSchemaValidator` class helps ensure data quality and consistency
by verifying that TOML schemas adhere to a predefined structure and contain
necessary information for data validation.  It supports various checks,
including required fields, data types, length constraints, and custom
validation rules.

Author: James Westwood
"""

import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

if sys.version_info >= (3, 11):
    import tomllib as tomli
else:
    import tomli


class MissingDataTypesError(Exception):
    """Raised when the 'datatypes' section is missing from the TOML config."""

    pass


class TOMLSchemaValidator:
    """Validates TOML-based data schemas against a predefined configuration.

    This class ensures data quality and consistency by verifying
    that TOML schemas adhere to a specific structure and contain
    the necessary information for data validation. It performs
    various checks including:

    - **Required Fields:** Checks for the presence of mandatory
      fields in each column's configuration.
    - **Data Types:** Validates data types against a list of
      allowed types.
    - **Constraints:** Verifies length restrictions,
      minimum/maximum values, and permissible values.
    - **Custom Checks:** Supports user-defined validation rules
      for specialised requirements.
    - **Regular Expressions:** Validates string values against
      specified regular expression patterns.
    - **Uniqueness:** Checks for the uniqueness of values within
      a column.
    - **Date/Number Formats:** Validates date and number string
      formats for correctness.

    The validator uses a separate TOML configuration file to
    define the validation rules and supported data types,
    allowing for flexible customisation. It provides detailed
    error reporting, logging any discrepancies found during the
    validation process, and offers a go/no-go mechanism to halt
    processing if errors exceed a defined threshold.

    Attributes
    ----------
        config : dict
            The loaded validation configuration from the config TOML file.
        all_data_types : list
            List of all supported data types defined in config.
        validation_functions : dict
            A dictionary mapping validation field names to their corresponding
            validation functions.
        selected_functions : dict
            Functions selected for each column being validated.
        string_types : list
            Recognised string data types.
        numeric_types : list
            Recognised numeric data types.
        datetime_types : list
            Recognised datetime data types.
    """

    def __init__(self, config_file_path="config_validator_config.toml"):
        self.toml_val_logger = logging.getLogger(__name__)
        self._load_config(config_file_path)
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
            "float",
            "int64",
            "int32",
            "int16",
            "int8",
            "float64",
            "float32",
            "IntegerType",
            "FloatType",
            "DoubleType",
        ]
        self.datetime_types = [
            "datetime.datetime",
            "datetime64[ns]",
            "TimestampType",
            "DateType",
        ]

    def _load_config(
        self,
        config_file_name: str = "config_validator_config.toml",
    ):
        """Load the TOML config file, handling errors gracefully.

        Parameters
        ----------
        config_file_name : str
            Name of config file (allows for easier testing)
        """
        try:
            config_path = Path(__file__).parent / config_file_name
            with open(config_path, "r", encoding="utf-8") as f:
                toml_string = f.read()
            self.config = tomli.loads(toml_string)
        except FileNotFoundError:
            self.toml_val_logger.error(
                f"Config file '{config_file_name}' not found.",
            )
            self.config = {}
        except tomli.TOMLDecodeError as e:
            self.toml_val_logger.error(
                f"Error decoding TOML file '{config_file_name}': {e}",
            )
            self.config = {}

    def _load_validation_schema(self, toml_path: str) -> Dict[str, Any]:
        """Load a data validation schema from a TOML file.

        Parameters
        ----------
        toml_path : str
            Path to the TOML file containing the schema.

        Returns
        -------
        Dict[str, Any] or None
            A dictionary representing the validation schema, or None if an
            error occurs.
        """
        try:
            with open(toml_path, "rb") as f:
                schema = tomli.load(f)
            if not isinstance(schema, dict):
                self.toml_val_logger.error(
                    "Invalid schema: TOML must be a dictionary.",
                )
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
        """Check for presence of required fields in each column's configuration.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        required_fields = self.config.get("required_fields", {}).get(
            "fields",
            [],
        )
        for field in required_fields:
            if field not in col_config or col_config[field] is None:
                col_errors.append(
                    f"Column '{col_name}' is missing required field '{field}'.",
                )
                if field == "data_type" and "possible_values" in col_config:
                    col_errors.append(
                        f"'{col_name}' cannot have possible values \
                            without a data_type",
                    )
        return col_errors

    def _select_validation_functions(self, schema: Dict[str, Any]) -> None:
        """Select validation functions and store for each column in the schema.

        This method populates the selected_functions dictionary with
        the appropriate validation functions for each column in the schema.

        Parameters
        ----------
        schema : Dict[str, Any]
            The schema to be validated
        """
        for col_name, col_config in schema.items():
            if col_name == "data_asset":
                continue
            self.selected_functions[col_name] = []
            for field_name in self.validation_functions:
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
        """Validate the 'description' field.

        Checks presence, valid string content, & minimum content requirements.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        if "description" not in col_config:
            col_errors.append(f"Column '{col_name}' is missing a description.")
        elif (
            not isinstance(col_config["description"], str)
            or not col_config["description"].strip()
        ):
            col_errors.append(
                f"Column '{col_name}' has an invalid description.",
            )
        elif len(col_config["description"].split()) == 0:
            col_errors.append(
                f"Column '{col_name}' description must contain \
                     at least one word.",
            )
        return col_errors

    def _validate_nullable(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'nullable' field.

        Checks field is a boolean and ensures consistency with possible values.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        if not isinstance(col_config["nullable"], bool):
            col_errors.append(
                f"Column '{col_name}': 'nullable' must be a boolean.",
            )
            return col_errors
        if not col_config["nullable"] and "possible_values" in col_config:
            invalid_values = [None, "nan", "None", "NULL", float("nan")]
            for val in col_config["possible_values"]:
                if pd.isna(val) or val in invalid_values:
                    col_errors.append(
                        f"'{col_name}' is non-nullable but \
                             may contain null values.",
                    )
                    return col_errors
        return col_errors

    def _get_data_type_names(self) -> List[str]:
        """Get all data type names from the loaded config.

        Returns
        -------
        List[str]
            List of all supported data types

        Raises
        ------
        MissingDataTypesError
            If the datatypes section is missing from the configuration
        """
        if "datatypes" not in self.config:
            e = "The 'datatypes' section is missing in the configuration file."
            raise MissingDataTypesError(e)
        all_type_names = []
        for cat in self.config["datatypes"]:
            all_type_names.extend(self.config["datatypes"][cat]["types"])
        return all_type_names

    def _validate_data_type(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'data_type' field.

        Checks data type is valid and consistent with other configuration.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        data_type = col_config.get("data_type", "")
        if not data_type:
            col_errors.append(f"Column '{col_name}' is missing a data_type.")
        elif data_type not in self.all_data_types:
            col_errors.append(
                f"{data_type} in column '{col_name}' is not a valid data type",
            )
        else:
            if data_type == "category":
                if (
                    "possible_values" not in col_config
                    or col_config["possible_values"] == "nan"
                ):
                    col_errors.append(
                        f"'{col_name}' must have 'possible_values' if \
                            data_type is 'category'.",
                    )
        return col_errors

    def _validate_length(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'length' field.

        Checks length constraints are valid for data type & properly formatted.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        length_val = col_config["length"]
        if isinstance(length_val, str):
            if length_val.startswith(">=") or length_val.startswith(">"):
                try:
                    _ = (
                        int(length_val[2:])
                        if length_val.startswith(">=")
                        else int(length_val[1:])
                    )
                except ValueError:
                    col_errors.append(
                        f"{col_name}: Invalid length value: {length_val}",
                    )
            else:
                try:
                    col_config["length"] = int(length_val)
                except ValueError:
                    col_errors.append(
                        f"{col_name}: Invalid length value: {length_val}",
                    )
        elif not isinstance(length_val, int):
            col_errors.append(
                f"{col_name}: Invalid length value \
                     (not int or str): {length_val}",
            )

        data_type = col_config.get("data_type")
        valid_data_types = ["str", "object", "StringType"]
        if (
            data_type not in valid_data_types
            and "length" in col_config
            and col_config["length"]
        ):
            col_errors.append(
                f"Column '{col_name}' is not a string type, it is {data_type}. \
                    'length' is not applicable.",
            )
        return col_errors

    def _validate_min_max(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate 'min_value' and 'max_value' fields.

        Ensures that min/max values are appropriate for the data type
        and that min_value is not greater than max_value.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        data_type = col_config.get("data_type")

        if "min_value" in col_config:
            min_val = col_config["min_value"]
            if data_type in self.numeric_types and not isinstance(
                min_val,
                (int, float),
            ):
                col_errors.append(
                    f"Column '{col_name}' min_value must be a number for \
                        data_type '{data_type}'.",
                )

        if "max_value" in col_config:
            max_val = col_config["max_value"]
            if data_type in self.numeric_types and not isinstance(
                max_val,
                (int, float),
            ):
                col_errors.append(
                    f"Column '{col_name}' max_value must be a number for \
                        data_type '{data_type}'.",
                )

        if "min_value" in col_config and "max_value" in col_config:
            if data_type in self.numeric_types:
                if (
                    isinstance(col_config["min_value"], (int, float))
                    and isinstance(col_config["max_value"], (int, float))
                    and col_config["min_value"] > col_config["max_value"]
                ):
                    col_errors.append(
                        f"Column '{col_name}' min_value cannot be greater than \
                            max_value for data_type: {data_type}",
                    )
            elif data_type in self.datetime_types:
                try:
                    min_val = pd.to_datetime(col_config["min_value"])
                    max_val = pd.to_datetime(col_config["max_value"])
                    if min_val > max_val:
                        col_errors.append(
                            f"Column '{col_name}' min_value cannot be greater \
                                than max_value for data_type: {data_type}",
                        )
                except (ValueError, TypeError) as e:
                    col_errors.append(
                        f"Error comparing datetime values for column\
                             '{col_name}': {e}",
                    )
        return col_errors

    def _validate_possible_values(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'possible_values' field.

        Checks that the field is a list with values of the correct type,
        and warns about non-categorical uses.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        possible_values = col_config["possible_values"]
        if not isinstance(possible_values, list):
            col_errors.append(
                f"Column '{col_name}': 'possible_values' must be a list.",
            )
        data_type = col_config.get("data_type")
        if data_type != "category":
            self.toml_val_logger.warning(
                f"Column '{col_name}': Using 'possible_values' with \
                    data_type '{data_type}'"
                "might not be memory-efficient. "
                "Consider using 'category' data_type.",
            )
        if data_type:
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
                "category": object,
            }
            expected_type = type_check_map.get(data_type)
            if expected_type:
                for value in possible_values:
                    if (
                        not isinstance(value, expected_type)
                        and value != "nan"
                        and value is not None
                    ):
                        col_errors.append(
                            f"Column '{col_name}': Value '{value}' in \
                                'possible_values' is not type '{data_type}'.",
                        )
        return col_errors

    def _validate_regex_pattern(
        self,
        col_config: Dict[str, Any],
        errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'regex_pattern' field.

        Checks that the pattern is valid regex and appropriate for data type.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        pattern = col_config["regex_pattern"]
        try:
            re.compile(pattern)
        except re.error:
            errors.append(
                f"Column '{col_name}': Invalid regex pattern '{pattern}'.",
            )
        data_type = col_config.get("data_type")
        if data_type not in self.string_types:
            errors.append(
                f"Column '{col_name}': 'regex_pattern' can only be applied to \
                    string type columns.",
            )
        return errors

    def _validate_unique(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'unique' field.

        Checks that the unique field is a boolean.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        if not isinstance(col_config["unique"], bool):
            col_errors.append(
                f"Column '{col_name}': 'unique' must be a boolean.",
            )
        return col_errors

    def _validate_date_format(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'date_format' field.

        Checks that the format is valid and appropriate for datetime types.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        date_format = col_config["date_format"]
        data_type = col_config.get("data_type")
        if data_type not in self.datetime_types:
            col_errors.append(
                f"Column '{col_name}': 'date_format' can only be used with \
                    datetime types, not '{data_type}'.",
            )
            return col_errors
        try:
            datetime.strptime("2024-05-03T00:00:00", date_format).replace(
                tzinfo=timezone.utc,
            )
        except ValueError:
            col_errors.append(
                f"Column '{col_name}': Invalid date format '{date_format}'.",
            )
        return col_errors

    def _validate_number_str_format(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'number_str_format' field.

        Checks that the format is valid and appropriate for numeric types.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        number_str_format = col_config["number_str_format"]
        data_type = col_config.get("data_type")
        if data_type not in self.numeric_types:
            col_errors.append(
                f"Column '{col_name}': 'number_str_format' can only be used \
                    with numeric types, not '{data_type}'.",
            )
            return col_errors
        try:
            if data_type in [float, "float64", "float32", "DoubleType"]:
                test_number = 1234.56
            elif data_type == str or data_type == "StringType":
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
            format_string = col_config["number_str_format"]
            format_string.format(test_number)
        except (ValueError, TypeError, KeyError) as e:
            col_errors.append(
                f"Column '{col_name}': Invalid number format \
                    '{number_str_format}' - {e}",
            )
        return col_errors

    def _validate_custom_check(
        self,
        col_config: Dict[str, Any],
        col_errors: List[str],
        col_name: str,
    ) -> List[str]:
        """Validate the 'custom_check' field.

        Checks that the custom check is either a valid function name or
        executable Python code.

        Parameters
        ----------
        col_config : Dict[str, Any]
            Configuration for a specific column
        col_errors : List[str]
            List of current errors for this column
        col_name : str
            Name of the column being validated

        Returns
        -------
        List[str]
            Updated list of errors
        """
        custom_check_val = col_config["custom_check"]
        if isinstance(custom_check_val, str):
            try:
                from rdsa_utils.rdsa_data_validator import data_validation

                check_function = getattr(data_validation, custom_check_val)
                if not callable(check_function):
                    col_errors.append(
                        f"Column '{col_name}': '{custom_check_val}' is not a \
                            callable in data_validation.py.",
                    )
            except AttributeError:
                try:
                    compile(custom_check_val, "<string>", "exec")
                except (SyntaxError, TypeError, ValueError) as e:
                    col_errors.append(
                        f"Column '{col_name}': Invalid Python code or function \
                            name in 'custom_check': {e}",
                    )
        elif not callable(custom_check_val):
            col_errors.append(
                f"Column '{col_name}': 'custom_check' must be\
                    a string or callable.",
            )
        return col_errors

    def _log_errors(self, errors_dict):
        """Log all errors found in the errors dictionary.

        Parameters
        ----------
        errors_dict : Dict[str, List[str]]
            Dictionary mapping column names to lists of error messages
        """
        for col_name, errors in errors_dict.items():
            if errors:
                for error in errors:
                    self.toml_val_logger.error(f"Column '{col_name}': {error}")

    def _go_no_go(self, errors_dict, stop_on_errors=True, threshold=0):
        """Stop the pipeline if the number of errors exceeds the threshold.

        Parameters
        ----------
        errors_dict : Dict[str, List[str]]
            Dictionary mapping column names to lists of error messages
        stop_on_errors : bool, optional
            Whether to raise exception if errors exceed threshold, default=True
        threshold : int, optional
            Maximum number of errors allowed before stopping, default=0

        Raises
        ------
        ValueError
            If number of errors exceeds the threshold and stop_on_errors is True
        """
        total_errors = sum(len(errs) for errs in errors_dict.values() if errs)
        if total_errors > threshold and stop_on_errors:
            error_messages = []
            for col, errs in errors_dict.items():
                if errs:
                    for error in errs:
                        error_messages.append(f"Column '{col}': {error}")
            raise ValueError(
                f"Validation failed with {total_errors} errors:\n"
                + "\n".join(error_messages),
            )
        elif total_errors > 0:
            for col, errs in errors_dict.items():
                for error in errs:
                    self.toml_val_logger.warning(f"Column '{col}': {error}")
        else:
            self.toml_val_logger.info("Validation successful. No errors found.")

    def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate the entire schema.

        Parameters
        ----------
        schema : Dict[str, Any]
            The schema to validate

        Returns
        -------
        Dict[str, List[str]]
            Dictionary mapping column names to lists of error messages
        """
        errors = {}
        self._select_validation_functions(schema)
        for col_name, val_func_names in self.selected_functions.items():
            errors[col_name] = []
            for func_name in val_func_names:
                val_func = self.validation_functions.get(func_name)
                if val_func:
                    errors[col_name] = val_func(
                        schema.get(col_name, {}),
                        errors[col_name],
                        col_name,
                    )
                else:
                    self.toml_val_logger.warning(
                        f"Validation function '{func_name}' not found.\
                            Skipping.",
                    )
        return errors

    def run_validation(self, toml_path: str) -> None:
        """Load the schema, run validation, and handle results.

        Parameters
        ----------
        toml_path : str
            Path to the TOML file containing the schema to validate
        """
        schema = self._load_validation_schema(toml_path)
        if not schema:
            self.toml_val_logger.error(
                "Schema is empty. Cannot proceed with validation.",
            )
            return
        errors = self.validate_schema(schema)
        self._log_errors(errors)
        self._go_no_go(errors)


if __name__ == "__main__":
    validator = TOMLSchemaValidator()
    toml_file_path = (
        Path("rdsa_utils") / "rdsa_data_validator" / "example_dataframe_schema.toml"
    )
    validator.run_validation(str(toml_file_path))
