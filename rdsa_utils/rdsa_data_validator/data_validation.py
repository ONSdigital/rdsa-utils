"""Functions that support the use of Great Expectation validators."""

import logging
from typing import Any, Dict

import great_expectations as gx
import pandas as pd
from great_expectations.core.expectation_suite import ExpectationSuite

try:
    import tomllib as tomli
except ImportError:
    import tomli

logger = logging.getLogger(__name__)


class GreatExpectationDataFrameValidator:
    """Validates Pandas DataFrames using Great Expectations.

    This class provides methods to create and utilise Great Expectations
    Expectation Suites for validating the schema and data quality of
    Pandas DataFrames. It leverages the TOML configuration for schema
    definition and integrates with Great Expectations for comprehensive
    validation and reporting.

    Methods:
    ----------
        create_expectation_suite_from_toml: Creates an Expectation Suite
            from a TOML file.
        validate_dataframe_with_expectation_suite: Validates a DataFrame
            against an Expectation Suite.
        validate_column_with_expectation_suit: Validates a specific column
            within a DataFrame.

    """

    def __init__(self, data_context_mode: str = "ephemeral"):
        """Initialize the validator with a Great Expectations context.

        Args:
            data_context_mode (str): Mode for GX context ("ephemeral", "file", "cloud")
        """
        self.context = gx.get_context(mode=data_context_mode)
        self._expectation_type_mapping = self._build_expectation_type_mapping()

    def _build_expectation_type_mapping(self) -> Dict[str, str]:
        """Map TOML schema validation fields to GX expectation types.

        Returns:
        --------
            Dict mapping TOML field types to GX expectation classes
        """
        return {
            "nullable": "expect_column_values_to_not_be_null",
            "data_type": "expect_column_values_to_be_of_type",
            "min_value": "expect_column_values_to_be_between",
            "max_value": "expect_column_values_to_be_between",
            "possible_values": "expect_column_values_to_be_in_set",
            "regex_pattern": "expect_column_values_to_match_regex",
            "length": "expect_column_value_lengths_to_be_between",
            "unique": "expect_column_values_to_be_unique",
        }

    def create_expectation_suite_from_toml(self, toml_path, data_asset_name):
        """Create a Great Expectations Expectation Suite from a TOML
        configuration file.

        This method reads a TOML configuration file and transforms its schema
        into a Great Expectations Expectation Suite.

        Args:
            toml_path (str): The path to the data schema for the data asset.
            data_asset_name (str): The name of the dataframe to be validated.

        Returns
        -------
            great_expectations.core.ExpectationSuite: The created Expectation
            Suite.
        """
        with open(toml_path, "rb") as f:
            schema = tomli.load(f)

        suite = self.context.create_expectation_suite(
            expectation_suite_name=f"{data_asset_name}_suite",
            overwrite_existing=True,
        )

        for col_name, col_config in schema.items():
            if col_name == "data_asset":
                continue

            # process each validation constraint for the column
            self._add_column_expectations(suite, col_name, col_config)

        return suite

    def _add_column_expectations(
        self, suite: ExpectationSuite, col_name: str, col_config: Dict[str, Any]
    ) -> None:
        """Add expectations for a column based on TOML config.

        Args:
            suite: The expectation suite to add to
            col_name: The column name
            col_config: Column configuration from TOML
        """
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col_name))

        if "nullable" in col_config and not col_config["nullable"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=col_name)
            )

        if "data_type" in col_config:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=col_name, type_=col_config["data_type"]
                )
            )

        if "min_value" in col_config and "max_value" in col_config:
            if col_config["min_value"] != "nan" or col_config["max_value"] != "nan":
                min_val = (
                    None
                    if col_config["min_value"] == "nan"
                    else col_config["min_value"]
                )
                max_val = (
                    None
                    if col_config["max_value"] == "nan"
                    else col_config["max_value"]
                )

                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=col_name, min_value=min_val, max_value=max_val
                    )
                )
        if "possible_values" in col_config and col_config["possible_values"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column=col_name, value_set=col_config["possible_values"]
                )
            )

        if "regex_pattern" in col_config and col_config["regex_pattern"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToMatchRegex(
                    column=col_name, regex=col_config["regex_pattern"]
                )
            )

        if "length" in col_config and col_config["length"]:
            length_spec = col_config["length"]
            min_len = None
            max_len = None
            length_parsed = True

            if isinstance(length_spec, int):
                # Exact lenght
                min_len = length_spec
                max_len = length_spec
            elif isinstance(length_spec, str):
                if length_spec.startswith(">="):
                    # minimum length
                    threshold = length_spec[2:].strip()
                    try:
                        min_len = int(threshold)
                    except ValueError:
                        logger.warning(
                            f"Invalid length format: '{length_spec}' for column '{col_name}'. Skipping length expectation."
                        )
                        length_parsed = False
                elif length_spec.startswith(">"):
                    # mnimum length (exclusive)
                    threshold = length_spec[1:].strip()
                    try:
                        min_len = int(threshold) + 1
                    except ValueError:
                        logger.warning(
                            f"Invalid length format: '{length_spec}' for column '{col_name}'. Skipping length expectation."
                        )
                        length_parsed = False
                else:
                    # try to parse as an integer (exact length)
                    try:
                        value_parsed = int(length_spec)
                        min_len = value_parsed
                        max_len = value_parsed
                    except ValueError:
                        logger.warning(
                            f"Invalid length format: '{length_spec}' for column '{col_name}'. Skipping length expectation."
                        )
                        length_parsed = False
            else:
                logger.warning(
                    f"Unsupported length type: {type(length_spec)} for column '{col_name}'. Skipping length expectation."
                )
                length_parsed = False

            if length_parsed:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValueLengthsToBeBetween(
                        column=col_name, min_value=min_len, max_value=max_len
                    )
                )

        if "unique" in col_config and col_config["unique"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeUnique(column=col_name)
            )

        if "date_format" in col_config and col_config["date_format"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
                    column=col_name, strftime_format=col_config["date_format"]
                )
            )

    def validate_dataframe_with_expectation_suite(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: ExpectationSuite,
        result_format: str = "SUMMARY",
    ) -> Dict:
        """Validate a DataFrame against a Great Expectations Expectation Suite.

        Args:
            dataframe (pd.DataFrame): The Pandas DataFrame to validate.
            expectation_suite (ExpectationSuite): The Expectation Suite to use
                for validation.
            result_format (str): Format for results ("BOOLEAN_ONLY", "BASIC",
                "COMPLETE", or "SUMMARY")

        Returns:
        --------
            Dict containing validation results.
        """
        datasource_name = "temp_datasource"
        if datasource_name not in self.context.datasources:
            self.context.sources.add_pandas(name=datasource_name)

        asset_name = "temp_asset"
        datasource = self.context.get_datasource(datasource_name)

        try:
            asset = datasource.get_asset(asset_name)
        except KeyError:
            asset = datasource.add_dataframe_asset(name=asset_name)

        batch_def_name = "temp_batch_def"
        try:
            batch_def = asset.get_batch_definition(batch_def_name)
        except KeyError:
            batch_def = asset.add_batch_definition_whole_dataframe(name=batch_def_name)

        batch = batch_def.get_batch(batch_parameters={"dataframe": dataframe})
        validation_result = batch.validate(
            expectation_suite=expectation_suite, result_format=result_format
        )

        return validation_result.to_json_dict()

    def validate_column_with_expectation_suit(
        self,
        dataframe: pd.DataFrame,
        column_name: str,
        expectation_suite: ExpectationSuite,
        result_format: str = "SUMMARY",
    ) -> Dict:
        """Validate a specific DataFrame column against an Expectation Suite.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the column.
            column_name (str): The name of the column to validate.
            expectation_suite (ExpectationSuite): The Expectation Suite to use.
            result_format (str): Format for results ("BOOLEAN_ONLY", "BASIC",
                "COMPLETE", or "SUMMARY")

        Returns:
        --------
            Dict containing validation results for the specified column.
        """
        datasource_name = "temp_column_datasource"
        if datasource_name not in self.context.datasources:
            self.context.sources.add_pandas(name=datasource_name)
        asset_name = f"column_{column_name}_asset"
        datasource = self.context.get_datasource(datasource_name)

        try:
            asset = datasource.get_asset(asset_name)
        except KeyError:
            asset = datasource.add_dataframe_asset(name=asset_name)

        batch_def_name = f"column_{column_name}_batch_def"
        try:
            batch_def = asset.get_batch_definition(batch_def_name)
        except KeyError:
            batch_def = asset.add_batch_definition_whole_dataframe(name=batch_def_name)

        batch = batch_def.get_batch(batch_parameters={"dataframe": dataframe})
        column_expectations = ExpectationSuite(
            expectation_suite_name=f"{expectation_suite.expectation_suite_name}_{column_name}",
            expectations=[
                exp
                for exp in expectation_suite.expectations
                if exp.kwargs.get("column") == column_name
            ],
        )

        validation_result = batch.validate(
            expectation_suite=column_expectations, result_format=result_format
        )

        return validation_result.to_json_dict()

    def generate_data_docs(self, validation_result=None):
        """Generate and open Data Docs for the validation results.

        Args:
            validation_result: Optional validation result to include (N.B. Not implemented)
        """
        # TODO: Implement validation_result logic
        docs_sites = self.context.build_data_docs()

        # Open the data docs in browser if available
        if docs_sites:
            self.context.open_data_docs()
