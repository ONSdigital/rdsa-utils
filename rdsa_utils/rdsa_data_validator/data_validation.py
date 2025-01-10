"""Functions that support the use of Great Expectation validators."""

import logging

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

    def __init__(self):
        pass

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
        pass

    def validate_dataframe_with_expectation_suite(self,
                                                  dataframe,
                                                  expectation_suite):
        """Validate a DataFrame against a Great Expectations Expectation Suite.

        Args:
             dataframe (pd.DataFrame): The Pandas DataFrame to validate.
             expectation_suite (great_expectations.core.ExpectationSuite):
             The Expectation Suite to use for validation.
        """
        pass

    def validate_column_with_expectation_suit(
        self,
        dataframe,
        column_name,
        expectation_suite,
    ):
        """Validate a specific DataFrame column against an Expectation Suite.

        Args:
             dataframe (pd.DataFrame): The DataFrame containing the column.
             column_name (str): The name of the column to validate.
             expectation_suite (great_expectations.core.ExpectationSuite):
             The Expectation Suite to use.

        """
        pass
