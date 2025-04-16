"""Example usage of the RDSA Data Validator."""

import logging
from pathlib import Path

import pandas as pd

from rdsa_utils.rdsa_data_validator.data_validation import (
    GreatExpectationDataFrameValidator,
)
from rdsa_utils.rdsa_data_validator.toml_schema_validator import TOMLSchemaValidator
from rdsa_utils.rdsa_data_validator.utils import format_validation_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CURRENT_DIR = Path(__file__).parent
SCHEMA_PATH = CURRENT_DIR / "example_dataframe_schema.toml"


data = {
    "passenger_count": [1, 2, 3, 6, 7],
    "pickup_datetime": [
        "2023-01-01",
        "2023-01-02",
        "2023-01-03",
        "2023-01-04",
        "2023-01-05",
    ],
    "pickup_location_id": ["X1001", "X1002", "X1003", "X1004", "X1005"],
    "reference": ["11001603625", "19891309165", "a", "b", "c"],
    "period": [
        202012,
        202012,
        202012,
        202012,
        202011,
    ],
    "survey": [
        "002",
        "002",
        "002",
        "002",
        "003",
    ],
}


def main():
    """
    Run the data validation workflow using TOML schema and Great Expectations.

    Creates a sample DataFrame, validates it against a TOML schema,
    and then applies Great Expectations validations. Also generates data documentation.
    """
    df = pd.DataFrame(data)
    logger.info(f"Created sample dataframe with {len(df)} rows")

    toml_validator = TOMLSchemaValidator()
    schema = toml_validator._load_validation_schema(str(SCHEMA_PATH))
    errors = toml_validator.validate_schema(schema)

    has_errors = any(len(err_list) > 0 for err_list in errors.values())
    if has_errors:
        logger.error("Schema validation failed")
        for col, err_list in errors.items():
            if err_list:
                error_msg = (
                    f"Column '{col}' has {len(err_list)} validation issue(s): "
                    + "; ".join(err_list)
                )
                logger.error(error_msg)
        return

    logger.info("Schema validation successful")

    gx_validator = GreatExpectationDataFrameValidator()
    suite = gx_validator.create_expectation_suite_from_toml(
        str(SCHEMA_PATH),
        "example_survey_results",
    )

    logger.info(
        f"Created GX expectation suite with {len(suite.expectations)} expectations",
    )

    validation_results = gx_validator.validate_dataframe_with_expectation_suite(
        df,
        suite,
    )

    formatted_results = format_validation_results(validation_results)

    summary = formatted_results["summary"]
    logger.info(
        f"Validation Summary: {summary['successful_expectations']} of "
        f"{summary['evaluated_expectations']} "
        f"expectations passed ({summary['unsuccessful_expectations']} failed)",
    )

    if summary["unsuccessful_expectations"] > 0:
        logger.warning(
            f"Found {summary['unsuccessful_expectations']} validation failures",
        )

        failures_by_column = {}
        for res in formatted_results["results"]:
            if not res["success"]:
                col = res.get("column", "N/A")
                if col not in failures_by_column:
                    failures_by_column[col] = []
                failures_by_column[col].append(
                    {
                        "expectation_type": res["expectation_type"],
                        "details": res.get("details", {}),
                    },
                )

        for col, failures in failures_by_column.items():
            failure_msgs = []
            for f in failures:
                failure_msgs.append(f"{f['expectation_type']}: {f['details']}")

            logger.warning(
                f"Column '{col}' has {len(failures)} failed expectation(s):\n"
                + "\n".join(f"  - {msg}" for msg in failure_msgs),
            )

    gx_validator.generate_data_docs()
    logger.info("Generated Great Expectations Data Docs")


if __name__ == "__main__":
    main()
