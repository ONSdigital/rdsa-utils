"""Utility functions for RDSA Data Validator."""

from pathlib import Path
from typing import Dict

from rdsa_utils.rdsa_data_validator.data_validation import (
    GreatExpectationDataFrameValidator,
)
from rdsa_utils.rdsa_data_validator.toml_schema_validator import TOMLSchemaValidator


def format_validation_results(validation_result: Dict) -> Dict:
    """Format GX validation results into a more readable structure.

    Parameters
    ----------
        validation_result: GX validation result dictionary

    Returns
    -------
        Simplified and formatted results
    """
    formatted_results = {
        "success": validation_result.get("success", False),
        "summary": {
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "evaluated_expectations": 0,
        },
        "results": [],
    }

    results = validation_result.get("results", [])
    formatted_results["summary"]["evaluated_expectations"] = len(results)

    for result in results:
        success = result.get("success", False)
        expectation_config = result.get("expectation_config", {})
        expectation_type = expectation_config.get("expectation_type", "unknown")
        column = expectation_config.get("kwargs", {}).get("column")

        if success:
            formatted_results["summary"]["successful_expectations"] += 1
        else:
            formatted_results["summary"]["unsuccessful_expectations"] += 1

        formatted_result = {
            "expectation_type": expectation_type,
            "column": column,
            "success": success,
        }

        if not success:
            if "result" in result:
                formatted_result["details"] = result["result"]

        formatted_results["results"].append(formatted_result)

    return formatted_results


def create_expectations_from_validated_schema(
    toml_path: str,
    validator: "GreatExpectationDataFrameValidator",
    toml_validator: "TOMLSchemaValidator" = None,
) -> Dict:
    """Create GX expectation suite from TOML after validation.

    Parameters
    ----------
        toml_path: Path to TOML schema file
        validator: GX DataFrame validator
        toml_validator: Optional TOML schema validator (created if None)

    Returns
    -------
        Dict with validation status and expectation suite
    """
    if toml_validator is None:
        toml_validator = TOMLSchemaValidator()

    schema = toml_validator._load_validation_schema(toml_path)
    if not schema:
        return {
            "success": False,
            "message": f"Failed to load TOML schema from {toml_path}",
        }

    errors = toml_validator.validate_schema(schema)
    has_errors = any(len(err_list) > 0 for err_list in errors.values())

    if has_errors:
        return {
            "success": False,
            "message": "Schema validation failed",
            "errors": errors,
        }

    data_asset_name = schema.get("data_asset", {}).get("name") or Path(toml_path).stem

    try:
        expectation_suite = validator.create_expectation_suite_from_toml(
            toml_path,
            data_asset_name,
        )
        return {
            "success": True,
            "expectation_suite": expectation_suite,
            "data_asset_name": data_asset_name,
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error creating expectation suite: {str(e)}",
        }
