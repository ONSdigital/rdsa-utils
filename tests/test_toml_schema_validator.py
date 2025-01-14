import sys
from pathlib import Path
from unittest import mock
from unittest.mock import mock_open

import pytest

from rdsa_utils.rdsa_data_validator.toml_schema_validator import TOMLSchemaValidator

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


# Fixtures to provide TOML file paths (adjust paths as needed)
@pytest.fixture()
def valid_test_toml_path():
    return str(Path(__file__).parent / "test_schema.toml")


@pytest.fixture()
def invalid_test_toml_path():
    return str(Path(__file__).parent / "invalid_test_schema.toml")


@pytest.fixture()
def toml_schema_validator_config_path():
    return str(Path(__file__).parent / "toml_schema_validator_config.toml")


@pytest.fixture()
def non_dict_toml_path():
    return str(Path(__file__).parent / "non_dict.toml")


# Tests
def test_load_validation_schema_valid(
    valid_test_toml_path, toml_schema_validator_config_path, caplog
):
    validator = TOMLSchemaValidator(
        schema_file_path=valid_test_toml_path,
        config_file_path=toml_schema_validator_config_path,
    )  # Schema & config loaded at init
    assert validator.schema is not None  # Check schema loaded correctly
    assert validator.schema["column1"]["description"] == "Test column 1"
    assert validator.schema["column2"]["data_type"] == "StringType"


def test_load_validation_schema_invalid_toml(
    invalid_test_toml_path, toml_schema_validator_config_path, caplog
):
    with pytest.raises(tomllib.TOMLDecodeError):
        TOMLSchemaValidator(
            schema_file_path=invalid_test_toml_path,  # Provide path to invalid toml
            config_file_path=toml_schema_validator_config_path,
        )
    assert "Invalid TOML" in caplog.text  # Check the specific message


def test_load_validation_schema_nonexistent_schema_file(
    toml_schema_validator_config_path, caplog
):
    nonexistent_path = "nonexistent_file.toml"  # Or use tmp_path to create a guaranteed nonexistent path

    with pytest.raises(FileNotFoundError):
        TOMLSchemaValidator(
            schema_file_path=nonexistent_path,
            config_file_path=toml_schema_validator_config_path,
        )
    assert f"TOML file not found at: {nonexistent_path}" in caplog.text
