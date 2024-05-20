from typing import List

import pytest
from pydantic import BaseModel, ValidationError

from rdsa_utils.io.config import *
from tests.conftest import Case, parametrize_cases


class ValidatorSection1(BaseModel):
    """Pydantic validator used with TestLoadConfig."""

    value_1_1: int
    value_1_2: str


class ValidatorSection2(BaseModel):
    """Pydantic validator used with TestLoadConfig."""

    value_2_1: List[int]
    value_2_2: List[int]


class _ValidatorSection31(BaseModel):
    """Pydantic validator used with TestLoadConfig."""

    value_3_1_1: int
    value_3_1_2: str


class ValidatorSection3(BaseModel):
    """Pydantic validator used with TestLoadConfig."""

    value_3_1: _ValidatorSection31


class ValidatorSection4(BaseModel):
    """Pydantic validator used with TestLoadConfig."""

    value_4_1: bool


class TestLoadConfig:
    """Tests for the LoadConfig class."""

    @pytest.fixture(scope="class")
    def config_json(self, json_config_string, tmp_path_factory) -> Path:
        """Fixture to create json file that exists for the length of the class tests."""
        config_file = tmp_path_factory.mktemp("data") / "config.json"
        config_file.write_text(json_config_string)
        return config_file

    @pytest.fixture(scope="class")
    def config_toml(self, toml_config_string, tmp_path_factory) -> Path:
        """Fixture to create toml file that exists for the length of the class tests."""
        config_file = tmp_path_factory.mktemp("data") / "config.toml"
        config_file.write_text(toml_config_string)
        return config_file

    @pytest.fixture(scope="class")
    def config_yaml(self, yaml_config_string, tmp_path_factory) -> Path:
        """Fixture to create yaml file that exists for the length of the class tests."""
        config_file = tmp_path_factory.mktemp("data") / "config.yaml"
        config_file.write_text(yaml_config_string)
        return config_file

    @parametrize_cases(
        Case(
            label="json_with_config_type_specified",
            config_file=pytest.lazy_fixture("config_json"),
            config_type="json",
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
        Case(
            label="json_no_config_type_specified",
            config_file=pytest.lazy_fixture("config_json"),
            config_type=None,
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
        Case(
            label="toml_with_config_type_specified",
            config_file=pytest.lazy_fixture("config_toml"),
            config_type="toml",
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
        Case(
            label="toml_no_config_type_specified",
            config_file=pytest.lazy_fixture("config_toml"),
            config_type=None,
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
        Case(
            label="yaml_with_config_type_specified",
            config_file=pytest.lazy_fixture("config_yaml"),
            config_type="yaml",
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
        Case(
            label="yaml_no_config_type_specified",
            config_file=pytest.lazy_fixture("config_yaml"),
            config_type=None,
            expected_config=pytest.lazy_fixture("expected_standard_config"),
        ),
    )
    def test_load_config_file_type(
        self,
        config_file,
        config_type,
        expected_config,
    ):
        """Test class can load different file types."""
        actual = LoadConfig(
            config_path=config_file,
            config_type=config_type,
        )

        assert actual.config == expected_config

    def test_has_config_contents_as_attributes(
        self,
        config_yaml,
        expected_standard_config,
    ):
        """Test returned class has an attribute for each section of config."""
        actual = LoadConfig(
            config_path=config_yaml,
        )

        for key, value in expected_standard_config.items():
            print(key)  # noqa: T201
            assert hasattr(actual, key)
            assert getattr(actual, key) == value

    def test_raises_config_error_applying_unsupported_config_file_type(
        self,
        config_yaml,
    ):
        """Test class can raises ConfigError for unsupported config file type."""
        with pytest.raises(
            ConfigError,
            match="No config parser present for file type = xlsx",
        ):
            LoadConfig(
                config_path=config_yaml,
                config_type="xlsx",
            )

    def test_expected_applying_config_override(
        self,
        config_yaml,
        expected_standard_config,
    ):
        """Test class applies config overrides."""
        config_overrides = {
            "section_1": {
                "value_1_2": "z",
            },
            "section_2": {
                "value_2_2": [9, 8, 7],
            },
            "section_3": {
                "value_3_1": {
                    "value_3_1_2": "y",
                },
            },
            "section_4": {
                "value_4_1": True,
            },
        }

        actual = LoadConfig(
            config_path=config_yaml,
            config_overrides=config_overrides,
        )

        expected = {
            "section_1": {
                "value_1_1": 1,
                "value_1_2": "z",
            },
            "section_2": {
                "value_2_1": [3, 4],
                "value_2_2": [9, 8, 7],
            },
            "section_3": {
                "value_3_1": {
                    "value_3_1_1": 2,
                    "value_3_1_2": "y",
                },
            },
            "section_4": {
                "value_4_1": True,
            },
        }

        assert actual.config == expected

        assert actual.config_original == expected_standard_config

    def test_raises_config_error_applying_bad_config_override(
        self,
        config_yaml,
    ):
        """Test class raises ConfigError for bad config overrides."""
        with pytest.raises(ConfigError, match="not in the base dictionary"):
            LoadConfig(
                config_path=config_yaml,
                config_overrides={"section_4": {"value_4_2": "not in base config"}},
            )

    def test_expected_apply_all_config_validators(
        self,
        config_yaml,
        expected_standard_config,
    ):
        """Test applying validation maintains config values."""
        actual = LoadConfig(
            config_path=config_yaml,
            config_validators={
                "section_1": ValidatorSection1,
                "section_2": ValidatorSection2,
                "section_3": ValidatorSection3,
                "section_4": ValidatorSection4,
            },
        )

        assert actual.config == expected_standard_config

    @parametrize_cases(
        Case(
            label="section_not_specified",
            config_validators={
                "section_2": ValidatorSection2,
                "section_3": ValidatorSection3,
                "section_4": ValidatorSection4,
            },
        ),
        Case(
            label="section_specified_as_none",
            config_validators={
                "section_1": None,
                "section_2": ValidatorSection2,
                "section_3": ValidatorSection3,
                "section_4": ValidatorSection4,
            },
        ),
    )
    def test_raise_warning_if_apply_some_config_validators(
        self,
        config_yaml,
        config_validators,
        expected_standard_config,
    ):
        """Test applying only some validators raises appropriate warnings."""
        with pytest.warns(
            UserWarning,
            match="No validator provided, config contents unvalidated.",
        ):
            actual = LoadConfig(
                config_path=config_yaml,
                config_validators=config_validators,
            )
            assert actual.config == expected_standard_config

    def test_raises_validation_error_config_override_changes_value_type_and_apply_config_validators(
        self,
        config_yaml,
    ):
        """Test that if config override changes value type and using validators then validation error raised."""
        with pytest.raises(ValidationError, match="value_4_1"):
            LoadConfig(
                config_path=config_yaml,
                config_overrides={"section_4": {"value_4_1": "should be bool"}},
                config_validators={
                    "section_1": ValidatorSection1,
                    "section_2": ValidatorSection2,
                    "section_3": ValidatorSection3,
                    "section_4": ValidatorSection4,
                },
            )
