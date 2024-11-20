"""Functions, classes and fixtures available across all tests in io."""

from typing import Any, Dict

import pytest


@pytest.fixture(scope="session")
def json_config_string() -> str:
    """Fixture to provide a json format string."""
    return (
        "{"
        '"section_1": {"value_1_1": 1, "value_1_2": "a"},'
        '"section_2": {"value_2_1": [3, 4], "value_2_2": [7, 8, 9]},'
        ' "section_3": {"value_3_1": {"value_3_1_1": 2, "value_3_1_2": "b"}},'
        '"section_4": {"value_4_1": true}'
        "}"
    )


@pytest.fixture(scope="session")
def toml_config_string() -> str:
    """Fixture to provide a toml format string."""
    return """
        [section_1]
        value_1_1 = 1
        value_1_2 = 'a'

        [section_2]
        value_2_1 = [3, 4]
        value_2_2 = [7, 8, 9]

        [section_3]
            [section_3.value_3_1]
            value_3_1_1 = 2
            value_3_1_2 = 'b'

        [section_4]
        value_4_1 = true
    """


@pytest.fixture(scope="session")
def yaml_config_string() -> str:
    """Fixture to provide a yaml format string."""
    return """
        section_1:
            value_1_1: 1
            value_1_2: a

        section_2:
            value_2_1:
                - 3
                - 4
            value_2_2:
                - 7
                - 8
                - 9

        section_3:
            value_3_1:
                value_3_1_1: 2
                value_3_1_2: b

        section_4:
            value_4_1: True
    """


@pytest.fixture()
def expected_standard_config() -> Dict[str, Any]:
    """Fixture providing the loaded config from loading the temp file."""
    return {
        "section_1": {
            "value_1_1": 1,
            "value_1_2": "a",
        },
        "section_2": {
            "value_2_1": [3, 4],
            "value_2_2": [7, 8, 9],
        },
        "section_3": {
            "value_3_1": {
                "value_3_1_1": 2,
                "value_3_1_2": "b",
            },
        },
        "section_4": {
            "value_4_1": True,
        },
    }
