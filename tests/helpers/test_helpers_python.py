"""Tests for the helpers module."""
import pytest

from tests.conftest import (
    Case,
    parametrize_cases,
)
from rdsa_utils.helpers.helpers_python import *


@pytest.mark.skip(reason="wrapper of third party function")
class TestAlwaysIterableLocal:
    """Tests for the always_iterable_local function."""

    pass


class TestTupleConvert:
    """Tests for tuple_convert."""

    def test_leaves_tuple_as_is(self):
        """Test method."""
        assert tuple_convert(("beans", "toast")) == ("beans", "toast")

    def test_converts_list_to_tuple(self):
        """Test method."""
        assert tuple_convert(["carnage", "venom"]) == ("carnage", "venom")

    def test_wraps_string_in_tuple_container(self):
        """Test method."""
        assert tuple_convert("rice") == ("rice",)

    def test_does_not_convert_dict(self):
        """Test that dictionary is not converted to just tuple of keys."""
        assert tuple_convert({"key": "value"}) == ({"key": "value"},)

    def test_converts_none_to_empty(self):
        """Test that when None passed tuple doesn't contain None value."""
        assert tuple_convert(None) == tuple()

    @pytest.mark.parametrize("obj", [67, 2.75])
    def test_wraps_other_objs_in_tuple_container(self, obj):
        """Test method."""
        assert tuple_convert(obj) == (obj,)


class TestListConvert:
    """Tests for list_convert."""

    def test_leaves_list_as_is(self):
        """Test method."""
        assert list_convert(["beans", "toast"]) == ["beans", "toast"]

    def test_converts_tuple_to_list(self):
        """Test method."""
        assert list_convert(("carnage", "venom")) == ["carnage", "venom"]

    def test_wraps_string_in_list_container(self):
        """Test method."""
        assert list_convert("rice") == ["rice"]

    def test_does_not_convert_dict(self):
        """Test that dictionary is not converted to just list of keys."""
        assert list_convert({"key": "value"}) == [{"key": "value"}]

    def test_converts_none_to_empty(self):
        """Test that when None passed list doesn't contain None value."""
        assert list_convert(None) == list()

    @pytest.mark.parametrize("obj", [67, 2.75])
    def test_wraps_other_objs_in_list_container(self, obj):
        """Test method."""
        assert list_convert(obj) == [obj]


class TestExtendLists:
    """Tests for extend_lists function."""

    def test_expected(self):
        """Test expected functionality."""
        sections = [[1, 2], [3, 4]]
        elements_to_add = [5, 6]
        extend_lists(
            sections,
            elements_to_add,
        )
        expected = [[1, 2, 5, 6], [3, 4, 5, 6]]
        assert sections == expected


class TestOverwriteDictionary:
    """Tests for the overwrite_dictionary function."""

    @pytest.fixture
    def base_dict(self):
        """Create base dictionary used across all tests."""
        return {
            "var1": "value1",
            "var2": {"var3": 1.1, "var4": 4.4},
            "var5": [1, 2, 3],
            "var6": {
                "var7": {
                    "var9": "helo",
                }
            },
        }

    @parametrize_cases(
        Case(
            label="overwrites_top_level_argument",
            config=pytest.lazy_fixture("base_dict"),
            override_dict={"var1": "value99"},
            expected={
                "var1": "value99",
                "var2": {"var3": 1.1, "var4": 4.4},
                "var5": [1, 2, 3],
                "var6": {
                    "var7": {
                        "var9": "helo",
                    }
                },
            },
        ),
        Case(
            label="overwrites_nested_level_argument",
            config=pytest.lazy_fixture("base_dict"),
            override_dict={"var6": {"var7": {"var9": "hwyl fawr"}}},
            expected={
                "var1": "value1",
                "var2": {"var3": 1.1, "var4": 4.4},
                "var5": [1, 2, 3],
                "var6": {
                    "var7": {
                        "var9": "hwyl fawr",
                    }
                },
            },
        ),
        Case(
            label="does_not_overwrite_dict_with_value",
            config=pytest.lazy_fixture("base_dict"),
            override_dict={"var2": "value99", "var6": ["a", "b", "c", "ch"]},
            expected={
                "var1": "value1",
                "var2": {"var3": 1.1, "var4": 4.4},
                "var5": [1, 2, 3],
                "var6": {
                    "var7": {
                        "var9": "helo",
                    }
                },
            },
        ),
    )
    def test_method(self, config, override_dict, expected):
        """Test expected behaviour."""
        result = overwrite_dictionary(config, override_dict)
        assert result == expected

    def test_raises_when_key_missing(self, base_dict):
        """Test error raised if override key isn't present in base_dict."""
        override_dict = {"var10": "value404"}
        with pytest.raises(Exception):
            overwrite_dictionary(base_dict, override_dict)


class TestCalcProductOfDictValues:
    """Tests for the calc_product_of_dict_values function."""

    def test_with_single_input(self):
        """Test method functionality with single input."""
        input_dict = {"key1": 1, "key2": [2, 3, 4]}
        result = list(calc_product_of_dict_values(**input_dict))

        expected = [
            {"key1": 1, "key2": 2},
            {"key1": 1, "key2": 3},
            {"key1": 1, "key2": 4},
        ]
        assert result == expected

    def test_with_many_input(self):
        """Test method functionality with many inputs."""
        input_dict1 = {"key1": 1, "key2": [2, 3, 4]}
        input_dict2 = {"key3": "raaaaa", "key4": ["a", "b"]}
        result = list(calc_product_of_dict_values(**input_dict1, **input_dict2))

        expected = [
            {"key1": 1, "key2": 2, "key3": "raaaaa", "key4": "a"},
            {"key1": 1, "key2": 2, "key3": "raaaaa", "key4": "b"},
            {"key1": 1, "key2": 3, "key3": "raaaaa", "key4": "a"},
            {"key1": 1, "key2": 3, "key3": "raaaaa", "key4": "b"},
            {"key1": 1, "key2": 4, "key3": "raaaaa", "key4": "a"},
            {"key1": 1, "key2": 4, "key3": "raaaaa", "key4": "b"},
        ]
        assert result == expected

    def test_with_dictionary_as_value(self):
        """Test method functionality when there is a nested dictionary.

        Expected behaviour is to treat the value as a single object (i.e. don't
        compute product of its values).
        """
        input_dict = {
            "key1": 1,
            "key2": [2, 3, 4],
            "key3": {"key4": [7, 8, 9], "key5": [4, 5, 6]},
        }

        result = list(calc_product_of_dict_values(**input_dict))

        expected = [
            {"key1": 1, "key2": 2, "key3": {"key4": [7, 8, 9], "key5": [4, 5, 6]}},
            {"key1": 1, "key2": 3, "key3": {"key4": [7, 8, 9], "key5": [4, 5, 6]}},
            {"key1": 1, "key2": 4, "key3": {"key4": [7, 8, 9], "key5": [4, 5, 6]}},
        ]
        assert result == expected
