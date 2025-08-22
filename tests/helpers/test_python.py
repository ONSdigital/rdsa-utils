"""Tests for the helpers/python.py module."""

from time import sleep
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from rdsa_utils.helpers.python import *
from rdsa_utils.test_utils import *


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
        assert tuple_convert(None) == ()

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
        assert list_convert(None) == []

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
                },
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
                    },
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
                    },
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
                    },
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
        with pytest.raises(ValueError):
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


class TestConvertDateStringsToDatetimes:
    """Test class for convert_date_strings_to_datetimes."""

    @parametrize_cases(
        Case(
            label="MMMM YYYY",
            dates=("January 2021", "March 2021"),
        ),
        Case(
            label="MMM YYYY",
            dates=("Jan 2021", "Mar 2021"),
        ),
        Case(
            label="YYYY MMMM",
            dates=("2021 January", "2021 March"),
        ),
        Case(
            label="YYYY MMM",
            dates=("2021 Jan", "2021 Mar"),
        ),
        Case(
            label="MM YYYY",
            dates=("01 2021", "03 2021"),
        ),
        Case(
            label="-M YYYY",
            dates=("1 2021", "3 2021"),
        ),
        Case(
            label="MM-YYYY",
            dates=("01-2021", "03-2021"),
        ),
        Case(
            label="-M-YYYY",
            dates=("1-2021", "3-2021"),
        ),
    )
    def test_year_month_format(
        self,
        dates,
    ):
        """Test different date formats for start and end date."""
        actual = convert_date_strings_to_datetimes(*dates)

        expected = (
            pd.Timestamp("2021-01-01 00:00:00"),
            pd.Timestamp("2021-03-31 23:59:59.999999"),
        )
        assert actual == expected

    def test_year_month_day_format(self):
        """Test for yyyy-mm-dd format."""
        dates = ("2021-01-01", "2021-03-01")

        actual = convert_date_strings_to_datetimes(*dates)

        expected = (
            pd.Timestamp("2021-01-01 00:00:00"),
            pd.Timestamp("2021-03-01 23:59:59.999999"),
        )
        assert actual == expected


class TestTimeIt:
    """Test class for the time_it decorator."""

    @mock.patch("rdsa_utils.helpers.python.logger.info")
    def test_time_it_execution(self, mock_logger):
        """Test with a function that takes arguments."""

        @time_it()
        def sample_function(delay):
            sleep(delay)
            return "Done"

        result = sample_function(1)

        assert result == "Done"

        mock_logger.assert_called_once()
        log_message = mock_logger.call_args[0][0]
        assert "Executed sample_function in" in log_message
        assert "seconds" in log_message

    @mock.patch("rdsa_utils.helpers.python.logger.info")  # Mock the logger
    def test_time_it_no_arguments(self, mock_logger):
        """Test with a function that takes no arguments."""

        @time_it()
        def sample_function():
            return "No arguments"

        result = sample_function()

        assert result == "No arguments"

        mock_logger.assert_called_once()
        log_message = mock_logger.call_args[0][0]
        assert "Executed sample_function in" in log_message
        assert "seconds" in log_message


class TestSetdiff:
    """Test class for the `setdiff` function."""

    def test_list_difference(self):
        """Test method."""
        a = [1, 2, 3, 4]
        b = [3, 4, 5, 6]
        result = setdiff(a, b)
        assert set(result) == {1, 2}

    def test_string_difference(self):
        """Test method."""
        a = "abcdef"
        b = "bdf"
        result = setdiff(a, b)
        assert set(result) == {"a", "c", "e"}

    def test_set_difference(self):
        """Test method."""
        a = {1, 2, 3}
        b = {2, 3, 4}
        result = setdiff(a, b)
        assert set(result) == {1}

    def test_range_difference(self):
        """Test method."""
        a = range(5)
        b = range(2, 7)
        result = setdiff(a, b)
        assert set(result) == {0, 1}

    def test_dict_keys_difference(self):
        """Test method."""
        a = {"a": 1, "b": 2, "c": 3}
        b = {"b": 4, "d": 5}
        result = setdiff(a.keys(), b.keys())
        assert set(result) == {"a", "c"}

    def test_empty_a(self):
        """Test method."""
        a = []
        b = [1, 2, 3]
        result = setdiff(a, b)
        assert result == []

    def test_empty_b(self):
        """Test method."""
        a = [1, 2, 3]
        b = []
        result = setdiff(a, b)
        assert set(result) == {1, 2, 3}

    def test_both_empty(self):
        """Test method."""
        a = []
        b = []
        result = setdiff(a, b)
        assert result == []

    def test_duplicates_in_a(self):
        """Test method."""
        a = [1, 2, 2, 3, 4]
        b = [3, 4]
        result = setdiff(a, b)
        assert set(result) == {1, 2}

    def test_non_iterable_a(self):
        """Test method."""
        b = [1, 2, 3]
        with pytest.raises(TypeError):
            setdiff(123, b)

    def test_non_iterable_b(self):
        """Test method."""
        a = [1, 2, 3]
        with pytest.raises(TypeError):
            setdiff(a, 123)

    def test_non_iterable_both(self):
        """Test method."""
        with pytest.raises(TypeError):
            setdiff(123, 456)

    def test_mixed_types(self):
        """Test method."""
        a = [1, "a", 3.5, (2, 3)]
        b = ["a", 3.5]
        result = setdiff(a, b)
        assert set(result) == {1, (2, 3)}


class TestFlattenIterable:
    """Test class for the `flatten_iterable` function."""

    def test_flatten_nested_list(self):
        """Test method."""
        iterable = [1, [2, 3], [4, [5, 6]]]
        result = flatten_iterable(iterable)
        assert result == [1, 2, 3, 4, [5, 6]]

    def test_flatten_nested_tuple(self):
        """Test method."""
        iterable = (1, (2, 3), (4, (5, 6)))
        result = flatten_iterable(iterable, types_to_flatten=(tuple,))
        assert result == [1, 2, 3, 4, (5, 6)]

    def test_flatten_mixed_types(self):
        """Test method."""
        iterable = [1, (2, 3), [4, 5]]
        result = flatten_iterable(iterable, types_to_flatten=(list, tuple))
        assert result == [1, 2, 3, 4, 5]

    def test_flatten_no_types_to_flatten(self):
        """Test method."""
        iterable = [1, [2, 3], (4, 5)]
        result = flatten_iterable(iterable, types_to_flatten=())
        assert result == [1, [2, 3], (4, 5)]

    def test_flatten_empty_iterable(self):
        """Test method."""
        iterable = []
        result = flatten_iterable(iterable)
        assert result == []

    def test_flatten_flat_iterable(self):
        """Test method."""
        iterable = [1, 2, 3]
        result = flatten_iterable(iterable)
        assert result == [1, 2, 3]

    def test_flatten_with_strings(self):
        """Test method."""
        iterable = ["abc", [1, 2], (3, 4)]
        result = flatten_iterable(iterable, types_to_flatten=str)
        assert result == ["a", "b", "c", [1, 2], (3, 4)]

    def test_flatten_invalid_iterable(self):
        """Test method."""
        with pytest.raises(TypeError):
            flatten_iterable(123)

    def test_flatten_invalid_types_to_flatten(self):
        """Test method."""
        with pytest.raises(TypeError):
            flatten_iterable([1, [2, 3]], types_to_flatten=123)

    def test_flatten_non_type_in_tuple(self):
        """Test method."""
        with pytest.raises(ValueError):
            flatten_iterable([1, [2, 3]], types_to_flatten=(list, 123))

    def test_flatten_generator_input(self):
        """Test method."""
        iterable = (i for i in [[1, 2], [3, 4]])
        result = flatten_iterable(iterable)
        assert result == [1, 2, 3, 4]


class TestConvertTypesIterable:
    """Test class for the `convert_types_iterable` function."""

    def test_convert_to_float(self):
        """Test method."""
        input_data = [1, 2, 3]
        result = convert_types_iterable(input_data)
        assert result == [1.0, 2.0, 3.0]

    def test_convert_to_string(self):
        """Test method."""
        input_data = (10, 20, 30)
        result = convert_types_iterable(input_data, dtype=str)
        assert result == ["10", "20", "30"]

    def test_convert_to_int(self):
        """Test method."""
        input_data = ["10", "20", "30"]
        result = convert_types_iterable(input_data, dtype=int)
        assert result == [10, 20, 30]

    def test_with_range(self):
        """Test method."""
        input_data = range(5)
        result = convert_types_iterable(input_data, dtype=str)
        assert result == ["0", "1", "2", "3", "4"]

    def test_empty_input(self):
        """Test method."""
        input_data = []
        result = convert_types_iterable(input_data)
        assert result == []

    def test_invalid_iterable(self):
        """Test method."""
        with pytest.raises(TypeError):
            convert_types_iterable(123)

    def test_invalid_dtype(self):
        """Test method."""
        with pytest.raises(TypeError):
            convert_types_iterable([1, 2, 3], dtype=123)

    def test_non_type_in_tuple(self):
        """Test method."""
        with pytest.raises(TypeError):
            convert_types_iterable([1, 2, 3], dtype=(int, 123))

    def test_mixed_types(self):
        """Test method."""
        input_data = [1.1, "2", 3]
        result = convert_types_iterable(input_data, dtype=int)
        assert result == [1, 2, 3]

    def test_nested_iterables(self):
        """Test method."""
        input_data = [[1, 2], [3, 4]]
        with pytest.raises(TypeError):
            convert_types_iterable(input_data)


class TestInterleaveIterables:
    """Test class for the `interleave_iterables` function."""

    def test_interleave_lists(self):
        """Test method."""
        iterable1 = [1, 2, 3]
        iterable2 = [4, 5, 6]
        result = interleave_iterables(iterable1, iterable2)
        assert result == [1, 4, 2, 5, 3, 6]

    def test_interleave_tuples(self):
        """Test method."""
        iterable1 = (1, 2, 3)
        iterable2 = ("a", "b", "c")
        result = interleave_iterables(iterable1, iterable2)
        assert result == [1, "a", 2, "b", 3, "c"]

    def test_interleave_strings(self):
        """Test method."""
        iterable1 = "ABC"
        iterable2 = "123"
        result = interleave_iterables(iterable1, iterable2)
        assert result == ["A", "1", "B", "2", "C", "3"]

    def test_interleave_ranges(self):
        """Test method."""
        iterable1 = range(3)
        iterable2 = range(10, 13)
        result = interleave_iterables(iterable1, iterable2)
        assert result == [0, 10, 1, 11, 2, 12]

    def test_empty_iterables(self):
        """Test method."""
        iterable1 = []
        iterable2 = []
        result = interleave_iterables(iterable1, iterable2)
        assert result == []

    def test_different_lengths(self):
        """Test method."""
        iterable1 = [1, 2]
        iterable2 = [3]
        with pytest.raises(ValueError):
            interleave_iterables(iterable1, iterable2)

    def test_invalid_iterable1(self):
        """Test method."""
        iterable1 = 123
        iterable2 = [1, 2, 3]
        with pytest.raises(TypeError):
            interleave_iterables(iterable1, iterable2)

    def test_invalid_iterable2(self):
        """Test method."""
        iterable1 = [1, 2, 3]
        iterable2 = 456
        with pytest.raises(TypeError):
            interleave_iterables(iterable1, iterable2)

    def test_mixed_iterable_types(self):
        """Test method."""
        iterable1 = [1, 2, 3]
        iterable2 = ("a", "b", "c")
        result = interleave_iterables(iterable1, iterable2)
        assert result == [1, "a", 2, "b", 3, "c"]

    def test_interleave_nested_iterables(self):
        """Test method."""
        iterable1 = [[1], [2], [3]]
        iterable2 = [[4], [5], [6]]
        result = interleave_iterables(iterable1, iterable2)
        assert result == [[1], [4], [2], [5], [3], [6]]


class TestPairwiseIterable:
    """Test class for the pairwise_iterable function."""

    def test_pairwise_list(self):
        """Test pairwise function with a list."""
        result = list(pairwise_iterable([1, 2, 3, 4]))
        expected = [(1, 2), (2, 3), (3, 4)]
        assert result == expected

    def test_pairwise_string(self):
        """Test pairwise function with a string."""
        result = list(pairwise_iterable("abcde"))
        expected = [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")]
        assert result == expected

    def test_pairwise_tuple(self):
        """Test pairwise function with a tuple."""
        result = list(pairwise_iterable((10, 20, 30)))
        expected = [(10, 20), (20, 30)]
        assert result == expected

    def test_pairwise_empty(self):
        """Test pairwise function with an empty iterable."""
        result = list(pairwise_iterable([]))
        expected = []
        assert result == expected

    def test_pairwise_single_element(self):
        """Test pairwise function with a single element."""
        result = list(pairwise_iterable([1]))
        expected = []
        assert result == expected

    def test_pairwise_non_iterable(self):
        """Test pairwise function with a non-iterable input."""
        with pytest.raises(TypeError, match="Input must be an iterable."):
            pairwise_iterable(1)

    def test_pairwise_custom_iterable(self):
        """Test pairwise function with a custom iterable (e.g., a generator)."""

        def my_gen():
            """Test method."""
            yield 5
            yield 10
            yield 15

        result = list(pairwise_iterable(my_gen()))
        expected = [(5, 10), (10, 15)]
        assert result == expected


class TestMergeMultiDfs:
    """Test class for the merge_multi_dfs function."""

    def test_merge_multi_inner(self):
        """Test merge_multi with 'inner' merge."""
        df1 = pd.DataFrame({"key": ["A", "B", "C"], "value1": [1, 2, 3]})
        df2 = pd.DataFrame({"key": ["A", "B"], "value2": [4, 5]})
        df3 = pd.DataFrame({"key": ["A"], "value3": [6]})

        result = merge_multi_dfs([df1, df2, df3], on="key", how="inner")
        expected = pd.DataFrame(
            {"key": ["A"], "value1": [1], "value2": [4], "value3": [6]},
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_merge_multi_outer(self):
        """Test merge_multi with 'outer' merge."""
        df1 = pd.DataFrame({"key": ["A", "B", "C"], "value1": [1, 2, 3]})
        df2 = pd.DataFrame({"key": ["A", "B"], "value2": [4, 5]})

        result = merge_multi_dfs([df1, df2], on="key", how="outer", fillna_val=0)
        expected = pd.DataFrame(
            {"key": ["A", "B", "C"], "value1": [1, 2, 3], "value2": [4, 5, 0]},
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    def test_merge_multi_left(self):
        """Test merge_multi with 'left' merge."""
        df1 = pd.DataFrame({"key": ["A", "B", "C"], "value1": [1, 2, 3]})
        df2 = pd.DataFrame({"key": ["A", "B"], "value2": [4, 5]})

        result = merge_multi_dfs([df1, df2], on="key", how="left")
        expected = pd.DataFrame(
            {"key": ["A", "B", "C"], "value1": [1, 2, 3], "value2": [4, 5, None]},
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_merge_multi_right(self):
        """Test merge_multi with 'right' merge."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})
        df2 = pd.DataFrame({"key": ["A", "B", "C"], "value2": [4, 5, 6]})

        result = merge_multi_dfs([df1, df2], on="key", how="right")
        expected = pd.DataFrame(
            {"key": ["A", "B", "C"], "value1": [1, 2, None], "value2": [4, 5, 6]},
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_merge_multi_fillna(self):
        """Test merge_multi with filling missing values."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})
        df2 = pd.DataFrame({"key": ["A", "C"], "value2": [4, 5]})

        result = merge_multi_dfs([df1, df2], on="key", how="outer", fillna_val=0)
        expected = pd.DataFrame(
            {"key": ["A", "B", "C"], "value1": [1, 2, 0], "value2": [4, 0, 5]},
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    def test_merge_multi_invalid_how(self):
        """Test merge_multi with an invalid 'how' value."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})
        df2 = pd.DataFrame({"key": ["A", "B"], "value2": [4, 5]})

        with pytest.raises(ValueError, match="`how` Must be one of"):
            merge_multi_dfs([df1, df2], on="key", how="invalid_method")

    def test_merge_multi_non_iterable_df_list(self):
        """Test merge_multi with a non-iterable `df_list`."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})

        with pytest.raises(
            TypeError,
            match="`df_list` must be a list of pandas DataFrames.",
        ):
            merge_multi_dfs(df1, on="key", how="inner")

    def test_merge_multi_non_dataframe_in_list(self):
        """Test merge_multi with a non-DataFrame element in `df_list`."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})

        with pytest.raises(
            TypeError,
            match="`df_list` must be a list of pandas DataFrames.",
        ):
            merge_multi_dfs([df1, "not_a_dataframe"], on="key", how="inner")

    def test_merge_multi_invalid_on(self):
        """Test merge_multi with invalid 'on' parameter."""
        df1 = pd.DataFrame({"key": ["A", "B"], "value1": [1, 2]})
        df2 = pd.DataFrame({"key": ["A", "B"], "value2": [4, 5]})

        with pytest.raises(
            TypeError,
            match="`on` must be a string or a list of strings.",
        ):
            merge_multi_dfs([df1, df2], on=123, how="inner")


class TestFileSize:
    """Tests for file_size function."""

    def test_expected(self, tmp_path):
        """Test expected functionality."""
        # Create a temporary file
        temp_file = tmp_path / "test_file.txt"
        content = "This is a test file."
        temp_file.write_text(content)

        # Get the file size
        actual = file_size(str(temp_file))

        # Assert the file size matches the content length
        assert actual == len(content)

    def test_file_not_found(self):
        """Test behavior when file does not exist."""
        with pytest.raises(FileNotFoundError):
            file_size("non_existent_file.txt")


class TestMd5Sum:
    """Tests for md5_sum function."""

    def test_expected(self, tmp_path):
        """Test expected functionality."""
        # Create a temporary file
        temp_file = tmp_path / "test_file.txt"
        content = "This is a test file."
        temp_file.write_text(content)

        # Calculate the expected md5 sum
        expected_md5 = hashlib.md5(content.encode()).hexdigest()

        # Get the actual md5 sum
        actual_md5 = md5_sum(str(temp_file))

        # Assert the md5 sums match
        assert actual_md5 == expected_md5

    def test_file_not_found(self):
        """Test behavior when file does not exist."""
        with pytest.raises(FileNotFoundError):
            md5_sum("non_existent_file.txt")


class TestSha256Sum:
    """Tests for sha256_sum function."""

    def test_expected(self, tmp_path):
        """Test expected functionality."""
        # Create a temporary file
        temp_file = tmp_path / "test_file.txt"
        content = "This is a test file."
        temp_file.write_text(content)

        # Calculate the expected sha256 sum
        expected_sha256 = hashlib.sha256(content.encode()).hexdigest()

        # Get the actual sha256 sum
        actual_sha256 = sha256_sum(str(temp_file))

        # Assert the sha256 sums match
        assert actual_sha256 == expected_sha256

    def test_file_not_found(self):
        """Test behavior when file does not exist."""
        with pytest.raises(FileNotFoundError):
            sha256_sum("non_existent_file.txt")


class TestFileExists:
    """Tests for file_exists function."""

    def test_file_exists(self, tmp_path):
        """Test when the file exists."""
        temp_file = tmp_path / "test_file.txt"
        temp_file.write_text("This is a test file.")
        assert file_exists(str(temp_file)) is True

    def test_file_does_not_exist(self):
        """Test when the file does not exist."""
        assert file_exists("non_existent_file.txt") is False

    def test_directory_instead_of_file(self, tmp_path):
        """Test when the path is a directory."""
        assert file_exists(str(tmp_path)) is False


class TestDirectoryExists:
    """Tests for directory_exists function."""

    def test_is_directory(self, tmp_path):
        """Test when the path is a directory."""
        assert directory_exists(str(tmp_path)) is True

    def test_is_not_directory(self, tmp_path):
        """Test when the path is not a directory."""
        temp_file = tmp_path / "test_file.txt"
        temp_file.write_text("This is a test file.")
        assert directory_exists(str(temp_file)) is False

    def test_non_existent_path(self):
        """Test when the path does not exist."""
        assert directory_exists("non_existent_path") is False


class TestCheckFile:
    """Tests for the check_file function."""

    def test_file_exists(self, tmp_path):
        """Test when the file exists."""
        temp_file = tmp_path / "test_file.txt"
        temp_file.write_text("This is a test file.")
        assert check_file(str(temp_file)) is True

    def test_is_directory(self, tmp_path):
        """Test when the path is a directory."""
        assert check_file(str(tmp_path)) is False

    def test_size_less_0(self, tmp_path):
        """Test when the path an empty file."""
        temp_file = tmp_path / "test_file.txt"
        temp_file.write_text("")
        assert check_file(str(temp_file)) is False


class TestReadHeader:
    """Tests for the read_header function."""

    def test_read_header_expected(self, tmp_path):
        """Test reading the first line of a file."""
        # Create a temporary file
        temp_file = tmp_path / "test_file.txt"
        content = "First line\nSecond line\nThird line"
        temp_file.write_text(content)

        # Read the header
        actual = read_header(str(temp_file))

        # Assert the header matches the first line
        assert actual == "First line"

    def test_read_header_empty_file(self, tmp_path):
        """Test reading the header of an empty file."""
        # Create an empty temporary file
        temp_file = tmp_path / "empty_file.txt"
        temp_file.write_text("")

        # Read the header
        actual = read_header(str(temp_file))

        # Assert the header is an empty string
        assert actual == ""

    def test_read_header_file_not_found(self):
        """Test behavior when the file does not exist."""
        with pytest.raises(FileNotFoundError):
            read_header("non_existent_file.txt")


class TestWriteStringToFile:
    """Tests for the write_string_to_file function."""

    def test_write_string_to_file_expected(self, tmp_path):
        """Test writing content to a file."""
        # Create a temporary file path
        temp_file = tmp_path / "test_file.txt"
        content = b"This is a test file."

        # Write the content to the file
        write_string_to_file(content, str(temp_file))

        # Read the file and verify the content
        with open(temp_file, "rb") as f:
            actual_content = f.read()

        assert actual_content == content

    def test_overwrite_existing_file(self, tmp_path):
        """Test overwriting an existing file."""
        # Create a temporary file and write initial content
        temp_file = tmp_path / "test_file.txt"
        initial_content = b"Initial content."
        temp_file.write_bytes(initial_content)

        # New content to overwrite the file
        new_content = b"New content."
        write_string_to_file(new_content, str(temp_file))

        # Read the file and verify the new content
        with open(temp_file, "rb") as f:
            actual_content = f.read()

        assert actual_content == new_content

    def test_empty_content(self, tmp_path):
        """Test writing empty content to a file."""
        # Create a temporary file path
        temp_file = tmp_path / "test_file.txt"
        content = b""

        # Write the empty content to the file
        write_string_to_file(content, str(temp_file))

        # Read the file and verify the content is empty
        with open(temp_file, "rb") as f:
            actual_content = f.read()

        assert actual_content == content

    def test_invalid_path(self):
        """Test behavior when the file path is invalid."""
        invalid_path = "/invalid_path/test_file.txt"
        content = b"Test content."

        with pytest.raises(OSError):
            write_string_to_file(content, invalid_path)


class TestCreateFolder:
    """Tests for the create_folder function."""

    def test_create_new_folder(self, tmp_path):
        """Test creating a new folder."""
        new_folder = tmp_path / "new_folder"
        create_folder(str(new_folder))
        assert new_folder.exists()
        assert new_folder.is_dir()

    def test_create_existing_folder(self, tmp_path):
        """Test creating a folder that already exists."""
        existing_folder = tmp_path / "existing_folder"
        existing_folder.mkdir()
        create_folder(str(existing_folder))
        assert existing_folder.exists()
        assert existing_folder.is_dir()

    def test_create_nested_folders(self, tmp_path):
        """Test creating nested folders."""
        nested_folder = tmp_path / "parent_folder" / "child_folder"
        create_folder(str(nested_folder))
        assert nested_folder.exists()
        assert nested_folder.is_dir()

    def test_invalid_path(self):
        """Test behavior when the path is invalid."""
        invalid_path = "/invalid_path/new_folder"
        with pytest.raises(OSError):
            create_folder(invalid_path)


class TestDumpEnvironmentRequirements:
    """Tests for the dump_environment_requirements function."""

    def test_writes_expected_output_to_file(self, tmp_path: Path) -> None:
        """Write output to file when tool runs successfully."""
        mock_output = "package-a==1.0.0\npackage-b==2.0.0"
        output_file = tmp_path / "requirements.txt"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout=mock_output)
            dump_environment_requirements(str(output_file))

        assert output_file.exists()
        assert output_file.read_text() == mock_output

    def test_calls_tool_with_custom_args(self, tmp_path: Path) -> None:
        """Call specified tool with provided arguments."""
        output_file = tmp_path / "reqs.txt"
        tool = "poetry"
        args = ["export", "--without-hashes"]

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="exported output")
            dump_environment_requirements(str(output_file), tool=tool, args=args)

            mock_run.assert_called_once_with(
                [tool] + args,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

    def test_creates_nested_output_directory(self, tmp_path: Path) -> None:
        """Create parent directories if they do not exist."""
        nested_dir = tmp_path / "nested" / "dir"
        output_file = nested_dir / "reqs.txt"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="some content")
            dump_environment_requirements(str(output_file))

        assert output_file.exists()
        assert output_file.read_text() == "some content"

    def test_raises_on_subprocess_failure(self, tmp_path: Path) -> None:
        """Raise CalledProcessError if tool execution fails."""
        output_file = tmp_path / "fail.txt"

        with patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "cmd"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                dump_environment_requirements(str(output_file))


class TestParsePyprojectMetadata:
    """Tests for parse_pyproject_metadata function."""

    def test_parses_expected_fields(self, tmp_path):
        """Parses expected fields."""
        py = tmp_path / "pyproject.toml"
        py.write_text(
            "[project]\nname = 'my-package'\nrequires-python = '>=3.10'\nversion = '1.2.3'\n",
            encoding="utf-8",
        )

        meta = parse_pyproject_metadata(py)

        assert meta["name"] == "my-package"
        assert meta["requires_python"] == ">=3.10"
        assert meta["package_version"] == "1.2.3"

    def test_missing_project_table_returns_none_fields(self, tmp_path):
        """Returns None fields when [project] missing."""
        py = tmp_path / "pyproject.toml"
        py.write_text("# no project table here\n", encoding="utf-8")

        meta = parse_pyproject_metadata(py)

        assert meta["name"] is None
        assert meta["requires_python"] is None
        assert meta["package_version"] is None

    def test_file_not_found_raises(self, tmp_path):
        """Raises FileNotFoundError for missing file."""
        missing = tmp_path / "does_not_exist.toml"
        with pytest.raises(FileNotFoundError) as excinfo:
            parse_pyproject_metadata(missing)
        assert "cannot be found" in str(excinfo.value)

    def test_invalid_toml_raises(self, tmp_path):
        """Raises TOMLDecodeError for invalid TOML."""
        py = tmp_path / "pyproject.toml"
        py.write_text("[project\nname = 'oops'\n", encoding="utf-8")  # broken TOML

        with pytest.raises(tomli.TOMLDecodeError):
            parse_pyproject_metadata(py)
