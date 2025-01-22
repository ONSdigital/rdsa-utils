"""Tests for the helpers/python.py module."""

from time import sleep
from unittest import mock

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

