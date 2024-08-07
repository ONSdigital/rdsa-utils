"""Tests for spark_helpers module."""

from unittest.mock import MagicMock

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import types as T

from rdsa_utils.helpers.pyspark import *
from rdsa_utils.helpers.pyspark import _convert_to_spark_col
from rdsa_utils.test_utils import *
from tests.conftest import (
    Case,
    create_dataframe,
    parametrize_cases,
    to_date,
    to_datetime,
)


@to_spark_col
def input_funct(s1: str):
    """Spark col function to use as test input."""
    return s1


@to_spark_col(exclude="s1")
def input_funct_with_exclude(s1: str):
    """Spark col function with exclude parameter to use as test input."""
    return s1


class TestSetDfColumnsNullable:
    """Tests for the set_df_columns_nullable function."""

    def test_expected(self, create_spark_df):
        """Test expected functionality."""
        input_schema = T.StructType(
            [
                T.StructField("code", T.StringType(), True),
                T.StructField("shop", T.StringType(), True),
                T.StructField("collection_date", T.DateType(), True),
                T.StructField("values", T.IntegerType(), False),
            ],
        )
        input_df = create_spark_df(
            [
                (input_schema),
                ("banana", "shop_1", to_datetime("2022-11-01"), 20),
                ("banana", "shop_1", to_datetime("2022-11-08"), 21),
                ("oranges", "shop_1", to_datetime("2022-12-01"), 22),
                ("oranges", "shop_1", to_datetime("2022-12-08"), 23),
            ],
        )

        column_list = ["code", "shop", "collection_date"]
        nullable = False
        actual = set_df_columns_nullable(
            df=input_df,
            column_list=column_list,
            nullable=nullable,
        )

        output_schema = T.StructType(
            [
                T.StructField("code", T.StringType(), False),
                T.StructField("shop", T.StringType(), False),
                T.StructField("collection_date", T.DateType(), False),
                T.StructField("values", T.IntegerType(), False),
            ],
        )
        expected = create_spark_df(
            [
                (output_schema),
                ("banana", "shop_1", to_datetime("2022-11-01"), 20),
                ("banana", "shop_1", to_datetime("2022-11-08"), 21),
                ("oranges", "shop_1", to_datetime("2022-12-01"), 22),
                ("oranges", "shop_1", to_datetime("2022-12-08"), 23),
            ],
        )

        assert_df_equality(actual, expected)


class TestMelt:
    """Tests for melt function."""

    @parametrize_cases(
        Case(
            label="id_vars=[col1]_value_vars=[col2, col3]",
            id_vars=["col1"],
            value_vars=["col2", "col3"],
            expected=create_dataframe(
                [
                    ("col1", "variable", "value"),
                    (1, "col2", 2),
                    (1, "col3", 3),
                    (5, "col2", 6),
                    (5, "col3", 7),
                    (9, "col2", 10),
                    (9, "col3", 11),
                ],
            ),
        ),
        Case(
            label="id_vars=[col1, col2]_value_vars=[col3, col4]",
            id_vars=["col1", "col2"],
            value_vars=["col3", "col4"],
            expected=create_dataframe(
                [
                    ("col1", "col2", "variable", "value"),
                    (1, 2, "col3", 3),
                    (1, 2, "col4", 4),
                    (5, 6, "col3", 7),
                    (5, 6, "col4", 8),
                    (9, 10, "col3", 11),
                    (9, 10, "col4", 12),
                ],
            ),
        ),
    )
    def test_expected(self, to_spark, id_vars, value_vars, expected):
        """Test expected functionality."""
        input_data = to_spark(
            [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]],
            ["col1", "col2", "col3", "col4"],
        )
        actual = melt(df=input_data, id_vars=id_vars, value_vars=value_vars)
        assert_df_equality(actual, to_spark(expected), ignore_nullable=True)


class TestToSparkCol:
    """Test the decorator func to_spark_col and its helper _convert_to_spark_col."""

    def test_string_positive_case(self, spark_session):
        """Test string input converts as expected."""
        string_input = "i_am_string_therefore_i_am_?1234!"
        assert isinstance(_convert_to_spark_col(string_input), SparkCol)

    def test_funct_positive_case(self):
        """Test function input converts as expected."""
        funct_input = input_funct("cheese")
        assert isinstance(_convert_to_spark_col(funct_input), SparkCol)

    def test_funct_negative_case(self):
        """Test function input with exclude parameters does not convert."""
        assert isinstance(input_funct_with_exclude("cheese"), str)

    @parametrize_cases(
        Case(
            label="null",
            func_input=None,
        ),
        Case(
            label="number",
            func_input=67,
        ),
        Case(
            label="bool",
            func_input=True,
        ),
        Case(
            label="decimal",
            func_input=7.68574,
        ),
        Case(
            label="list",
            func_input=["car", "van"],
        ),
        Case(
            label="tuple",
            func_input=(10, "green", "bottles"),
        ),
        Case(
            label="dict",
            func_input={"ace": "spades", "queen": "hearts"},
        ),
        Case(
            label="float",
            func_input=float("nan"),
        ),
    )
    def test_value_errors_raised(self, func_input):
        """Test value errors raised for convert_to_spark_col."""
        with pytest.raises(ValueError):
            _convert_to_spark_col(func_input)


@pytest.mark.skip(reason="Already tested above as part of TestToSparkCol.")
class TestConvertToSparkCol:
    """Tests for _convert_to_spark_col function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestToList:
    """Tests for to_list function."""

    def test_expected_one_column(self, to_spark):
        """Test expected functionality for one column."""
        input_data = to_spark(["banana", "banana"], "string").toDF("code")
        assert to_list(input_data) == ["banana", "banana"]

    def test_expected_two_columns(self, create_spark_df):
        """Test expected functionality for two columns."""
        input_data = create_spark_df(
            [
                ("code", "values"),
                ("banana", 22),
                ("banana", 23),
            ],
        )
        assert to_list(input_data) == [["banana", 22], ["banana", 23]]


class TestMapColumnNames:
    """Tests for map_column_names function."""

    def test_map_column_names(self, create_spark_df):
        """Test column names are mapped to given values."""
        input_df = create_spark_df(
            [
                ("col_A", "col_B", "col_Y", "col_D", "col_Z"),
                ("aaa", "bbb", "ccc", "ddd", "eee"),
            ],
        )

        actual = map_column_names(
            input_df,
            {"col_Y": "col_C", "col_Z": "col_E"},
        )

        expected = create_spark_df(
            [
                ("col_A", "col_B", "col_C", "col_D", "col_E"),
                ("aaa", "bbb", "ccc", "ddd", "eee"),
            ],
        )

        assert_df_equality(actual, expected)


@pytest.mark.skip(reason="test not required")
class TestTransform:
    """Tests for transform function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestIsDfEmpty:
    """Test whether spark df contains any records."""

    def test_non_empty_df(self, create_spark_df):
        """Test whether spark df contains any records."""
        non_empty_df = create_spark_df(
            [
                ("col_a", "col_b"),
                ("aaa", "bbb"),
            ],
        )

        assert is_df_empty(non_empty_df) is False

    def test_empty_df(self, create_spark_df):
        """Test whether spark df contains any records."""
        empty_df = create_spark_df(
            [
                ("col_a", "col_b"),
                ("aaa", "bbb"),
            ],
        ).filter(
            F.col("col_a") == "bbb",
        )  # Drop rows.

        assert is_df_empty(empty_df) is True


class TestUnpackListCol:
    """Tests for the unpack_list_col function."""

    @parametrize_cases(
        Case(
            label="one_item_in_list",
            input_df=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", ["cheddar"]),
                ]
            ),
            expected=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", "cheddar"),
                ]
            ),
        ),
        Case(
            label="multiple_items_in_list",
            input_df=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", ["cheddar", "brie", "gorgonzola"]),
                ]
            ),
            expected=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", "cheddar"),
                    ("cheese", "brie"),
                    ("cheese", "gorgonzola"),
                ]
            ),
        ),
        Case(
            label="empty_string",
            input_df=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", ["cheddar", ""]),
                ]
            ),
            expected=(
                [
                    ("col_1", "to_unpack"),
                    ("cheese", "cheddar"),
                    ("cheese", ""),
                ]
            ),
        ),
    )
    def test_expected(self, create_spark_df, input_df, expected):
        """Test expected functionality."""
        expected = create_spark_df(expected)

        actual = unpack_list_col(
            create_spark_df(input_df),
            list_col="to_unpack",
            unpacked_col="to_unpack",
        )

        assert_df_equality(actual, expected)


class TestCreateColnameToValueMap:
    """Tests for the create_colname_to_value_map function."""

    def test_collects_columns_to_mapping_when_schemas_are_consistent(
        self,
        create_spark_df,
    ):
        """Collects to a mapping of colname to value for each column in the list."""
        input_df = create_spark_df(
            [
                ("item", "available"),
                ("bacon", "yes"),
                ("toast", "yes"),
                ("egg", "no"),
            ],
        )

        actual = input_df.withColumn(
            "menu",
            create_colname_to_value_map(["item", "available"]),
        )

        expected = create_spark_df(
            [
                ("item", "available", "menu"),
                ("bacon", "yes", {"item": "bacon", "available": "yes"}),
                ("toast", "yes", {"item": "toast", "available": "yes"}),
                ("egg", "no", {"item": "egg", "available": "no"}),
            ],
        )

        assert_df_equality(actual, expected, ignore_nullable=True)

    def test_coerces_number_types_to_string_when_collecting_to_map(
        self,
        create_spark_df,
    ):
        """Coerce to string as MapType requires consistent schema."""
        input_df = create_spark_df(
            [
                ("item", "cost"),
                ("bacon", 2.0),
                ("toast", 0.5),
                ("egg", 1.0),
            ],
        )

        actual = input_df.withColumn(
            "menu",
            create_colname_to_value_map(["item", "cost"]),
        )

        expected = create_spark_df(
            [
                ("item", "cost", "menu"),
                ("bacon", 2.0, {"item": "bacon", "cost": "2.0"}),
                ("toast", 0.5, {"item": "toast", "cost": "0.5"}),
                ("egg", 1.0, {"item": "egg", "cost": "1.0"}),
            ],
        )

        assert_df_equality(actual, expected, ignore_nullable=True)


class TestGetWindowSpec:
    """Tests for get_window_spec function."""

    @parametrize_cases(
        Case(
            label="partition_cols_and_order_cols",
            partition_cols=["code"],
            order_cols=["collection_date"],
            expected=(
                [
                    ("code", "shop", "collection_date", "values", "test"),
                    ("oranges", "shop_1", to_datetime("2022-12-01"), 22, 22),
                    ("oranges", "shop_1", to_datetime("2022-12-08"), 23, 45),
                    ("banana", "shop_1", to_datetime("2022-11-01"), 20, 20),
                    ("banana", "shop_1", to_datetime("2022-11-08"), 21, 41),
                ]
            ),
        ),
        Case(
            label="partition_cols",
            partition_cols=["code"],
            order_cols=None,
            expected=(
                [
                    ("code", "shop", "collection_date", "values", "test"),
                    ("oranges", "shop_1", to_datetime("2022-12-01"), 22, 45),
                    ("oranges", "shop_1", to_datetime("2022-12-08"), 23, 45),
                    ("banana", "shop_1", to_datetime("2022-11-01"), 20, 41),
                    ("banana", "shop_1", to_datetime("2022-11-08"), 21, 41),
                ]
            ),
        ),
        Case(
            label="order_cols",
            partition_cols=None,
            order_cols=["collection_date"],
            expected=(
                [
                    ("code", "shop", "collection_date", "values", "test"),
                    ("banana", "shop_1", to_datetime("2022-11-01"), 20, 20),
                    ("banana", "shop_1", to_datetime("2022-11-08"), 21, 41),
                    ("oranges", "shop_1", to_datetime("2022-12-01"), 22, 63),
                    ("oranges", "shop_1", to_datetime("2022-12-08"), 23, 86),
                ]
            ),
        ),
        Case(
            label="no_1rgs",
            partition_cols=None,
            order_cols=None,
            expected=(
                [
                    ("code", "shop", "collection_date", "values", "test"),
                    ("oranges", "shop_1", to_datetime("2022-12-01"), 22, 86),
                    ("oranges", "shop_1", to_datetime("2022-12-08"), 23, 86),
                    ("banana", "shop_1", to_datetime("2022-11-01"), 20, 86),
                    ("banana", "shop_1", to_datetime("2022-11-08"), 21, 86),
                ]
            ),
        ),
    )
    def test_expected(self, create_spark_df, expected, partition_cols, order_cols):
        """Test expected functionality."""
        input_data = create_spark_df(
            [
                ("code", "shop", "collection_date", "values"),
                ("banana", "shop_1", to_datetime("2022-11-01"), 20),
                ("banana", "shop_1", to_datetime("2022-11-08"), 21),
                ("oranges", "shop_1", to_datetime("2022-12-01"), 22),
                ("oranges", "shop_1", to_datetime("2022-12-08"), 23),
            ],
        )
        window_spec = get_window_spec(
            partition_cols=partition_cols,
            order_cols=order_cols,
        )
        assert_df_equality(
            input_data.withColumn("test", F.sum("values").over(window_spec)),
            create_spark_df(expected),
            ignore_row_order=True,
        )


class TestRankNumeric:
    """Tests for the rank_numeric function."""

    @parametrize_cases(
        Case(
            label="rank_desc",
            input_df=[
                ("group", "area", "expenditure"),
                ("a", "b", 60),
                ("a", "b", 36),
                ("a", "c", 11),
            ],
            expected=[
                ("group", "area", "expenditure", "rank"),
                ("a", "b", 60, 1),
                ("a", "b", 36, 2),
                ("a", "c", 11, 1),
            ],
            group=["group", "area"],
            ascending=False,
        ),
        Case(
            label="rank_ascend",
            input_df=[
                ("group", "area", "expenditure"),
                ("a", "b", 60),
                ("a", "b", 36),
                ("a", "c", 11),
            ],
            expected=[
                ("group", "area", "expenditure", "rank"),
                ("a", "b", 60, 2),
                ("a", "b", 36, 1),
                ("a", "c", 11, 1),
            ],
            group=["group", "area"],
            ascending=True,
        ),
        Case(
            label="rank_desc_same_expenditure",
            input_df=[
                ("group", "area", "expenditure"),
                ("a", "b", 60),
                ("a", "c", 60),
            ],
            expected=[
                ("group", "area", "expenditure", "rank"),
                ("a", "b", 60, 1),
                ("a", "c", 60, 2),
            ],
            group="group",
            ascending=False,
        ),
        Case(
            label="rank_ascend_same_expenditure",
            input_df=[
                ("group", "area", "expenditure"),
                ("a", "b", 60),
                ("a", "c", 60),
            ],
            expected=[
                ("group", "area", "expenditure", "rank"),
                ("a", "b", 60, 1),
                ("a", "c", 60, 2),
            ],
            group="group",
            ascending=True,
        ),
    )
    def test_expected(self, create_spark_df, input_df, expected, group, ascending):
        """Test the numeric column ranked as expected."""
        input_df = create_spark_df(input_df)
        expected = create_spark_df(expected)

        actual = input_df.withColumn(
            "rank",
            rank_numeric("expenditure", group, ascending),
        )

        assert_df_equality(
            actual,
            expected.withColumn("rank", F.col("rank").astype("int")),
            ignore_row_order=True,
            ignore_nullable=True,
        )

    def test_value_errors_raised(self):
        """Test value errors raised for rank_numeric."""
        with pytest.raises(ValueError):
            rank_numeric(["expenditure"], "group", False)


class TestCalcMedianPrice:
    """Tests for calc_median_price function."""

    def test_calc_median_price(self, create_spark_df):
        """Test the median is calculated per grouping level."""
        groups = ["group", "other_group"]

        input_df = create_spark_df(
            (
                [
                    ("period", "group", "other_group", "price"),
                    (to_date("2021-01-01"), "group_1", "other_group_1", 1.0),
                    (to_date("2021-01-07"), "group_1", "other_group_1", 1.0),
                    (to_date("2021-01-01"), "group_2", "other_group_2", 5.0),
                    (to_date("2021-01-07"), "group_2", "other_group_2", 5.0),
                    (to_date("2021-01-14"), "group_2", "other_group_2", 5.1),
                    (to_date("2021-01-01"), "group_3", "other_group_3", 2.3),
                ]
            ),
        )

        expected = create_spark_df(
            (
                [
                    ("period", "group", "other_group", "price", "median"),
                    (to_date("2021-01-01"), "group_1", "other_group_1", 1.0, 1.0),
                    (to_date("2021-01-07"), "group_1", "other_group_1", 1.0, 1.0),
                    (to_date("2021-01-01"), "group_2", "other_group_2", 5.0, 5.0),
                    (to_date("2021-01-07"), "group_2", "other_group_2", 5.0, 5.0),
                    (to_date("2021-01-14"), "group_2", "other_group_2", 5.1, 5.0),
                    (to_date("2021-01-01"), "group_3", "other_group_3", 2.3, 2.3),
                ]
            ),
        )

        actual = input_df.withColumn("median", calc_median_price(groups, "price"))

        assert_df_equality(actual, expected, ignore_row_order=True)


class TestConvertColsToStructCol:
    """Tests for the convert_cols_to_struct_col function.

    Note, to define the schema for struct type columns in a dataframe we need
    to explicitly define the schema fully which results in a slightly more
    unusual definition of the expected dataframe in these tests.
    """

    @pytest.fixture()
    def input_df_fixture(self, create_spark_df) -> SparkDF:
        """Provide a basic spark dataframe."""
        return create_spark_df(
            [
                ("column_a", "column_b", "column_c"),
                ("AA1", "BB1", "CC1"),
                ("AA2", "BB2", "CC2"),
            ],
        )

    @parametrize_cases(
        Case(
            label="convert_single_column",
            input_df=pytest.lazy_fixture("input_df_fixture"),
            struct_cols=["column_c"],
            struct_col_name="struct_column",
            expected_schema=T.StructType(
                [
                    T.StructField("column_a", T.StringType(), True),
                    T.StructField("column_b", T.StringType(), True),
                    T.StructField(
                        "struct_column",
                        T.StructType(
                            [
                                T.StructField("column_c", T.StringType(), True),
                            ],
                        ),
                        True,
                    ),
                ],
            ),
            expected_data=(
                ("AA1", "BB1", ("CC1",)),
                ("AA2", "BB2", ("CC2",)),
            ),
        ),
        Case(
            label="convert_multiple_columns",
            input_df=pytest.lazy_fixture("input_df_fixture"),
            struct_cols=["column_b", "column_c"],
            struct_col_name="struct_column",
            expected_schema=T.StructType(
                [
                    T.StructField("column_a", T.StringType(), True),
                    T.StructField(
                        "struct_column",
                        T.StructType(
                            [
                                T.StructField("column_b", T.StringType(), True),
                                T.StructField("column_c", T.StringType(), True),
                            ],
                        ),
                        True,
                    ),
                ],
            ),
            expected_data=(
                ("AA1", ("BB1", "CC1")),
                ("AA2", ("BB2", "CC2")),
            ),
        ),
    )
    def test_expected_with_struct_cols(
        self,
        create_spark_df,
        input_df,
        struct_cols,
        struct_col_name,
        expected_schema,
        expected_data,
    ):
        """Test expected functionality when creating struct col from existing columns."""
        expected = create_spark_df([(expected_schema), *expected_data])
        result = convert_cols_to_struct_col(
            df=input_df,
            struct_cols=struct_cols,
            struct_col_name=struct_col_name,
        )
        assert_df_equality(
            result,
            expected,
            ignore_column_order=True,
            ignore_nullable=True,
        )

    @parametrize_cases(
        Case(
            label="default_no_struct_col_args",
            input_df=pytest.lazy_fixture("input_df_fixture"),
            struct_cols=None,
            struct_col_name="struct_column",
            no_struct_col_type=T.BooleanType(),
            no_struct_col_value=None,
            expected_schema=T.StructType(
                [
                    T.StructField("column_a", T.StringType(), True),
                    T.StructField("column_b", T.StringType(), True),
                    T.StructField("column_c", T.StringType(), True),
                    T.StructField(
                        "struct_column",
                        T.StructType(
                            [
                                T.StructField(
                                    "no_struct_column",
                                    T.BooleanType(),
                                    True,
                                ),
                            ],
                        ),
                        True,
                    ),
                ],
            ),
            expected_data=(
                ("AA1", "BB1", "CC1", (None,)),
                ("AA2", "BB2", "CC2", (None,)),
            ),
        ),
        Case(
            label="type_is_string",
            input_df=pytest.lazy_fixture("input_df_fixture"),
            struct_cols=None,
            struct_col_name="struct_column",
            no_struct_col_type=T.StringType(),
            no_struct_col_value="missing",
            expected_schema=T.StructType(
                [
                    T.StructField("column_a", T.StringType(), True),
                    T.StructField("column_b", T.StringType(), True),
                    T.StructField("column_c", T.StringType(), True),
                    T.StructField(
                        "struct_column",
                        T.StructType(
                            [
                                T.StructField("no_struct_column", T.StringType(), True),
                            ],
                        ),
                        True,
                    ),
                ],
            ),
            expected_data=(
                ("AA1", "BB1", "CC1", ("missing",)),
                ("AA2", "BB2", "CC2", ("missing",)),
            ),
        ),
    )
    def test_expected_no_struct_cols(
        self,
        create_spark_df,
        input_df,
        struct_cols,
        struct_col_name,
        no_struct_col_type,
        no_struct_col_value,
        expected_schema,
        expected_data,
    ):
        """Test expected functionality when no  cols being used to create new struct cols."""
        expected = create_spark_df([(expected_schema), *expected_data])
        result = convert_cols_to_struct_col(
            df=input_df,
            struct_cols=struct_cols,
            struct_col_name=struct_col_name,
            no_struct_col_type=no_struct_col_type,
            no_struct_col_value=no_struct_col_value,
        )
        assert_df_equality(
            result,
            expected,
            ignore_column_order=True,
            ignore_nullable=True,
        )

    def test_raises_value_error(self, input_df_fixture):
        """Test ValueError raised when specified struct_cols are not present in input_df."""
        with pytest.raises(ValueError):
            convert_cols_to_struct_col(
                df=input_df_fixture,
                struct_cols=["column_c", "column_ch"],
                struct_col_name="struct_col",
            )


class TestSelectFirstObsAppearingInGroup:
    """Tests for the select_first_obs_appearing_in_group function."""

    @parametrize_cases(
        Case(
            label="earliest_date",
            ascending=True,
            expected_data=[
                ("group", "week_start_date", "price"),
                ("a", to_datetime("2022-05-20"), 5),
                ("b", to_datetime("2022-04-02"), 1),
            ],
        ),
        Case(
            label="latest_date",
            ascending=False,
            expected_data=[
                ("group", "week_start_date", "price"),
                ("a", to_datetime("2022-05-22"), 7),
                ("b", to_datetime("2022-04-07"), 3),
            ],
        ),
    )
    def test_expected(self, create_spark_df, ascending, expected_data):
        """Test expected outputs."""
        input_df = create_spark_df(
            [
                ("group", "week_start_date", "price"),
                ("a", to_datetime("2022-05-20"), 5),
                ("a", to_datetime("2022-05-21"), 6),
                ("a", to_datetime("2022-05-22"), 7),
                ("b", to_datetime("2022-04-02"), 1),
                ("b", to_datetime("2022-04-06"), 2),
                ("b", to_datetime("2022-04-07"), 3),
            ],
        )
        expected = create_spark_df(expected_data)

        actual = select_first_obs_appearing_in_group(
            df=input_df,
            group=["group"],
            date_col="week_start_date",
            ascending=ascending,
        )

        assert_df_equality(
            actual,
            expected,
        )


class TestConvertStrucColToColumns:
    """Tests for the convert_struc_col_to_columns function."""

    @parametrize_cases(
        Case(
            label="no_struct_type_columns",
            input_df=[
                ("string_col", "num1_col", "num2_col"),
                ("a", 1, 2),
                ("b", 9, 8),
            ],
            expected=[
                ("string_col", "num1_col", "num2_col"),
                ("a", 1, 2),
                ("b", 9, 8),
            ],
        ),
        Case(
            label="one_struct_type_column",
            input_df=[
                ("string_col", "struct_col"),
                ("a", (1, 2)),
                ("b", (9, 8)),
            ],
            expected=[
                ("string_col", "_1", "_2"),
                ("a", 1, 2),
                ("b", 9, 8),
            ],
        ),
        Case(
            label="many_struct_type_columns",
            input_df=[
                ("string_col", "struct1_col", "struct2_col"),
                ("a", (1, 2), (3, 4)),
                ("b", (9, 8), (7, 6)),
            ],
            expected=[
                ("string_col", "_1", "_2", "_1", "_2"),
                ("a", 1, 2, 3, 4),
                ("b", 9, 8, 7, 6),
            ],
        ),
        Case(
            label="nested_struct_type_column",
            input_df=[
                ("string_col", "struct_col"),
                ("a", ((1, 2), (3, 4))),
                ("b", ((9, 8), (7, 6))),
            ],
            expected=[
                ("string_col", "_1", "_2"),
                ("a", (1, 2), (3, 4)),
                ("b", (9, 8), (7, 6)),
            ],
        ),
    )
    def test_method(self, create_spark_df, input_df, expected):
        """Test expected functionality.

        Note it is non-trivial to implement column names within the struct
        being defined, so left out, these then default to `_n` where n is the
        ordered position in the struct for the column.
        """
        actual = convert_struc_col_to_columns(df=create_spark_df(input_df))

        assert_df_equality(actual, create_spark_df(expected))

    def test_convert_nested_structs(self, create_spark_df):
        """Test expected functionality for recursive flattening."""
        actual = convert_struc_col_to_columns(
            df=create_spark_df(
                [
                    ("string_col", "struct_col"),
                    ("a", ((1, 2), (3, 4))),
                    ("b", ((9, 8), (7, 6))),
                ],
            ),
            convert_nested_structs=True,
        )

        assert_df_equality(
            actual,
            create_spark_df(
                [
                    ("string_col", "_1", "_2", "_1", "_2"),
                    ("a", 1, 2, 3, 4),
                    ("b", 9, 8, 7, 6),
                ],
            ),
        )


class TestCutLineage:
    """Tests for cut_lineage function."""

    def test_cut_lineage(self, spark_session: SparkSession) -> None:
        """Test that cut_lineage returns a DataFrame and doesn't raise any
        exceptions during the process.
        """
        # Create a mock DataFrame with all necessary attributes
        df = MagicMock(spec=SparkDF)
        df._jdf = MagicMock()
        df._jdf.toJavaRDD.return_value = MagicMock()
        df._jdf.schema.return_value = MagicMock()
        df.sql_ctx = spark_session
        spark_session._jsqlContext = MagicMock()
        spark_session._jsqlContext.createDataFrame.return_value = MagicMock()
        try:
            new_df = cut_lineage(df)
            assert isinstance(new_df, SparkDF)
        except Exception:
            pytest.fail("cut_lineage raised Exception unexpectedly!")

    def test_cut_lineage_error(self) -> None:
        """Test that cut_lineage raises an exception when an error occurs during
        the lineage cutting process.
        """
        # Create a mock DataFrame with all necessary attributes
        df = MagicMock(spec=SparkDF)
        df._jdf = MagicMock()
        df._jdf.toJavaRDD.side_effect = Exception(
            "An error occurred during the lineage cutting process.",
        )
        with pytest.raises(
            Exception,
            match="An error occurred during the lineage cutting process.",
        ):
            cut_lineage(df)


class TestFindSparkDataFrames:
    """Tests find_spark_dataframes function."""

    def test_find_spark_dataframes(
        self,
        spark_session: SparkSession,
        create_spark_df: Callable,
    ) -> None:
        """Test that find_spark_dataframes correctly identifies DataFrames and
        dictionaries containing DataFrames.
        """
        input_schema = T.StructType(
            [
                T.StructField("name", T.StringType(), True),
                T.StructField("department", T.StringType(), True),
                T.StructField("salary", T.IntegerType(), True),
            ],
        )
        df = create_spark_df(
            [
                (input_schema),
                ("John", "Sales", 20),
                ("Jane", "Marketing", 21),
            ],
        )
        locals_dict = {
            "df": df,
            "not_df": "I'm not a DataFrame",
            "df_dict": {"df1": df, "df2": df},
        }

        result = find_spark_dataframes(locals_dict)

        assert "df" in result
        assert "df_dict" in result
        assert "not_df" not in result

        assert isinstance(result["df"], SparkDF)
        assert isinstance(result["df_dict"], dict)
        assert all(isinstance(val, SparkDF) for val in result["df_dict"].values())


class TestCreateSparkSession:
    """Tests for create_spark_session function."""

    @pytest.mark.parametrize(
        "session_size",
        ["small", "medium", "large", "extra-large"],
    )
    def test_create_spark_session_valid_sizes(self, session_size: str) -> None:
        """Test create_spark_session with valid sizes."""
        spark = create_spark_session(size=session_size)
        assert isinstance(
            spark,
            SparkSession,
        ), "The function should return a SparkSession instance."
        spark.stop()

    @pytest.mark.parametrize("session_size", ["tiny", "huge", "invalid"])
    def test_create_spark_session_invalid_sizes(self, session_size: str) -> None:
        """Test create_spark_session with invalid sizes."""
        with pytest.raises(ValueError):
            create_spark_session(size=session_size)

    def test_create_spark_session_with_extra_configs(
        self,
    ) -> None:
        """Test create_spark_session with extra configurations."""
        extra_configs = {"spark.ui.enabled": "false"}
        spark = create_spark_session(app_name="default", extra_configs=extra_configs)
        assert (
            spark.conf.get("spark.ui.enabled") == "false"
        ), "Extra configurations should be applied."
        spark.stop()


@pytest.fixture(scope="module")
def test_csv(tmpdir_factory):
    """Create a temporary CSV file for testing."""
    data = """col1,col2,col3
    1,A,foo
    2,B,bar
    3,C,baz"""
    csv_file = tmpdir_factory.mktemp("data").join("test.csv")
    with open(csv_file, "w") as f:
        f.write(data)
    return str(csv_file)


class TestLoadCSV:
    """Tests for load_csv function."""

    def test_load_csv_basic(self, spark_session, test_csv):
        """Test loading CSV file."""
        df = load_csv(spark_session, test_csv)
        assert df.count() == 3
        assert len(df.columns) == 3

    def test_load_csv_multiline(self, spark_session, tmpdir_factory):
        """Test loading multiline CSV file."""
        data = """col1,col2,col3
        1,A,"foo
        bar"
        2,B,"baz
        qux" """
        csv_file = tmpdir_factory.mktemp("data").join("multiline_test.csv")
        with open(csv_file, "w") as f:
            f.write(data)

        df = load_csv(spark_session, str(csv_file), multi_line=True)
        assert df.count() == 2
        assert len(df.columns) == 3

    def test_load_csv_keep_columns(self, spark_session, test_csv):
        """Test keeping specific columns."""
        df = load_csv(spark_session, test_csv, keep_columns=["col1", "col2"])
        assert df.count() == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col2" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_drop_columns(self, spark_session, test_csv):
        """Test dropping specific columns."""
        df = load_csv(spark_session, test_csv, drop_columns=["col2"])
        assert df.count() == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col3" in df.columns
        assert "col2" not in df.columns

    def test_load_csv_rename_columns(self, spark_session, test_csv):
        """Test renaming columns."""
        df = load_csv(
            spark_session,
            test_csv,
            rename_columns={"col1": "new_col1", "col3": "new_col3"},
        )
        assert df.count() == 3
        assert len(df.columns) == 3
        assert "new_col1" in df.columns
        assert "col1" not in df.columns
        assert "new_col3" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_missing_keep_column(self, spark_session, test_csv):
        """Test error when keep column is missing."""
        with pytest.raises(ValueError):
            load_csv(spark_session, test_csv, keep_columns=["col4"])

    def test_load_csv_missing_drop_column(self, spark_session, test_csv):
        """Test error when drop column is missing."""
        with pytest.raises(ValueError):
            load_csv(spark_session, test_csv, drop_columns=["col4"])

    def test_load_csv_missing_rename_column(self, spark_session, test_csv):
        """Test error when rename column is missing."""
        with pytest.raises(ValueError):
            load_csv(spark_session, test_csv, rename_columns={"col4": "new_col4"})
