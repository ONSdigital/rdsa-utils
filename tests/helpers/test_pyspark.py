"""Tests for helpers/pyspark.py module."""

from unittest.mock import MagicMock, patch

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import types as T

from rdsa_utils.helpers.pyspark import *
from rdsa_utils.helpers.pyspark import _convert_to_spark_col
from rdsa_utils.test_utils import *


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

    def test_funct_positive_case(self, spark_session):
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

    @pytest.fixture
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


class TestLoadCSV:
    """Tests for load_csv function."""

    data_basic = """col1,col2,col3
1,A,foo
2,B,bar
3,C,baz
"""

    data_multiline = """col1,col2,col3
1,A,"foo
bar"
2,B,"baz
qux"
"""

    @pytest.fixture(scope="class")
    def custom_spark_session(self):
        """Spark session fixture for this test class."""
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("test_load_csv")
            .getOrCreate()
        )
        yield spark
        spark.stop()

    def create_temp_csv(self, tmp_path, data):
        """Create a temporary CSV file."""
        temp_file = tmp_path / "test.csv"
        temp_file.write_text(data)
        return str(temp_file)

    def test_load_csv_basic(self, custom_spark_session, tmp_path):
        """Test loading CSV file."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(custom_spark_session, temp_file)
        assert df.count() == 3
        assert len(df.columns) == 3

    def test_load_csv_multiline(self, custom_spark_session, tmp_path):
        """Test loading multiline CSV file."""
        temp_file = self.create_temp_csv(tmp_path, self.data_multiline)
        df = load_csv(custom_spark_session, temp_file, multiLine=True)
        assert df.count() == 2
        assert len(df.columns) == 3

    def test_load_csv_keep_columns(self, custom_spark_session, tmp_path):
        """Test keeping specific columns."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(custom_spark_session, temp_file, keep_columns=["col1", "col2"])
        assert df.count() == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col2" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_drop_columns(self, custom_spark_session, tmp_path):
        """Test dropping specific columns."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(custom_spark_session, temp_file, drop_columns=["col2"])
        assert df.count() == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col3" in df.columns
        assert "col2" not in df.columns

    def test_load_csv_rename_columns(self, custom_spark_session, tmp_path):
        """Test renaming columns."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(
            custom_spark_session,
            temp_file,
            rename_columns={"col1": "new_col1", "col3": "new_col3"},
        )
        assert df.count() == 3
        assert len(df.columns) == 3
        assert "new_col1" in df.columns
        assert "col1" not in df.columns
        assert "new_col3" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_missing_keep_column(self, custom_spark_session, tmp_path):
        """Test error when keep column is missing."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        with pytest.raises(ValueError):
            load_csv(custom_spark_session, temp_file, keep_columns=["col4"])

    def test_load_csv_missing_drop_column(self, custom_spark_session, tmp_path):
        """Test error when drop column is missing."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        with pytest.raises(ValueError):
            load_csv(custom_spark_session, temp_file, drop_columns=["col4"])

    def test_load_csv_missing_rename_column(self, custom_spark_session, tmp_path):
        """Test error when rename column is missing."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        with pytest.raises(ValueError):
            load_csv(
                custom_spark_session,
                temp_file,
                rename_columns={"col4": "new_col4"},
            )

    def test_load_csv_with_encoding(self, custom_spark_session, tmp_path):
        """Test loading CSV with a specific encoding."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(custom_spark_session, temp_file, encoding="ISO-8859-1")
        assert df.count() == 3
        assert len(df.columns) == 3

    def test_load_csv_with_custom_delimiter(self, custom_spark_session, tmp_path):
        """Test loading CSV with a custom delimiter."""
        data_with_semicolon = """col1;col2;col3
1;A;foo
2;B;bar
3;C;baz
"""
        temp_file = self.create_temp_csv(tmp_path, data_with_semicolon)
        df = load_csv(custom_spark_session, temp_file, sep=";")
        assert df.count() == 3
        assert len(df.columns) == 3

    def test_load_csv_with_infer_schema(self, custom_spark_session, tmp_path):
        """Test loading CSV with schema inference."""
        temp_file = self.create_temp_csv(tmp_path, self.data_basic)
        df = load_csv(custom_spark_session, temp_file, inferSchema=True)
        assert df.schema["col1"].dataType.typeName() == "integer"
        assert df.schema["col2"].dataType.typeName() == "string"
        assert df.schema["col3"].dataType.typeName() == "string"

    def test_load_csv_with_custom_quote(self, custom_spark_session, tmp_path):
        """Test loading CSV with a custom quote character."""
        data_with_custom_quote = """col1,col2,col3
1,A,foo
2,B,'bar'
3,C,'baz'
"""
        temp_file = self.create_temp_csv(tmp_path, data_with_custom_quote)
        df = load_csv(custom_spark_session, temp_file, quote="'")
        assert df.count() == 3
        assert len(df.columns) == 3
        assert df.filter(df.col3 == "bar").count() == 1
        assert df.filter(df.col3 == "baz").count() == 1


class TestTruncateExternalHiveTable:
    """Tests for truncate_external_hive_table function."""

    @pytest.fixture
    def create_external_table(self, spark_session: SparkSession):
        """Create a mock external Hive table for testing."""
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("test_external_table")
            .enableHiveSupport()
            .getOrCreate()
        )
        table_name = "test_db.test_table"
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        schema = T.StructType([T.StructField("name", T.StringType(), True)])
        df = spark.createDataFrame([("Alice",), ("Bob",)], schema)
        df.write.mode("overwrite").saveAsTable(table_name)
        yield table_name, spark
        spark.sql(f"DROP TABLE {table_name}")
        spark.sql("DROP DATABASE test_db")
        spark.stop()

    @pytest.fixture
    def create_partitioned_table(self, spark_session: SparkSession):
        """Create a mock partitioned external Hive table for testing."""
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("test_partitioned_table")
            .enableHiveSupport()
            .getOrCreate()
        )
        table_name = "test_db.test_partitioned_table"
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        schema = T.StructType(
            [
                T.StructField("name", T.StringType(), True),
                T.StructField("year", T.IntegerType(), True),
            ],
        )
        df = spark.createDataFrame([("Alice", 2020), ("Bob", 2021)], schema)
        df.write.mode("overwrite").partitionBy("year").saveAsTable(table_name)
        yield table_name, spark
        spark.sql(f"DROP TABLE {table_name}")
        spark.sql("DROP DATABASE test_db")
        spark.stop()

    def test_truncate_table(self, create_external_table):
        """Test truncating an external Hive table."""
        table_name, spark_session = create_external_table
        truncate_external_hive_table(spark_session, table_name)
        truncated_df = spark_session.table(table_name)
        assert truncated_df.count() == 0

    def test_schema_preservation(self, create_external_table):
        """Test schema preservation after truncation."""
        table_name, spark_session = create_external_table
        original_schema = spark_session.table(table_name).schema
        truncate_external_hive_table(spark_session, table_name)
        truncated_schema = spark_session.table(table_name).schema
        assert original_schema == truncated_schema

    def test_truncate_partitioned_table(self, create_partitioned_table):
        """Test truncating a partitioned external Hive table."""
        table_name, spark_session = create_partitioned_table
        truncate_external_hive_table(spark_session, table_name)
        truncated_df = spark_session.table(table_name)
        assert truncated_df.count() == 0

    def test_truncate_without_database(self, create_external_table):
        """Test truncating a table when only table name is provided."""
        table_name, spark_session = create_external_table

        # Set the current database and pass only the table name
        spark_session.catalog.setCurrentDatabase("test_db")
        truncate_external_hive_table(spark_session, "test_table")
        truncated_df = spark_session.table(table_name)
        assert truncated_df.count() == 0

    def test_partition_preservation(self, create_partitioned_table):
        """Test partition preservation after truncation."""
        table_name, spark_session = create_partitioned_table
        original_partitions = spark_session.sql(
            f"SHOW PARTITIONS {table_name}",
        ).collect()

        truncate_external_hive_table(spark_session, table_name)
        remaining_partitions = spark_session.sql(
            f"SHOW PARTITIONS {table_name}",
        ).collect()

        # The partition should be dropped after truncation
        assert len(original_partitions) > 0
        assert len(remaining_partitions) == 0

    def test_no_exceptions(self, create_external_table):
        """Test no exceptions are raised during truncation."""
        table_name, spark_session = create_external_table
        try:
            truncate_external_hive_table(spark_session, table_name)
        except Exception as e:
            pytest.fail(f"Truncation raised an exception: {e}")


class TestCacheTimeDf:
    """Tests for the `cache_time_df` function."""

    @patch("rdsa_utils.helpers.pyspark.logger.info")  # Mock the logger
    def test_expected(self, mock_logger, create_spark_df):
        """Test caching a DataFrame and timing the process."""
        input_df = create_spark_df([("A", "B", "C"), (1, 2, 3)])

        start_time = time.time()
        cache_time_df(input_df)
        end_time = time.time()

        elapsed_time = round(end_time - start_time, 2)
        mock_logger.assert_called_once()
        log_message = mock_logger.call_args[0][0]
        assert f"Cached in {elapsed_time} seconds" in log_message

    def test_invalid_input(self):
        """Test invalid input type raises an error."""
        with pytest.raises(TypeError, match="Input must be a PySpark DataFrame"):
            cache_time_df(["not", "a", "DataFrame"])


class TestCountNulls:
    """Tests for the `count_nulls` function."""

    def test_expected(self, create_spark_df):
        """Test counting nulls in a DataFrame."""
        input_df = create_spark_df(
            [("col1", "col2", "col3"), (1, 2, None), (None, 4, None), (3, None, "C")],
        )

        expected = pd.DataFrame({"col1": [1], "col2": [1], "col3": [2]})

        actual = count_nulls(input_df)

        assert actual.equals(expected)

    def test_with_subset_columns(self, create_spark_df):
        """Test counting nulls with a subset of columns."""
        input_df = create_spark_df(
            [("col1", "col2", "col3"), (1, 2, None), (None, 4, None), (3, None, "C")],
        )

        expected = pd.DataFrame({"col1": [1], "col2": [1]})

        actual = count_nulls(input_df, subset_cols=["col1", "col2"])

        assert actual.equals(expected)

    def test_invalid_input(self):
        """Test invalid DataFrame input raises an error."""
        with pytest.raises(TypeError, match="Input must be a PySpark DataFrame"):
            count_nulls(["not", "a", "DataFrame"])

    def test_invalid_subset_cols(self, create_spark_df):
        """Test invalid subset_cols input raises an error."""
        input_df = create_spark_df(
            [("col1", "col2", "col3"), (1, 2, None), (None, 4, None), (3, None, "C")],
        )

        with pytest.raises(
            TypeError,
            match="subset_cols must be a list, a string, or None",
        ):
            count_nulls(input_df, subset_cols=12345)

        with pytest.raises(
            TypeError,
            match="All elements of subset_cols must be strings",
        ):
            count_nulls(input_df, subset_cols=["col1", 12345])


class TestAggregateCol:
    """Tests for the `aggregate_col` function."""

    def test_sum(self, create_spark_df):
        """Test summing values in a column."""
        input_df = create_spark_df([("col1 INT"), (1,), (2,), (3,)])
        result = aggregate_col(input_df, "col1", "sum")
        assert result == 6

    def test_invalid_operation(self, create_spark_df):
        """Test invalid operation raises ValueError."""
        input_df = create_spark_df([("col1 INT"), (1,), (2,), (3,)])
        with pytest.raises(ValueError, match="`operation` must be one of"):
            aggregate_col(input_df, "col1", "invalid")


class TestGetUnique:
    """Tests for the `get_unique` function."""

    def test_remove_null(self, create_spark_df):
        """Test removing null values from the unique list."""
        input_df = create_spark_df(["col1 INT", (1,), (2,), (None,)])
        result = get_unique(input_df, "col1", remove_null=True)
        assert result == [1, 2]

    def test_keep_null(self, create_spark_df):
        """Test keeping null values in the unique list."""
        input_df = create_spark_df(["col1 INT", (1,), (2,), (None,)])
        result = get_unique(input_df, "col1", remove_null=False)
        assert result == [1, 2, None]

    def test_invalid_column(self, create_spark_df):
        """Test invalid column raises an error."""
        input_df = create_spark_df(["col1 INT", (1,), (2,), (3,)])
        with pytest.raises(TypeError, match="Column name must be a string"):
            get_unique(input_df, 123)


class TestDropDuplicatesReproducible:
    """Tests for the `dropDuplicates_reproducible` function."""

    def test_with_id_col(self, create_spark_df):
        """Test dropping duplicates with a specified ID column."""
        input_df = create_spark_df(
            ["group_col STRING, id_col INT", ("A", 1), ("A", 2), ("B", 3), ("B", 4)],
        )
        result_df = drop_duplicates_reproducible(input_df, "group_col", id_col="id_col")
        expected_df = create_spark_df(
            ["group_col STRING, id_col INT", ("A", 1), ("B", 3)],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_without_id_col(self, create_spark_df):
        """Test dropping duplicates without a specified ID column."""
        input_df = create_spark_df(
            ["group_col STRING, value_col INT", ("A", 1), ("A", 2), ("B", 3), ("B", 4)],
        )
        result_df = drop_duplicates_reproducible(input_df, "group_col")
        assert result_df.select("group_col").distinct().count() == 2

    def test_invalid_column(self, create_spark_df):
        """Test invalid column raises an error."""
        input_df = create_spark_df(["group_col STRING, id_col INT", ("A", 1), ("B", 3)])
        with pytest.raises(TypeError, match="col must be a string"):
            drop_duplicates_reproducible(input_df, 123)


class TestApplyColFunc:
    """Tests for the `apply_col_func` function."""

    def test_apply_function(self, create_spark_df):
        """Test applying a function to multiple columns."""
        input_df = create_spark_df(["col1 INT, col2 INT", (1, 2), (3, 4)])

        def increment_column(df, col):
            """Test method."""
            return df.withColumn(col, F.col(col) + 1)

        result_df = apply_col_func(input_df, ["col1", "col2"], increment_column)
        expected_df = create_spark_df(["col1 INT, col2 INT", (2, 3), (4, 5)])
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_invalid_column(self, create_spark_df):
        """Test invalid column list raises an error."""
        input_df = create_spark_df(["col1 INT, col2 INT", (1, 2), (3, 4)])
        with pytest.raises(TypeError, match="cols must be a list of strings."):
            apply_col_func(input_df, "not_a_list", lambda df, col: df)

    def test_invalid_function(self, create_spark_df):
        """Test invalid function raises an error."""
        input_df = create_spark_df(["col1 INT, col2 INT", (1, 2), (3, 4)])
        with pytest.raises(TypeError, match="func must be a callable function"):
            apply_col_func(input_df, ["col1", "col2"], "not_a_function")


class TestPysparkRandomUniform:
    """Tests for the `pyspark_random_uniform` function."""

    def test_random_uniform_default_bounds(self, create_spark_df):
        """Test random uniform column with default bounds (0 to 1)."""
        input_df = create_spark_df(["id INT", (1,), (2,), (3,)])
        result_df = pyspark_random_uniform(input_df, "random_col")
        assert "random_col" in result_df.columns
        assert (
            result_df.filter(
                (F.col("random_col") < 0) | (F.col("random_col") > 1),
            ).count()
            == 0
        )

    def test_random_uniform_custom_bounds(self, create_spark_df):
        """Test random uniform column with custom bounds."""
        input_df = create_spark_df(["id INT", (1,), (2,), (3,)])
        result_df = pyspark_random_uniform(
            input_df,
            "random_col",
            lower_bound=5,
            upper_bound=10,
        )
        assert "random_col" in result_df.columns
        assert (
            result_df.filter(
                (F.col("random_col") < 5) | (F.col("random_col") > 10),
            ).count()
            == 0
        )

    def test_random_uniform_with_seed(self, create_spark_df):
        """Test random uniform column with a fixed seed."""
        input_df = create_spark_df(["id INT", (1,), (2,), (3,)])
        result_df_1 = pyspark_random_uniform(input_df, "random_col", seed=42)
        result_df_2 = pyspark_random_uniform(input_df, "random_col", seed=42)
        assert_df_equality(result_df_1, result_df_2, ignore_nullable=True)


class TestCumulativeArray:
    """Tests for the `cumulative_array` function."""

    def test_cumulative_array_basic(self, create_spark_df):
        """Test creating a cumulative array column."""
        input_df = create_spark_df(
            ["id INT, values ARRAY<DOUBLE>", (1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0])],
        )
        result_df = cumulative_array(input_df, "values", "cumulative_values")
        expected_df = create_spark_df(
            [
                "id INT, values ARRAY<DOUBLE>, cumulative_values ARRAY<DOUBLE>",
                (1, [1.0, 2.0, 3.0], [1.0, 3.0, 6.0]),
                (2, [4.0, 5.0], [4.0, 9.0]),
            ],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_cumulative_array_empty(self, create_spark_df):
        """Test cumulative array column with an empty array."""
        input_df = create_spark_df(["id INT, values ARRAY<DOUBLE>", (1, [])])
        result_df = cumulative_array(input_df, "values", "cumulative_values")
        expected_df = create_spark_df(
            [
                "id INT, values ARRAY<DOUBLE>, cumulative_values ARRAY<DOUBLE>",
                (1, [], []),
            ],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)


class TestUnionMismatchedDfs:
    """Tests for the `union_mismatched_dfs` function."""

    def test_union_mismatched_basic(self, create_spark_df):
        """Test union of DataFrames with mismatched columns."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice"), (2, "Bob")])
        df2 = create_spark_df(["id INT, age INT", (3, 30), (4, 40)])
        result_df = union_mismatched_dfs(df1, df2)
        expected_df = create_spark_df(
            [
                "id INT, name STRING, age INT",
                (1, "Alice", None),
                (2, "Bob", None),
                (3, None, 30),
                (4, None, 40),
            ],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_union_mismatched_no_overlap(self, create_spark_df):
        """Test union of DataFrames with no overlapping columns."""
        df1 = create_spark_df(["id INT", (1,), (2,)])
        df2 = create_spark_df(["name STRING", ("Alice",), ("Bob",)])
        result_df = union_mismatched_dfs(df1, df2)
        expected_df = create_spark_df(
            [
                "id INT, name STRING",
                (1, None),
                (2, None),
                (None, "Alice"),
                (None, "Bob"),
            ],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_union_mismatched_empty_df(self, create_spark_df):
        """Test union where one DataFrame is empty."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice")])
        df2 = create_spark_df(["id INT, name STRING"])
        result_df = union_mismatched_dfs(df1, df2)
        expected_df = create_spark_df(["id INT, name STRING", (1, "Alice")])
        assert_df_equality(result_df, expected_df, ignore_nullable=True)


class TestSumColumns:
    """Tests for the `sum_columns` function."""

    def test_sum_columns_basic(self, create_spark_df):
        """Test summing multiple columns into a new column."""
        input_df = create_spark_df(
            ["col1 INT, col2 INT, col3 INT", (1, 2, 3), (4, 5, 6)],
        )
        result_df = sum_columns(input_df, ["col1", "col2"], "sum_col")
        expected_df = create_spark_df(
            ["col1 INT, col2 INT, col3 INT, sum_col INT", (1, 2, 3, 3), (4, 5, 6, 9)],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_sum_columns_invalid_cols(self, create_spark_df):
        """Test invalid column names raise an error."""
        input_df = create_spark_df(["col1 INT, col2 INT", (1, 2)])
        with pytest.raises(TypeError, match="cols_to_sum must be a list"):
            sum_columns(input_df, "not_a_list", "sum_col")

    def test_sum_columns_invalid_output_col(self, create_spark_df):
        """Test invalid output column raises an error."""
        input_df = create_spark_df(["col1 INT, col2 INT", (1, 2)])
        with pytest.raises(TypeError, match="output_col must be a string"):
            sum_columns(input_df, ["col1", "col2"], 123)


class TestSetNulls:
    """Tests for the `set_nulls` function."""

    def test_set_nulls_basic(self, create_spark_df):
        """Test replacing specified values with nulls."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",), ("C",)])
        result_df = set_nulls(input_df, "col1", ["B", "C"])
        expected_df = create_spark_df(["col1 STRING", ("A",), (None,), (None,)])
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_set_nulls_single_value(self, create_spark_df):
        """Test replacing a single value with nulls."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",), ("C",)])
        result_df = set_nulls(input_df, "col1", "B")
        expected_df = create_spark_df(["col1 STRING", ("A",), (None,), ("C",)])
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_set_nulls_invalid_column(self, create_spark_df):
        """Test invalid column raises an error."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",)])
        with pytest.raises(TypeError, match="column must be a string"):
            set_nulls(input_df, 123, ["A"])


class TestUnionMultiDfs:
    """Tests for the `union_multi_dfs` function."""

    def test_union_multi_basic(self, create_spark_df):
        """Test union of multiple DataFrames."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice"), (2, "Bob")])
        df2 = create_spark_df(["id INT, name STRING", (3, "Charlie"), (4, "Diana")])
        result_df = union_multi_dfs([df1, df2])
        expected_df = create_spark_df(
            [
                "id INT, name STRING",
                (1, "Alice"),
                (2, "Bob"),
                (3, "Charlie"),
                (4, "Diana"),
            ],
        )
        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_union_multi_empty_list(self):
        """Test union with an empty list raises an error."""
        with pytest.raises(ValueError, match="df_list must not be empty"):
            union_multi_dfs([])

    def test_union_multi_invalid_list(self, create_spark_df):
        """Test union with a non-DataFrame list raises an error."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice")])
        with pytest.raises(
            TypeError,
            match="All elements in df_list must be PySpark DataFrames.",
        ):
            union_multi_dfs([df1, "not_a_dataframe"])


class TestJoinMultiDfs:
    """Tests for the `join_multi_dfs` function."""

    def test_join_multi_inner(self, create_spark_df):
        """Test inner join of multiple DataFrames."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice"), (2, "Bob")])
        df2 = create_spark_df(["id INT, age INT", (1, 25), (2, 30)])
        df3 = create_spark_df(
            ["id INT, city STRING", (1, "New York"), (2, "Los Angeles")],
        )

        result_df = join_multi_dfs([df1, df2, df3], on="id", how="inner")
        expected_df = create_spark_df(
            [
                "id INT, name STRING, age INT, city STRING",
                (1, "Alice", 25, "New York"),
                (2, "Bob", 30, "Los Angeles"),
            ],
        )

        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_join_multi_outer(self, create_spark_df):
        """Test outer join of multiple DataFrames."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice"), (2, "Bob")])
        df2 = create_spark_df(["id INT, age INT", (1, 25), (3, 40)])

        result_df = join_multi_dfs([df1, df2], on="id", how="outer")
        expected_df = create_spark_df(
            [
                "id INT, name STRING, age INT",
                (1, "Alice", 25),
                (2, "Bob", None),
                (3, None, 40),
            ],
        )

        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_join_multi_invalid_how(self, create_spark_df):
        """Test invalid join type raises an error."""
        df1 = create_spark_df(["id INT, name STRING", (1, "Alice")])
        df2 = create_spark_df(["id INT, age INT", (1, 25)])

        with pytest.raises(ValueError, match="'how' must be one of"):
            join_multi_dfs([df1, df2], on="id", how="invalid")


class TestMapColumnValues:
    """Tests for the `map_column_values` function."""

    def test_map_column_values_basic(self, create_spark_df):
        """Test basic dictionary replacement."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",), ("C",)])

        result_df = map_column_values(input_df, {"A": "Apple", "B": "Banana"}, "col1")
        expected_df = create_spark_df(["col1 STRING", ("Apple",), ("Banana",), ("C",)])

        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_map_column_values_with_output_col(self, create_spark_df):
        """Test dictionary replacement with a specified output column."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",), ("C",)])

        result_df = map_column_values(
            input_df,
            {"A": "Apple", "B": "Banana"},
            "col1",
            "new_col",
        )
        expected_df = create_spark_df(
            [
                "col1 STRING, new_col STRING",
                ("A", "Apple"),
                ("B", "Banana"),
                ("C", "C"),
            ],
        )

        assert_df_equality(result_df, expected_df, ignore_nullable=True)

    def test_map_column_values_invalid_dict(self, create_spark_df):
        """Test invalid dictionary raises an error."""
        input_df = create_spark_df(["col1 STRING", ("A",), ("B",)])

        with pytest.raises(TypeError, match="dict_ must be a dictionary"):
            map_column_values(input_df, "not_a_dict", "col1")
