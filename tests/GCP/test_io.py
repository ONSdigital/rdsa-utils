"""Tests for the io.py module."""
import textwrap
from unittest import mock

from chispa import assert_df_equality
from pandas import Timestamp
import pytest

from tests.conftest import (
    Case,
    create_dataframe,
    parametrize_cases,
)
from rdsa_utils.GCP.io import (
    build_sql_query,
    convert_struc_col_to_columns,
    filter_dates_to_analysis_period,
    load_config_from_local,
)


@pytest.mark.skip(reason='required table path')
class TestReadTable:
    """Test for read_table function.."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestBuildSqlQuery:
    """Tests for build_sql_query."""

    @parametrize_cases(
        Case(
            label='no additional filters specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=None,
            expected="""
                SELECT *
                FROM database_name.table_name
            """,
        ),
        Case(
            label='columns_specified',
            columns=['col1', 'col2'],
            date_column=None,
            date_range=None,
            column_filter_dict=None,
            expected="""
                SELECT col1, col2
                FROM database_name.table_name
            """,
        ),
        Case(
            label='date range specified',
            columns=None,
            date_column='date',
            date_range=('2019-01-01', '2021-01-01'),
            column_filter_dict=None,
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                date >= '2019-01-01'
                AND date < '2021-01-01'
                )
            """,
        ),
        Case(
            label='one filter column, one option specified as string',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': 'value_1.1',
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                )
            """,
        ),
        Case(
            label='one filter column, one option specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': ['value_1.1'],
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                )
            """,
        ),
        Case(
            label='one filter column, two options specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': ['value_1.1', 'value_1.2'],
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
            """,
        ),
        Case(
            label='two filter columns, one option specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': [2020],
                'column_2': ['value_2.1'],
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 2020
                )
                AND (
                column_2 = 'value_2.1'
                )
            """,
        ),
        Case(
            label='two filter columns, 2 and 1 (as string) options specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': ['value_1.1', 'value_1.2'],
                'column_2': 2020,
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
                AND (
                column_2 = 2020
                )
            """,
        ),
        Case(
            label='two filter columns, 2 and 1 options specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict={
                'column_1': ['value_1.1', 'value_1.2'],
                'column_2': [2020],
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
                AND (
                column_2 = 2020
                )
            """,
        ),
        Case(
            label='date_range and filter columns specified',
            columns=None,
            date_column='date',
            date_range=('2019-01-01', '2021-01-01'),
            column_filter_dict={
                'column_1': ['value_1.1', 'value_1.2'],
                'column_2': ['value_2.1', 'value_2.2', 'value_2.3'],
            },
            expected="""
                SELECT *
                FROM database_name.table_name
                WHERE (
                date >= '2019-01-01'
                AND date < '2021-01-01'
                )
                AND (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
                AND (
                column_2 = 'value_2.1'
                OR column_2 = 'value_2.2'
                OR column_2 = 'value_2.3'
                )
            """,
        ),
    )
    def test_method(
        self,
        columns,
        date_column,
        date_range,
        column_filter_dict,
        expected,
    ):
        """Test expected behaviour."""
        table_path = 'database_name.table_name'

        result = (
            build_sql_query(
                table_path,
                columns=columns,
                date_column=date_column,
                date_range=date_range,
                column_filter_dict=column_filter_dict,
            )
        )

        # Use textwrap.dedent to remove leading whitespace from the
        # string for comparing
        expected = textwrap.dedent(expected)

        assert(result.strip('\n') == expected.strip('\n'))


class TestConvertStrucColToColumns:
    """Tests for the convert_struc_col_to_columns function."""

    @parametrize_cases(
        Case(
            label='no_struct_type_columns',
            input_df=[
                ('string_col', 'num1_col', 'num2_col'),
                ('a', 1, 2),
                ('b', 9, 8),
            ],
            expected=[
                ('string_col', 'num1_col', 'num2_col'),
                ('a', 1, 2),
                ('b', 9, 8),
            ],
        ),
        Case(
            label='one_struct_type_column',
            input_df=[
                ('string_col', 'struct_col'),
                ('a', (1, 2)),
                ('b', (9, 8)),
            ],
            expected=[
                ('string_col', '_1', '_2'),
                ('a', 1, 2),
                ('b', 9, 8),
            ],
        ),
        Case(
            label='many_struct_type_columns',
            input_df=[
                ('string_col', 'struct1_col', 'struct2_col'),
                ('a', (1, 2), (3, 4)),
                ('b', (9, 8), (7, 6)),
            ],
            expected=[
                ('string_col', '_1', '_2', '_1', '_2'),
                ('a', 1, 2, 3, 4),
                ('b', 9, 8, 7, 6),
            ],
        ),
        Case(
            label='nested_struct_type_column',
            input_df=[
                ('string_col', 'struct_col'),
                ('a', ((1, 2), (3, 4))),
                ('b', ((9, 8), (7, 6))),
            ],
            expected=[
                ('string_col', '_1', '_2'),
                ('a', (1, 2), (3, 4)),
                ('b', (9, 8), (7, 6)),
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
            df=create_spark_df([
                ('string_col', 'struct_col'),
                ('a', ((1, 2), (3, 4))),
                ('b', ((9, 8), (7, 6))),
            ]),
            convert_nested_structs=True,
        )

        assert_df_equality(
            actual,
            create_spark_df([
                ('string_col', '_1', '_2', '_1', '_2'),
                ('a', 1, 2, 3, 4),
                ('b', 9, 8, 7, 6),
            ]),
        )


@pytest.mark.skip(reason='requires table path')
class TestWriteTable:
    """Test for write_table function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason='requires table path')
class TestGetTableColumns:
    """Test for get_table_columns function.."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason='requires table path')
class TestTableExists:
    """Test for table_exists function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestFilterDatesToAnalysisPeriod:
    """Test dates are within specified analysis period."""

    @pytest.fixture()
    def input_data(self, to_spark):
        """YYYY-MM-DD test data for filtering tests."""
        return to_spark(create_dataframe([
            ('dates', ),
            (Timestamp('2020-12-01'), ),  # Dec 2020
            (Timestamp('2021-01-01'), ),  # Jan 2021
            (Timestamp('2021-01-10'), ),  # Jan 2021
            (Timestamp('2021-02-01'), ),  # Feb 2021
            (Timestamp('2021-03-01'), ),  # Mar 2021
            (Timestamp('2021-03-31'), ),  # Mar 2021
            (Timestamp('2021-04-01'), ),  # Apr 2021
        ]))

    @parametrize_cases(
        Case(
            label='MMMM YYYY',
            start_date='January 2021',
            end_date='March 2021',
        ),
        Case(
            label='MMM YYYY',
            start_date='Jan 2021',
            end_date='Mar 2021',
        ),
        Case(
            label='YYYY MMMM',
            start_date='2021 January',
            end_date='2021 March',
        ),
        Case(
            label='YYYY MMM',
            start_date='2021 Jan',
            end_date='2021 Mar',
        ),
        Case(
            label='MM YYYY',
            start_date='01 2021',
            end_date='03 2021',
        ),
        Case(
            label='-M YYYY',
            start_date='1 2021',
            end_date='3 2021',
        ),
        Case(
            label='MM-YYYY',
            start_date='01-2021',
            end_date='03-2021',
        ),
        Case(
            label='-M-YYYY',
            start_date='1-2021',
            end_date='3-2021',
        ),
        Case(
            label='YYYY MM',
            marks=pytest.mark.xfail(reason='date format not used'),
            start_date='2021 01',
            end_date='2021 03',
        ),
        Case(
            label='YYYY -M',
            marks=pytest.mark.xfail(reason='date format not used'),
            start_date='2021 1',
            end_date='2021 3',
        ),
        Case(
            label='YYYY-MM',
            marks=pytest.mark.xfail(reason='date format not used'),
            start_date='2021-01',
            end_date='2021-03',
        ),
        Case(
            label='YYYY--M',
            marks=pytest.mark.xfail(reason='date format not used'),
            start_date='2021-1',
            end_date='2021-3',
        ),
    )
    def test_filter_dates_for_year_month_format(
        self,
        to_spark,
        input_data,
        start_date,
        end_date,
    ):
        """Test different date formats for start and end date."""
        actual = filter_dates_to_analysis_period(
            df=input_data,
            dates='dates',
            start_date=start_date,
            end_date=end_date,
        )

        expected = to_spark(create_dataframe([
            ('dates', ),
            (Timestamp('2021-01-01'), ),  # Jan 2021
            (Timestamp('2021-01-10'), ),  # Jan 2021
            (Timestamp('2021-02-01'), ),  # Feb 2021
            (Timestamp('2021-03-01'), ),  # Mar 2021
            (Timestamp('2021-03-31'), ),  # Mar 2021
        ]))

        assert_df_equality(actual, expected)

    def test_filter_dates_for_year_month_day_format(
        self,
        to_spark,
        input_data,
    ):
        """Test for yyyy-mm-dd format."""
        actual = filter_dates_to_analysis_period(
            df=input_data,
            dates='dates',
            start_date='2021-01-01',
            end_date='2021-03-01',
        )

        expected = to_spark(create_dataframe([
            ('dates', ),
            (Timestamp('2021-01-01'), ),  # Jan 2021
            (Timestamp('2021-01-10'), ),  # Jan 2021
            (Timestamp('2021-02-01'), ),  # Feb 2021
            (Timestamp('2021-03-01'), ),  # Mar 2021
        ]))

        assert_df_equality(actual, expected)

    @parametrize_cases(
        Case(
            label='start_date_on_date',
            start_date='2021-01-10',
            end_date='2021-03-10',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2021-01-10'), ),  # Jan 2021
                (Timestamp('2021-02-01'), ),  # Feb 2021
                (Timestamp('2021-03-01'), ),  # Mar 2021
            ]),
        ),
        Case(
            label='start_date_after_date',
            start_date='2021-01-11',
            end_date='2021-03-10',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2021-02-01'), ),  # Feb 2021
                (Timestamp('2021-03-01'), ),  # Mar 2021
            ]),
        ),
        Case(
            label='end_date_on_date',
            start_date='2021-01-10',
            end_date='2021-03-31',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2021-01-10'), ),  # Jan 2021
                (Timestamp('2021-02-01'), ),  # Feb 2021
                (Timestamp('2021-03-01'), ),  # Mar 2021
                (Timestamp('2021-03-31'), ),  # Mar 2021
            ]),
        ),
        Case(
            label='end_date_before_date',
            start_date='2021-01-10',
            end_date='2021-03-30',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2021-01-10'), ),  # Jan 2021
                (Timestamp('2021-02-01'), ),  # Feb 2021
                (Timestamp('2021-03-01'), ),  # Mar 2021
            ]),
        ),
    )
    def test_date_filter_is_inclusive(
        self,
        to_spark,
        input_data,
        start_date,
        end_date,
        expected,
    ):
        """Test demonstrating the start and end dates are inclusive."""
        actual = filter_dates_to_analysis_period(
            df=input_data,
            dates='dates',
            start_date=start_date,
            end_date=end_date,
        )

        assert_df_equality(actual, to_spark(expected))

    @pytest.fixture()
    def input_data_date_and_time(self, to_spark):
        """YYYY-MM-DD hh:mm:ss test data for filtering tests."""
        return to_spark(create_dataframe([
            ('dates', ),
            (Timestamp('2020-12-01 00:00:00'), ),  # Dec 2020
            (Timestamp('2020-12-01 01:00:00'), ),  # Dec 2020
            (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
            (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
            (Timestamp('2021-01-22 00:00:00'), ),  # Jan 2021
            (Timestamp('2021-01-22 00:00:01'), ),  # Jan 2021
            (Timestamp('2021-01-31 00:00:00'), ),  # Jan 2021
            (Timestamp('2021-01-31 00:00:01'), ),  # Jan 2021
            (Timestamp('2021-01-31 23:59:59'), ),  # Jan 2021
            (Timestamp('2021-02-01 00:00:00'), ),  # Feb 2021
            (Timestamp('2021-02-01 00:00:01'), ),  # Feb 2021
        ]))

    @parametrize_cases(
        Case(
            label='specifying_end_date_as_day_in_month',
            start_date='2020-12-01',
            end_date='2021-01-22',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2020-12-01 00:00:00'), ),  # Dec 2020
                (Timestamp('2020-12-01 01:00:00'), ),  # Dec 2020
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:01'), ),  # Jan 2021
            ]),
        ),
        Case(
            label='specifying_end_date_as_month',
            start_date='Dec 2020',
            end_date='Jan 2021',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2020-12-01 00:00:00'), ),  # Dec 2020
                (Timestamp('2020-12-01 01:00:00'), ),  # Dec 2020
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 23:59:59'), ),  # Jan 2021
            ]),
        ),
        Case(
            label='specifying_end_date_as_last_day_of_month',
            start_date='2020-12-01',
            end_date='2021-01-31',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2020-12-01 00:00:00'), ),  # Dec 2020
                (Timestamp('2020-12-01 01:00:00'), ),  # Dec 2020
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 23:59:59'), ),  # Jan 2021
            ]),
        ),
        Case(
            label='specifying_end_date_as_first_day_of_next_month_',
            start_date='2020-12-01',
            end_date='2021-02-01',
            expected=create_dataframe([
                ('dates', ),
                (Timestamp('2020-12-01 00:00:00'), ),  # Dec 2020
                (Timestamp('2020-12-01 01:00:00'), ),  # Dec 2020
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-01 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-22 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:00'), ),  # Jan 2021
                (Timestamp('2021-01-31 00:00:01'), ),  # Jan 2021
                (Timestamp('2021-01-31 23:59:59'), ),  # Jan 2021
                (Timestamp('2021-02-01 00:00:00'), ),  # Feb 2021
                (Timestamp('2021-02-01 00:00:01'), ),  # Feb 2021
            ]),
        ),
    )
    def test_date_filter_inclusivity_on_date_and_time(
        self,
        to_spark,
        input_data_date_and_time,
        start_date,
        end_date,
        expected,
    ):
        """Test demonstrating the start and end dates are inclusive."""
        actual = filter_dates_to_analysis_period(
            df=input_data_date_and_time,
            dates='dates',
            start_date=start_date,
            end_date=end_date,
        )

        assert_df_equality(actual, to_spark(expected))


class TestLoadConfigFromLocal:
    """Tests for load_config_from_local function."""

    @mock.patch('builtins.open', mock.mock_open(read_data='data'))
    def test_load_config_from_local(self):
        """Tests for expected functionality."""
        result = load_config_from_local('some_file.yaml')
        assert 'data' == result


@pytest.mark.skip(reason='test shell')
class TestLoadConfigGcp:
    """Test for load_config_gcp function."""

    def test_expected(self):
        """Test expected functionality."""
        pass
