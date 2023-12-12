"""Tests for the io.py module."""
import textwrap

import pytest

from tests.conftest import (
    Case,
    parametrize_cases,
)
from rdsa_utils.gcp.io.inputs import (
    build_sql_query,
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
