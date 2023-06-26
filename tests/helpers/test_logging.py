"""Tests for the logging.py module."""
import logging
import pytest
from unittest import mock

from tests.conftest import (
    Case,
    create_dataframe,
    parametrize_cases,
)

from rdsa_utils.helpers.logging import (
    timer_args,
    print_full_table_and_raise_error,
)


logger = logging.getLogger(__name__)


@pytest.mark.skip(
    reason='Not required to test creating a custom log level between INFO and DEBUG named DEV.',
)
class TestLogDev:
    """Tests for log_dev."""

    def test_init(self):
        """Test for log_dev."""
        pass


@pytest.mark.skip(reason='Not required to test instantiating of logger.')
class TestInitLoggerBasic:
    """Tests for the init_logger_basic function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


class TestTimerArgs:
    """Tests for the timer_args function."""

    @parametrize_cases(
        Case(
            label='logger_is_None',
            logger=None,
            expected={
                'name': 'load configs',
                'text': 'test',
                'logger': None,
                'initial_text': 'Running load configs',
            },
        ),
        Case(
            label='logger.info',
            logger=logger.info,
            expected={
                'name': 'load configs',
                'text': 'test',
                'logger': logger.info,
                'initial_text': 'Running load configs',
            },
        ),
    )
    def test_expected(self, logger, expected):
        """Test expected outputs."""
        actual = timer_args(name='load configs', logger=logger)
        # Need to mock as function will be timer_args vs TestTimerArgs and Running load configs vs Running {name} will fail.
        with mock.patch.dict(
            actual, {'text': 'test', 'initial_text': 'Running load configs'},
        ):
            assert actual == expected


class TestPrintFullTables:
    """Tests for the print_full_table_and_raise_error."""

    @pytest.fixture()
    def input_df(self):
        """Input pandas dataframe."""
        return create_dataframe(
            [
                ('shop', 'code', 'product_name'),
                ('shop_1', '111', 'lemonade 200ml'),
                ('shop_1', '222', 'royal gala 4 pack'),
            ],
        )

    @parametrize_cases(
        Case(
            label='stop_pipeline_True_show_records_True',
            stop_pipeline=True,
            show_records=True,
        ),
        Case(
            label='stop_pipeline_True_show_records_True',
            stop_pipeline=True,
            show_records=False,
        ),
    )
    def test_raises_error(self, input_df, stop_pipeline, show_records):
        """Tests that an error is raised."""
        message = 'This causes an error'
        with pytest.raises(ValueError):
            print_full_table_and_raise_error(
                input_df, message, stop_pipeline, show_records,
            )

    def test_expected_with_no_errors_show_records_true(self, caplog, input_df):
        """Tests that the correct logger infomation is displayed."""
        stop_pipeline = False
        show_records = True
        message = 'Info'
        caplog.set_level(logging.INFO)
        print_full_table_and_raise_error(input_df, message, stop_pipeline, show_records)
        assert input_df.to_string() in caplog.text
        assert message in caplog.text

    def test_expected_with_no_errors_all_args_false(self, caplog, input_df):
        """Tests that the correct logger infomation is displayed."""
        stop_pipeline = False
        show_records = False
        message = 'Info'
        caplog.set_level(logging.INFO)
        print_full_table_and_raise_error(input_df, message, stop_pipeline, show_records)
        assert message in caplog.text


@pytest.mark.skip(reason='Not required to test decorator functions.')
class TestLogSparkDfSchema:
    """Tests for the log_spark_df_schema function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


@pytest.mark.skip(reason='Not required to test decorator functions.')
class TestLogRowsInSparkDf:
    """Tests for the log_rows_in_spark_df function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


@pytest.mark.skip(reason='Not required to test decorator functions.')
class TestAddWarningMessageToFunction:
    """Tests for the _add_warning_message_to_function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass
