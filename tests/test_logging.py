"""Tests for the logging.py module."""

import logging
from unittest import mock

import pytest

from rdsa_utils.logging import (
    init_logger_advanced,
    print_full_table_and_raise_error,
    timer_args,
)
from tests.conftest import Case, create_dataframe, parametrize_cases

logger = logging.getLogger(__name__)


@pytest.mark.skip(
    reason="Not required to test creating a custom log level between INFO and DEBUG named DEV.",
)
class TestLogDev:
    """Tests for log_dev."""

    def test_init(self):
        """Test for log_dev."""
        pass


@pytest.mark.skip(reason="Not required to test instantiating of logger.")
class TestInitLoggerBasic:
    """Tests for the init_logger_basic function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


class TestTimerArgs:
    """Tests for the timer_args function."""

    @parametrize_cases(
        Case(
            label="logger_is_None",
            logger=None,
            expected={
                "name": "load configs",
                "text": "test",
                "logger": None,
                "initial_text": "Running load configs",
            },
        ),
        Case(
            label="logger.info",
            logger=logger.info,
            expected={
                "name": "load configs",
                "text": "test",
                "logger": logger.info,
                "initial_text": "Running load configs",
            },
        ),
    )
    def test_expected(self, logger, expected):
        """Test expected outputs."""
        actual = timer_args(name="load configs", logger=logger)
        # Need to mock as function will be timer_args vs TestTimerArgs and Running load configs vs Running {name} will fail.
        with mock.patch.dict(
            actual,
            {"text": "test", "initial_text": "Running load configs"},
        ):
            assert actual == expected


class TestPrintFullTables:
    """Tests for the print_full_table_and_raise_error."""

    @pytest.fixture()
    def input_df(self):
        """Input pandas dataframe."""
        return create_dataframe(
            [
                ("shop", "code", "product_name"),
                ("shop_1", "111", "lemonade 200ml"),
                ("shop_1", "222", "royal gala 4 pack"),
            ],
        )

    @parametrize_cases(
        Case(
            label="stop_pipeline_True_show_records_True",
            stop_pipeline=True,
            show_records=True,
        ),
        Case(
            label="stop_pipeline_True_show_records_True",
            stop_pipeline=True,
            show_records=False,
        ),
    )
    def test_raises_error(self, input_df, stop_pipeline, show_records):
        """Tests that an error is raised."""
        message = "This causes an error"
        with pytest.raises(ValueError):
            print_full_table_and_raise_error(
                input_df,
                message,
                stop_pipeline,
                show_records,
            )

    def test_expected_with_no_errors_show_records_true(self, caplog, input_df):
        """Tests that the correct logger infomation is displayed."""
        stop_pipeline = False
        show_records = True
        message = "Info"
        caplog.set_level(logging.INFO)
        print_full_table_and_raise_error(input_df, message, stop_pipeline, show_records)
        assert input_df.to_string() in caplog.text
        assert message in caplog.text

    def test_expected_with_no_errors_all_args_false(self, caplog, input_df):
        """Tests that the correct logger infomation is displayed."""
        stop_pipeline = False
        show_records = False
        message = "Info"
        caplog.set_level(logging.INFO)
        print_full_table_and_raise_error(input_df, message, stop_pipeline, show_records)
        assert message in caplog.text


@pytest.mark.skip(reason="Not required to test decorator functions.")
class TestLogSparkDfSchema:
    """Tests for the log_spark_df_schema function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


@pytest.mark.skip(reason="Not required to test decorator functions.")
class TestLogRowsInSparkDf:
    """Tests for the log_rows_in_spark_df function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


@pytest.mark.skip(reason="Not required to test decorator functions.")
class TestAddWarningMessageToFunction:
    """Tests for the _add_warning_message_to_function."""

    def test_expected(self):
        """Test expected behaviour."""
        pass


class TestInitLoggerAdvanced:
    """Tests for init_logger_advanced function."""

    def test_logger_with_no_handler(self, caplog):
        """Test whether a logger is properly initialized with no handlers."""
        caplog.set_level(logging.DEBUG)
        init_logger_advanced(logging.DEBUG)
        assert caplog.records[0].levelname == "DEBUG"

    def test_logger_with_handlers(self, caplog):
        """Test whether a logger is properly initialized with a valid handler."""
        caplog.set_level(logging.DEBUG)
        handler = logging.FileHandler("logfile.log")
        handlers = [handler]
        init_logger_advanced(logging.DEBUG, handlers)

        logger = logging.getLogger("rdsa_utils.logging")

        assert caplog.records[0].levelname == "DEBUG"
        assert any(isinstance(h, type(handler)) for h in logger.handlers)

    def test_logger_with_invalid_handler(self):
        """Test whether a ValueError is raised when an invalid handler is provided."""
        log_level = logging.DEBUG
        invalid_handler = "I am not a handler"
        handlers = [invalid_handler]
        with pytest.raises(ValueError) as exc_info:
            init_logger_advanced(log_level, handlers)
        assert (
            str(exc_info.value)
            == f"Handler {invalid_handler} is not an instance of logging.Handler or its subclasses"
        )
