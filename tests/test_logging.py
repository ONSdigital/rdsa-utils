"""Tests for the logging.py module."""

import logging
import sys
from pathlib import Path
from unittest import mock

import pytest

from rdsa_utils.logging import (
    init_logger_advanced,
    print_full_table_and_raise_error,
    timer_args,
)
from rdsa_utils.test_utils import *

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

    @pytest.fixture
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
    """Tests for init_logger_advanced."""

    def setup_method(self) -> None:
        """Clear all handlers before each test."""
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

    def test_basic_config_applied_when_no_handlers(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """BasicConfig applied when no handlers provided."""
        # Simulate no existing handlers
        monkeypatch.setattr(logging.Logger, "hasHandlers", lambda self: False)
        init_logger_advanced(logging.INFO)
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO
        assert len(root_logger.handlers) >= 1

    def test_custom_handlers_added(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Custom handlers are added to root logger with correct formatter."""
        # Ensure handlers are treated as none initially
        monkeypatch.setattr(logging.Logger, "hasHandlers", lambda self: False)
        stream_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
        log_file: Path = tmp_path / "test.log"
        file_handler: logging.FileHandler = logging.FileHandler(str(log_file))
        init_logger_advanced(
            logging.DEBUG,
            [stream_handler, file_handler],
            "%(message)s",
            "%H:%M:%S",
        )
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG
        handlers = root_logger.handlers
        # Check that our handlers are present
        assert stream_handler in handlers
        assert file_handler in handlers
        # Verify each custom handler uses the provided format
        for handler in (stream_handler, file_handler):
            assert handler.formatter._fmt == "%(message)s"

    def test_value_error_for_invalid_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Raises ValueError for invalid handler types."""
        monkeypatch.setattr(logging.Logger, "hasHandlers", lambda self: False)
        with pytest.raises(ValueError):
            init_logger_advanced(logging.WARNING, handlers=[object()])

    def test_no_duplicate_handlers_if_already_configured(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Existing handler remains when logger already configured."""
        # Simulate existing handlers to trigger early exit
        monkeypatch.setattr(logging.Logger, "hasHandlers", lambda self: True)
        existing_handler: logging.StreamHandler = logging.StreamHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(existing_handler)
        init_logger_advanced(logging.ERROR)
        handlers = logging.getLogger().handlers
        assert existing_handler in handlers
