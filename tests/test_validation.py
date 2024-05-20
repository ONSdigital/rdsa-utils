"""Tests for the validation helpers module."""

import pytest

from rdsa_utils.validation import *


@pytest.mark.skip(reason="test shell")
class TestApplyValidation:
    """Tests for the apply_validation function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason="decorator wrapper of list_convert function.")
class TestListConvertValidator:
    """Tests for the list_convert_validator function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestAllowedDateFormat:
    """Tests for the allowed_date_format function.

    Exhaustive positive and negative testing not done as thee function relies
    soley on the pandas to_datetime function so testing is assumed there.
    """

    def test_expected(self):
        """Test expected functionality."""
        date = "Feb 2022"

        actual = allowed_date_format(date)

        assert actual == date
