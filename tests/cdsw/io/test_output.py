"""Tests for cdsw/io/output.py module."""

import pytest


@pytest.mark.skip(reason='requires HDFS')
class TestSaveCSVToHDFS:
    """Test for save_csv_to_hdfs function."""

    def test_expected(self):
        """Test expected functionality."""
        pass
