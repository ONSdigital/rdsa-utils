import pytest

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


@pytest.mark.skip(reason='requires GCP')
class TestLoadConfigGcp:
    """Test for load_config_gcp function."""

    def test_expected(self):
        """Test expected functionality."""
        pass
