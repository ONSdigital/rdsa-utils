"""Tests for the load_configs module."""
from unittest import mock

from rdsa_utils.helpers.load_configs import load_config_from_local


class TestLoadConfigFromLocal:
    """Tests for load_config_from_local function."""

    @mock.patch('builtins.open', mock.mock_open(read_data='data'))
    def test_load_config_from_local(self):
        """Tests for expected functionality."""
        result = load_config_from_local('some_file.yaml')
        assert 'data' == result
