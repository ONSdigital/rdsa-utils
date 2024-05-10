"""Tests for impala.py module."""
import subprocess

from rdsa_utils.cdp.helpers.impala import invalidate_impala_metadata


class TestInvalidateImpalaMetadata:
    """Tests for invalidate_impala_metadata function."""

    def test_invalidate_impala_metadata(self, mocker):
        """Test the invalidate_impala_metadata function.

        Parameters
        ----------
        mocker : pytest_mock.MockFixture
            Pytest's MockFixture object to mock subprocess.run().

        Notes
        -----
        This test verifies the following:
        1. The correct impala-shell command is executed with
           the correct arguments.
        2. The function does not raise any exceptions.
        3. The function correctly handles and logs the stderr output
           when keep_stderr is True.
        """
        # Mock the subprocess.run() call
        mock_subprocess_run = mocker.patch('subprocess.run')

        # Set up test parameters
        table = 'test_table'
        impalad_address_port = 'localhost:21050'
        impalad_ca_cert = '/path/to/ca_cert.pem'

        # Mock logger.info
        mock_logger_info = mocker.patch('logging.Logger.info')

        # Call the function without keep_stderr
        invalidate_impala_metadata(table, impalad_address_port, impalad_ca_cert)

        # Check that subprocess.run() was called with the correct arguments
        mock_subprocess_run.assert_called_with(
            [
                'impala-shell',
                '-k',
                '--ssl',
                '-i',
                impalad_address_port,
                '--ca_cert',
                impalad_ca_cert,
                '-q',
                f'invalidate metadata {table};',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Reset the mock
        mock_subprocess_run.reset_mock()

        # Call the function with keep_stderr
        result = subprocess.CompletedProcess(
            args=['dummy'], returncode=0, stdout=b'', stderr=b'Test Error',
        )
        mock_subprocess_run.return_value = result

        invalidate_impala_metadata(
            table,
            impalad_address_port,
            impalad_ca_cert,
            keep_stderr=True,
        )

        # Check that subprocess.run() was called with the correct arguments
        # and logger.info() was called with the expected error message.
        mock_subprocess_run.assert_called_with(
            [
                'impala-shell',
                '-k',
                '--ssl',
                '-i',
                impalad_address_port,
                '--ca_cert',
                impalad_ca_cert,
                '-q',
                f'invalidate metadata {table};',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_logger_info.assert_called_once_with('Test Error')
