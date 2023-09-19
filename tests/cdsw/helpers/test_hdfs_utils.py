"""Tests for hdfs_utils.py module."""
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from rdsa_utils.cdsw.helpers.hdfs_utils import (
    _perform,
    change_permissions,
    copy,
    copy_local_to_hdfs,
    create_dir,
    create_txt_from_string,
    delete_dir,
    delete_file,
    file_exists,
    get_date_modified,
    isdir,
    move_local_to_hdfs,
    read_dir,
    read_dir_files,
    read_dir_files_recursive,
    rename,
)


class BaseTest:
    """BaseTest provides common pytest fixtures for mocking subprocess.Popen.

    The class contains two fixtures:
    1. mock_subprocess_popen: Replaces subprocess.Popen function with a mock that
       simulates a successful command execution.
    2. mock_subprocess_popen_date_modified: Replaces subprocess.Popen function
       with a mock that simulates a successful command execution with a specific date
       string as output.

    These fixtures are intended to be used by test cases that need to mock
    subprocess.Popen calls.
    """

    @pytest.fixture()
    def mock_subprocess_popen(self, monkeypatch):  # noqa: PT004
        """Fixture to mock the subprocess.Popen function.

        This fixture replaces the subprocess.Popen function with a mock implementation.
        The mock implementation returns a MagicMock object that simulates the behavior
        of the Popen object. It sets the returncode to 0 and configures the communicate
        method to return empty byte strings (b"") for stdout and stderr.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            The monkeypatch object provided by the pytest framework.

        Returns
        -------
        None
            This fixture does not return any value, but it patches the
            subprocess.Popen function.
        """

        def mock_popen(*args, **kwargs):
            process_mock = MagicMock()
            process_mock.returncode = 0
            process_mock.communicate.return_value = (b'', b'')

            return process_mock

        monkeypatch.setattr(subprocess, 'Popen', mock_popen)

    @pytest.fixture()
    def mock_subprocess_popen_date_modifed(self):
        """Fixture to mock subprocess.Popen for testing get_date_modified.

        This fixture patches the subprocess.Popen function using the patch decorator
        from the unittest.mock module.

        It configures the mock implementation to return a MagicMock object for
        mocking the Popen object.

        The MagicMock object is configured to simulate the behavior of stdout.read()
        method by returning a byte string representing the date "2023-05-25" when
        called with decode method.

        Yields
        ------
        MagicMock
            The MagicMock object that simulates the behavior of subprocess.Popen.
        """
        with patch('subprocess.Popen') as mock_popen:
            mock_stdout = MagicMock()
            mock_stdout.read.return_value.decode.return_value = '2023-05-25'
            mock_popen.return_value.communicate.return_value = (mock_stdout, None)
            yield mock_popen


class TestPerform(BaseTest):
    """Tests for _perform function."""

    def test__perform(self, mock_subprocess_popen):
        """Check _perform function behavior on successful command execution."""
        command = ['ls', '-l', '/home/user']
        assert _perform(command) is True


class TestChangePermissons(BaseTest):
    """Tests for change_permissions function."""

    def test_change_permissions(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -chmod' command by the change_permissions function.

        Checks if the command is correctly constructed based on the provided arguments.
        """
        # Test case 1: Test change_permissions without recursive option
        path = '/user/example'
        permission = 'go+rwx'
        command = ['hadoop', 'fs', '-chmod', permission, path]
        assert change_permissions(path, permission) == _perform(command)

        # Test case 2: Test change_permissions with recursive option
        recursive_path = '/user/example'
        recursive_permission = '777'
        recursive_command = [
            'hadoop',
            'fs',
            '-chmod',
            '-R',
            recursive_permission,
            recursive_path,
        ]
        assert change_permissions(
            recursive_path,
            recursive_permission,
            recursive=True,
        ) == _perform(recursive_command)


class TestCopy(BaseTest):
    """Tests for copy function."""

    def test_copy(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -cp' command by the copy function.

        Checks if the command is correctly constructed based on the provided arguments.
        """
        # Test case 1: Test copy without overwrite option
        from_path = '/user/example/file.txt'
        to_path = '/user/backup/file.txt'
        command = ['hadoop', 'fs', '-cp', from_path, to_path]
        assert copy(from_path, to_path) == _perform(command)

        # Test case 2: Test copy with overwrite option
        overwrite_from_path = '/user/example/file.txt'
        overwrite_to_path = '/user/backup/file.txt'
        overwrite_command = [
            'hadoop',
            'fs',
            '-cp',
            '-f',
            overwrite_from_path,
            overwrite_to_path,
        ]
        assert copy(overwrite_from_path, overwrite_to_path, overwrite=True) == _perform(
            overwrite_command,
        )


class TestCopyLocalToHDFS(BaseTest):
    """Tests for copy_local_to_hdfs function."""

    def test_copy_local_to_hdfs(self, mock_subprocess_popen):
        """Verify 'hadoop fs -copyFromLocal' command execution by copy_local_to_hdfs.

        Checks if the command is correctly constructed based on the provided arguments.
        """
        # Test case 1: Test copy_local_to_hdfs without overwrite option
        from_path = '/local/path/file.txt'
        to_path = '/user/hdfs/path/file.txt'
        command = ['hadoop', 'fs', '-copyFromLocal', from_path, to_path]
        assert copy_local_to_hdfs(from_path, to_path) == _perform(command)

        # Test case 2: Test copy_local_to_hdfs with overwrite option
        overwrite_from_path = '/local/path/file.txt'
        overwrite_to_path = '/user/hdfs/path/file.txt'
        overwrite_command = [
            'hadoop',
            'fs',
            '-copyFromLocal',
            '-f',
            overwrite_from_path,
            overwrite_to_path,
        ]
        assert copy_local_to_hdfs(overwrite_from_path, overwrite_to_path) == _perform(
            overwrite_command,
        )


class TestCreateDir(BaseTest):
    """Tests for create_dir function."""

    def test_create_dir(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -mkdir' command by the create_dir function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test create_dir with a valid path
        path = '/user/new_directory'
        command = ['hadoop', 'fs', '-mkdir', path]
        assert create_dir(path) == _perform(command)


class TestCreateTxtFromString:
    """Tests for create_txt_from_string function."""

    @pytest.mark.parametrize(
        ('path', 'string_to_write', 'replace', 'expected_call'),
        [
            (
                '/some/directory/newfile.txt',
                'Hello, world!',
                False,
                ['echo "Hello, world!" | hadoop fs -put - /some/directory/newfile.txt'],
            ),
            (
                '/some/directory/newfile.txt',
                'Hello, world!',
                True,
                ['echo "Hello, world!" | hadoop fs -put - /some/directory/newfile.txt'],
            ),
        ],
    )
    def test_create_txt_from_string(
        self,
        path,
        string_to_write,
        replace,
        expected_call,
    ):
        """Verify 'echo | hadoop fs -put -' command execution by create_txt_from_string."""
        with patch('subprocess.call') as subprocess_mock, patch(
            'rdsa_utils.cdsw.hdfs_utils.file_exists',
        ) as file_exists_mock, patch(
            'rdsa_utils.cdsw.hdfs_utils.delete_file',
        ) as delete_file_mock:
            file_exists_mock.return_value = (
                replace  # Assume file exists if replace is True
            )

            if expected_call:
                # Test if subprocess.call is called correctly
                create_txt_from_string(path, string_to_write, replace)
                subprocess_mock.assert_called_with(expected_call, shell=True)
            else:
                # Test if FileNotFoundError is raised
                with pytest.raises(FileNotFoundError) as excinfo:
                    create_txt_from_string(path, string_to_write, replace)
                assert (
                    str(excinfo.value)
                    == f'File {path} already exists and replace is set to False.'
                )

            # Verify if file_exists mock is called correctly
            file_exists_mock.assert_called_with(path)

            if replace and file_exists_mock.return_value:
                # Verify if delete_file mock is called when replace is True
                # and file exists
                delete_file_mock.assert_called_with(path)
            else:
                # Verify delete_file mock is not called in other cases
                delete_file_mock.assert_not_called()


class TestDeleteDir(BaseTest):
    """Tests for delete_dir function."""

    def test_delete_dir(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -rmdir' command by the delete_dir function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test delete_dir with a valid path
        path = '/user/directory'
        command = ['hadoop', 'fs', '-rmdir', path]
        assert delete_dir(path) == _perform(command)


class TestDeleteFile(BaseTest):
    """Tests for delete_file function."""

    def test_delete_file(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -rm' command by the delete_file function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test delete_file with a valid path
        path = '/user/file.txt'
        command = ['hadoop', 'fs', '-rm', path]
        assert delete_file(path) == _perform(command)


class TestFileExits(BaseTest):
    """Tests for file_exists function."""

    def test_file_exists(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -test -e' command by the file_exists function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test file_exists with an existing file
        path = '/user/file.txt'
        command = ['hadoop', 'fs', '-test', '-e', path]
        assert file_exists(path) == _perform(command)

        # Test case 2: Test file_exists with a non-existing file
        non_existing_path = '/user/non_existing_file.txt'
        non_existing_command = ['hadoop', 'fs', '-test', '-e', non_existing_path]
        assert file_exists(non_existing_path) == _perform(non_existing_command)


class TestDateModified(BaseTest):
    """Tests for get_date_modified function."""

    def test_get_date_modified(self, mock_subprocess_popen_date_modifed):
        """Verify proper execution of 'hadoop fs -stat %y' command by the get_date_modified function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case: Test get_date_modified with a valid path
        filepath = '/user/file.txt'
        command_mock = mock_subprocess_popen_date_modifed.return_value
        stdout_mock = command_mock.stdout
        stdout_mock.read.return_value.decode.return_value.__getitem__.return_value = (
            '2023-05-25'
        )
        expected_output = '2023-05-25'
        assert get_date_modified(filepath) == expected_output


class TestIsDir(BaseTest):
    """Tests for isdir function."""

    def test_isdir(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -test -d' command by the isdir function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test isdir with an existing directory
        path = '/user/directory'
        command = ['hadoop', 'fs', '-test', '-d', path]
        assert isdir(path) == _perform(command)

        # Test case 2: Test isdir with a non-existing directory
        non_existing_path = '/user/non_existing_directory'
        non_existing_command = ['hadoop', 'fs', '-test', '-d', non_existing_path]
        assert isdir(non_existing_path) == _perform(non_existing_command)


class TestMoveLocalToHDFS(BaseTest):
    """Tests for move_local_to_hdfs function."""

    def test_move_local_to_hdfs(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -moveFromLocal' command by move_local_to_hdfs.

        Checks if the command is correctly constructed based on the provided arguments.
        """
        # Test case 1: Test move_local_to_hdfs without overwrite option
        from_path = '/local/path/file.txt'
        to_path = '/user/hdfs/path/file.txt'
        command = ['hadoop', 'fs', '-moveFromLocal', from_path, to_path]
        assert move_local_to_hdfs(from_path, to_path) == _perform(command)

        # Test case 2: Test move_local_to_hdfs with overwrite option
        overwrite_from_path = '/local/path/file.txt'
        overwrite_to_path = '/user/hdfs/path/file.txt'
        overwrite_command = [
            'hadoop',
            'fs',
            '-moveFromLocal',
            overwrite_from_path,
            overwrite_to_path,
        ]
        assert move_local_to_hdfs(overwrite_from_path, overwrite_to_path) == _perform(
            overwrite_command,
        )


class TestReadDir(BaseTest):
    """Tests for read_dir function."""

    def test_read_dir(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -ls' command by the read_dir function.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test read_dir with a valid path
        path = '/user/directory'
        ls = subprocess.Popen(['hadoop', 'fs', '-ls', path], stdout=subprocess.PIPE)
        expected_files = [
            line.decode('utf-8').split()[-1]
            for line in ls.stdout
            if 'Found' not in line.decode('utf-8')
        ]
        assert read_dir(path) == expected_files


class TestReadDirFiles(BaseTest):
    """Tests for read_dir_files function."""

    def test_read_dir_files(self, mock_subprocess_popen):
        """Verify proper extraction of filenames from paths by the read_dir_files function."""
        # Test case 1: Test read_dir_files with a valid path
        path = '/user/directory'
        expected_files = [Path(p).name for p in read_dir(path)]
        assert read_dir_files(path) == expected_files


class TestReadDirFilesRecursive(BaseTest):
    """Tests for read_dir_files_recursive function."""

    def test_read_dir_files_recursive(self, mock_subprocess_popen):
        """Verify proper execution of 'hadoop fs -ls -R' command by read_dir_files_recursive.

        Checks if the command is correctly constructed based on the provided path.
        """
        # Test case 1: Test read_dir_files_recursive without return_path option
        path = '/user/directory'
        command = subprocess.Popen(
            f"hadoop fs -ls -R {path} | grep -v ^d | tr -s ' ' | cut -d ' ' -f 8-",
            stdout=subprocess.PIPE,
            shell=True,
        )
        expected_files = [
            obj.decode('utf-8') for obj in command.stdout.read().splitlines()
        ]
        assert read_dir_files_recursive(path) == expected_files

        # Test case 2: Test read_dir_files_recursive with return_path option
        return_path = True
        return_path_files = [
            obj.decode('utf-8') for obj in command.stdout.read().splitlines()
        ]
        assert read_dir_files_recursive(path, return_path) == return_path_files


class TestRename(BaseTest):
    """Tests for rename function."""

    def test_rename(self, mock_subprocess_popen):
        """Verify proper execution of the 'hadoop fs -mv' command by the rename function.

        Checks if the command is correctly constructed based on the provided arguments.
        """
        # Test case 1: Test rename without overwrite option
        from_path = '/user/old_file.txt'
        to_path = '/user/new_file.txt'
        command = ['hadoop', 'fs', '-mv', from_path, to_path]
        assert rename(from_path, to_path) == _perform(command)

        # Test case 2: Test rename with overwrite option
        overwrite_from_path = '/user/old_file.txt'
        overwrite_to_path = '/user/new_file.txt'
        overwrite_command = [
            'hadoop',
            'fs',
            '-mv',
            overwrite_from_path,
            overwrite_to_path,
        ]
        assert rename(
            overwrite_from_path,
            overwrite_to_path,
            overwrite=True,
        ) == _perform(overwrite_command)
