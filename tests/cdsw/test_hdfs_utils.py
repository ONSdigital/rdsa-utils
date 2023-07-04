import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from rdsa_utils.cdsw.hdfs_utils import (
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


@pytest.fixture
def mock_subprocess_popen(monkeypatch):
    """
    Fixture to mock the subprocess.Popen function.

    This fixture replaces the subprocess.Popen function with a mock implementation.
    The mock implementation returns a MagicMock object that simulates the behavior
    of the Popen object. It sets the returncode to 0 and configures the communicate method
    to return empty byte strings (b"") for stdout and stderr.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        The monkeypatch object provided by the pytest framework.

    Returns
    -------
    None
        This fixture does not return any value, but it patches the subprocess.Popen function.
    """

    def mock_popen(*args, **kwargs):
        process_mock = MagicMock()
        process_mock.returncode = 0
        process_mock.communicate.return_value = (b"", b"")

        return process_mock

    monkeypatch.setattr(subprocess, "Popen", mock_popen)


@pytest.fixture
def mock_subprocess_popen_date_modifed():
    """
    Fixture to mock the subprocess.Popen function for testing get_date_modified function.

    This fixture patches the subprocess.Popen function using the patch decorator from the unittest.mock module.
    It configures the mock implementation to return a MagicMock object for mocking the Popen object.
    The MagicMock object is configured to simulate the behavior of stdout.read() method by returning
    a byte string representing the date "2023-05-25" when called with decode method.

    Yields
    ------
    MagicMock
        The MagicMock object that simulates the behavior of subprocess.Popen.
    """
    with patch("subprocess.Popen") as mock_popen:
        mock_stdout = MagicMock()
        mock_stdout.read.return_value.decode.return_value = "2023-05-25"
        mock_popen.return_value.communicate.return_value = (mock_stdout, None)
        yield mock_popen


def test__perform(mock_subprocess_popen):
    """
    Test _perform function.

    This test checks the behavior of the _perform function when the command execution is successful.
    """
    command = ["ls", "-l", "/home/user"]
    assert _perform(command) == True


def test_change_permissions(mock_subprocess_popen):
    """
    Test change_permissions function.

    This test verifies that the change_permissions function properly constructs and executes the 'hadoop fs -chmod' command.
    It checks if the command is constructed correctly based on the provided arguments.
    """
    # Test case 1: Test change_permissions without recursive option
    path = "/user/example"
    permission = "go+rwx"
    command = ["hadoop", "fs", "-chmod", permission, path]
    assert change_permissions(path, permission) == _perform(command)

    # Test case 2: Test change_permissions with recursive option
    recursive_path = "/user/example"
    recursive_permission = "777"
    recursive_command = [
        "hadoop",
        "fs",
        "-chmod",
        "-R",
        recursive_permission,
        recursive_path,
    ]
    assert change_permissions(
        recursive_path, recursive_permission, recursive=True
    ) == _perform(recursive_command)


def test_copy(mock_subprocess_popen):
    """
    Test copy function.

    This test verifies that the copy function properly constructs and executes the 'hadoop fs -cp' command.
    It checks if the command is constructed correctly based on the provided arguments.
    """
    # Test case 1: Test copy without overwrite option
    from_path = "/user/example/file.txt"
    to_path = "/user/backup/file.txt"
    command = ["hadoop", "fs", "-cp", from_path, to_path]
    assert copy(from_path, to_path) == _perform(command)

    # Test case 2: Test copy with overwrite option
    overwrite_from_path = "/user/example/file.txt"
    overwrite_to_path = "/user/backup/file.txt"
    overwrite_command = [
        "hadoop",
        "fs",
        "-cp",
        "-f",
        overwrite_from_path,
        overwrite_to_path,
    ]
    assert copy(overwrite_from_path, overwrite_to_path, overwrite=True) == _perform(
        overwrite_command
    )


def test_copy_local_to_hdfs(mock_subprocess_popen):
    """
    Test copy_local_to_hdfs function.

    This test verifies that the copy_local_to_hdfs function properly constructs and executes the 'hadoop fs -copyFromLocal' command.
    It checks if the command is constructed correctly based on the provided arguments.
    """
    # Test case 1: Test copy_local_to_hdfs without overwrite option
    from_path = "/local/path/file.txt"
    to_path = "/user/hdfs/path/file.txt"
    command = ["hadoop", "fs", "-copyFromLocal", from_path, to_path]
    assert copy_local_to_hdfs(from_path, to_path) == _perform(command)

    # Test case 2: Test copy_local_to_hdfs with overwrite option
    overwrite_from_path = "/local/path/file.txt"
    overwrite_to_path = "/user/hdfs/path/file.txt"
    overwrite_command = [
        "hadoop",
        "fs",
        "-copyFromLocal",
        "-f",
        overwrite_from_path,
        overwrite_to_path,
    ]
    assert copy_local_to_hdfs(overwrite_from_path, overwrite_to_path) == _perform(
        overwrite_command
    )


def test_create_dir(mock_subprocess_popen):
    """
    Test create_dir function.

    This test verifies that the create_dir function properly constructs and executes the 'hadoop fs -mkdir' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test create_dir with a valid path
    path = "/user/new_directory"
    command = ["hadoop", "fs", "-mkdir", path]
    assert create_dir(path) == _perform(command)


@pytest.fixture
def subprocess_mock():
    """
    Fixture for mocking the subprocess.call function.

    Yields
    ------
    MagicMock
        The mocked subprocess.call function.
    """
    # Mock subprocess.call
    with patch("subprocess.call") as mock:
        yield mock


@pytest.fixture
def file_exists_mock():
    """
    Fixture for mocking the file_exists function from rdsa_utils.cdsw.hdfs_utils module.

    Yields
    ------
    MagicMock
        The mocked file_exists function.
    """
    # Mock file_exists from rdsa_utils.cdsw.hdfs_utils
    with patch("rdsa_utils.cdsw.hdfs_utils.file_exists") as mock:
        yield mock


@pytest.fixture
def delete_file_mock():
    """
    Fixture for mocking the delete_file function from rdsa_utils.cdsw.hdfs_utils module.

    Yields
    ------
    MagicMock
        The mocked delete_file function.
    """
    # Mock delete_file from rdsa_utils.cdsw.hdfs_utils
    with patch("rdsa_utils.cdsw.hdfs_utils.delete_file") as mock:
        yield mock


@pytest.mark.parametrize(
    "path, string_to_write, replace, expected_call",
    [
        (
            "/some/directory/newfile.txt",
            "Hello, world!",
            False,
            ['echo "Hello, world!" | hadoop fs -put - /some/directory/newfile.txt'],
        ),
        (
            "/some/directory/newfile.txt",
            "Hello, world!",
            True,
            ['echo "Hello, world!" | hadoop fs -put - /some/directory/newfile.txt'],
        ),
    ],
)
def test_create_txt_from_string(
    path,
    string_to_write,
    replace,
    expected_call,
    subprocess_mock,
    file_exists_mock,
    delete_file_mock,
):
    """
    Test create_txt_from_string function.

    This test verifies that the create_txt_from_string function properly constructs and executes the 'echo | hadoop fs -put -' command.
    It checks if the command is constructed correctly based on the provided arguments.

    Parameters
    ----------
    path : str
        The path to the new file to be created.
    string_to_write : str
        The string to be written to the file.
    replace : bool
        Flag indicating whether an existing file should be replaced.
    expected_call : str
        The expected subprocess call command.

    subprocess_mock : MagicMock
        The mocked subprocess.call function.
    file_exists_mock : MagicMock
        The mocked file_exists function.
    delete_file_mock : MagicMock
        The mocked delete_file function.

    Raises
    ------
    AssertionError
        If the actual subprocess call is not equal to the expected call or if the delete_file function is not called when expected.
    """
    file_exists_mock.return_value = replace  # Assume file exists if replace is True

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
            == f"File {path} already exists and replace is set to False."
        )

    # Verify if file_exists mock is called correctly
    file_exists_mock.assert_called_with(path)

    if replace and file_exists_mock.return_value:
        # Verify if delete_file mock is called when replace is True and file exists
        delete_file_mock.assert_called_with(path)
    else:
        # Verify delete_file mock is not called in other cases
        delete_file_mock.assert_not_called()


def test_delete_dir(mock_subprocess_popen):
    """
    Test delete_dir function.

    This test verifies that the delete_dir function properly constructs and executes the 'hadoop fs -rmdir' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test delete_dir with a valid path
    path = "/user/directory"
    command = ["hadoop", "fs", "-rmdir", path]
    assert delete_dir(path) == _perform(command)


def test_delete_file(mock_subprocess_popen):
    """
    Test delete_file function.

    This test verifies that the delete_file function properly constructs and executes the 'hadoop fs -rm' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test delete_file with a valid path
    path = "/user/file.txt"
    command = ["hadoop", "fs", "-rm", path]
    assert delete_file(path) == _perform(command)


def test_file_exists(mock_subprocess_popen):
    """
    Test file_exists function.

    This test verifies that the file_exists function properly constructs and executes the 'hadoop fs -test -e' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test file_exists with an existing file
    path = "/user/file.txt"
    command = ["hadoop", "fs", "-test", "-e", path]
    assert file_exists(path) == _perform(command)

    # Test case 2: Test file_exists with a non-existing file
    non_existing_path = "/user/non_existing_file.txt"
    non_existing_command = ["hadoop", "fs", "-test", "-e", non_existing_path]
    assert file_exists(non_existing_path) == _perform(non_existing_command)


def test_get_date_modified(mock_subprocess_popen_date_modifed):
    """
    Test get_date_modified function.

    This test verifies that the get_date_modified function properly constructs and executes the 'hadoop fs -stat %y' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case: Test get_date_modified with a valid path
    filepath = "/user/file.txt"
    command_mock = mock_subprocess_popen_date_modifed.return_value
    stdout_mock = command_mock.stdout
    stdout_mock.read.return_value.decode.return_value.__getitem__.return_value = (
        "2023-05-25"
    )
    expected_output = "2023-05-25"
    assert get_date_modified(filepath) == expected_output


def test_isdir(mock_subprocess_popen):
    """
    Test isdir function.

    This test verifies that the isdir function properly constructs and executes the 'hadoop fs -test -d' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test isdir with an existing directory
    path = "/user/directory"
    command = ["hadoop", "fs", "-test", "-d", path]
    assert isdir(path) == _perform(command)

    # Test case 2: Test isdir with a non-existing directory
    non_existing_path = "/user/non_existing_directory"
    non_existing_command = ["hadoop", "fs", "-test", "-d", non_existing_path]
    assert isdir(non_existing_path) == _perform(non_existing_command)


def test_move_local_to_hdfs(mock_subprocess_popen):
    """
    Test move_local_to_hdfs function.

    This test verifies that the move_local_to_hdfs function properly constructs and executes the 'hadoop fs -moveFromLocal' command.
    It checks if the command is constructed correctly based on the provided arguments.
    """
    # Test case 1: Test move_local_to_hdfs without overwrite option
    from_path = "/local/path/file.txt"
    to_path = "/user/hdfs/path/file.txt"
    command = ["hadoop", "fs", "-moveFromLocal", from_path, to_path]
    assert move_local_to_hdfs(from_path, to_path) == _perform(command)

    # Test case 2: Test move_local_to_hdfs with overwrite option
    overwrite_from_path = "/local/path/file.txt"
    overwrite_to_path = "/user/hdfs/path/file.txt"
    overwrite_command = [
        "hadoop",
        "fs",
        "-moveFromLocal",
        overwrite_from_path,
        overwrite_to_path,
    ]
    assert move_local_to_hdfs(overwrite_from_path, overwrite_to_path) == _perform(
        overwrite_command
    )


def test_read_dir(mock_subprocess_popen):
    """
    Test read_dir function.

    This test verifies that the read_dir function properly constructs and executes the 'hadoop fs -ls' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test read_dir with a valid path
    path = "/user/directory"
    ls = subprocess.Popen(["hadoop", "fs", "-ls", path], stdout=subprocess.PIPE)
    expected_files = [
        line.decode("utf-8").split()[-1]
        for line in ls.stdout
        if "Found" not in line.decode("utf-8")
    ]
    assert read_dir(path) == expected_files


def test_read_dir_files(mock_subprocess_popen):
    """
    Test read_dir_files function.

    This test verifies that the read_dir_files function properly extracts filenames from the list of paths returned by read_dir.
    """
    # Test case 1: Test read_dir_files with a valid path
    path = "/user/directory"
    expected_files = [os.path.basename(path) for path in read_dir(path)]
    assert read_dir_files(path) == expected_files


def test_read_dir_files_recursive(mock_subprocess_popen):
    """
    Test read_dir_files_recursive function.

    This test verifies that the read_dir_files_recursive function properly constructs and executes the 'hadoop fs -ls -R' command.
    It checks if the command is constructed correctly based on the provided path.
    """
    # Test case 1: Test read_dir_files_recursive without return_path option
    path = "/user/directory"
    command = subprocess.Popen(
        f"hadoop fs -ls -R {path} | grep -v ^d | tr -s ' ' | cut -d ' ' -f 8-",
        stdout=subprocess.PIPE,
        shell=True,
    )
    expected_files = [obj.decode("utf-8") for obj in command.stdout.read().splitlines()]
    assert read_dir_files_recursive(path) == expected_files

    # Test case 2: Test read_dir_files_recursive with return_path option
    return_path = True
    return_path_files = [
        obj.decode("utf-8") for obj in command.stdout.read().splitlines()
    ]
    assert read_dir_files_recursive(path, return_path) == return_path_files


def test_rename(mock_subprocess_popen):
    """
    Test rename function.

    This test verifies that the rename function properly constructs and executes the 'hadoop fs -mv' command.
    It checks if the command is constructed correctly based on the provided arguments.
    """
    # Test case 1: Test rename without overwrite option
    from_path = "/user/old_file.txt"
    to_path = "/user/new_file.txt"
    command = ["hadoop", "fs", "-mv", from_path, to_path]
    assert rename(from_path, to_path) == _perform(command)

    # Test case 2: Test rename with overwrite option
    overwrite_from_path = "/user/old_file.txt"
    overwrite_to_path = "/user/new_file.txt"
    overwrite_command = ["hadoop", "fs", "-mv", overwrite_from_path, overwrite_to_path]
    assert rename(overwrite_from_path, overwrite_to_path, overwrite=True) == _perform(
        overwrite_command
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
