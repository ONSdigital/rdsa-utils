# Contribution Guide

We welcome contributions. To contribute, please follow these guidelines:

## Pull Requests

- All pull requests should be made to the `development` branch. Please use the following
**Naming Conventions** for your **branches**:
    - **Feature Branches**: `feature-<feature_name>` for introducing new features.
    - **Bug Fixes**: `fix-<bug_description>` for resolving bugs.
    - **Hotfixes**: `hotfix-<issue>` for urgent fixes that go straight to production.
    - **Improvements/Refactors**: `refactor-<description>` or `improvement-<description>` for code improvements.
    - **Documentation**: `docs-<change_description>` for updates to documentation.
    - **Experimental**: `experiment-<experiment_name>` for trial and exploratory work.
- Please make sure that your code passes all unit tests before submitting a pull request.
- Include unit tests with your code changes whenever possible, preferably written in [pytest](https://docs.pytest.org/en/stable/) format.
- Make sure that all existing unit tests still pass with your code changes.
- Please ensure that your code is compliant with the project's coding style guidelines, which include:
  - Writing docstrings in [Scipy/numpy style format](https://numpydoc.readthedocs.io/en/latest/format.html).
  - Using [type hints](https://docs.python.org/3/library/typing.html) in Python functions.
  - Adhering to the [PEP 8 style guide](https://www.python.org/dev/peps/pep-0008/) for Python code.
  - No wildcard imports.
  - Import PySpark functions as `F`.
  - Providing well-documented and easy-to-understand code, including clear variable and function names, as well as explanatory comments where necessary.
- If you are making a significant change to the codebase, please make sure to update the documentation to reflect the changes.
- If you are adding new functionality, please provide examples of how to use it in the project's documentation or in a separate README file.
- If you are fixing a bug, please include a description of the bug and how your changes address it.
- If you are adding a new dependency, please include a brief explanation of why it is necessary and what it does.
- If you are making significant changes to the project's architecture or design, please discuss your ideas with the project maintainers first to ensure they align with the project's goals and vision.

---

## House Style for Example Functions and Unit Tests

### Function House Style

When writing functions in `rdsa-utils`, please follow these guidelines to
ensure clarity, consistency, and ease of use for others:

- Use **type hints** in the function signature to clearly indicate input and
  output types. Avoid repeating type information in the docstring — this reduces
  duplication and keeps documentation clean.
- Docstrings must follow the **Numpy/Scipy format**, with clearly
  defined `Parameters`, `Returns`, and an `Examples` section.
- Try to include an **Examples** section showing how the function can be
  used in practice. These examples should be minimal, runnable, and
  demonstrate typical use cases.
- Use clear, descriptive, and self-explanatory parameter and variable
  names — avoid overly abbreviated or cryptic names.
- If a function is complex, split it into smaller helper functions
  where possible, and ensure that each part has a clear purpose.
- Keep explanatory comments focused and purposeful. Comment on the
  *why* rather than the *what* when the code’s intent isn't obvious.
- Ensure the function is readable, maintainable, and easy for others
  to understand — readability is preferred over overly clever one-liners.

#### Example:

```python
def delete_file(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
    overwrite: bool = False,
) -> bool:
    """Delete a file from an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the bucket from which the file will be deleted.
    object_name
        The S3 object name of the file to delete.
    overwrite
        If False, the function will not delete the file if it does not exist;
        set to True to ignore non-existence on delete.

    Returns
    -------
    bool
        True if the file was deleted successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> delete_file(client, 'mybucket', 'folder/s3_file.txt')
    True
    """
```

#### ✅ Function Writing Checklist

| Requirement                                   | Check before submitting                                              |
|-----------------------------------------------|----------------------------------------------------------------------|
| **Type hints** used in function signature     | Clearly specify input and output types; no duplication in docstring |
| **Docstring format**                          | Follow Numpy/Scipy style with `Parameters`, `Returns`, `Examples`   |
| **Examples section** included                 | Example is runnable, minimal, and shows common usage                |
| Parameter and variable names are descriptive  | Avoid cryptic or overly abbreviated names                            |
| Complex logic is broken into helpers          | Keep functions focused and small where possible                      |
| Comments explain *why*, not *what*            | Only where the intent isn’t obvious                                  |
| Code prioritises readability                  | Prefer clear structure over clever one-liners                        |

### Unit Test House Style

- **Use test classes** with clear, one-line docstrings explaining the
  purpose of the class, where possible. For more complex test classes,
  a short descriptive multi-line docstring is acceptable.
- **Write test functions** with concise, descriptive names and one-line docstrings
  explaining the scenario being tested, where possible.
  Longer explanations are fine if needed.
- Write separate test methods for each scenario.
- Use assertions that clearly show expected vs. actual results.
- Aim for full test coverage, including edge cases to ensure robustness
  and handle unexpected input.

#### Example:

```python
class TestParseYaml:
    """Tests for parse_yaml function."""

    def test_expected(
        self,
        yaml_config_string,
        expected_standard_config,
    ):
        """Test expected functionality."""
        actual = parse_yaml(yaml_config_string)

        assert actual == expected_standard_config

    def test_invalid_yaml_string(self):
        """Test that invalid YAML input raises YAMLError."""
        with pytest.raises(YAMLError):
            parse_yaml('invalid: [unclosed_list')
```

#### ✅ Testing Checklist

| Requirement                                   | Check before submitting                                              |
|-----------------------------------------------|----------------------------------------------------------------------|
| **Test classes** used where applicable        | One-line descriptive docstring where possible, multi-line if needed  |
| **Test functions** are clear and focused      | One scenario per test, descriptive function names                    |
| Assertions are clear and meaningful           | Shows expected vs. actual results                                    |
| Edge cases are covered                        | Aim for full coverage, not just the happy path                       |
| Parameterised tests used if helpful           | Reduce duplication for similar test scenarios                        |

---

## Issues

If you find a bug or would like to request a feature, please open an issue on the project's [GitHub page](https://github.com/ONSdigital/rdsa-utils/issues). When opening an issue, please provide as much detail as possible, including:

- A clear and descriptive title.
- A description of the problem you're experiencing, including steps to reproduce it.
- Any error messages or logs related to the issue.
- Your operating system and Python version (if relevant).

Please search through the existing issues before opening a new one to avoid duplicates. If you find an existing issue that covers your problem, please add any additional information as a comment. Issues will be triaged and prioritized by the project maintainers.

If you would like to contribute to the project by fixing an existing issue, please leave a comment on the issue to let the maintainers know that you are working on it.

---

## Getting Started

### Installing Python

Before getting started, you need to have Python 3.8 or higher installed on your system. You can download Python from the [official website](https://www.python.org/downloads/). Make sure to add Python to your `PATH` during the installation process.

Alternatively, you can use [Anaconda](https://www.anaconda.com/download) to create a Python 3.8 or higher virtual environment. Anaconda is a popular Python distribution that comes with many pre-installed scientific computing packages and tools. Here's how to create a new environment with Anaconda:

1. Download and install Anaconda from the [official website](https://www.anaconda.com/download).
2. Open the Anaconda prompt.
3. Create a new virtual environment with Python 3.8 or higher:

    ```
    conda create --name myenv python=3.8
    ```

4. Activate the virtual environment:

    ```
    conda activate myenv
    ```

### Clone the Repository

Clone the repository to your local machine:

```
git clone https://github.com/ONSdigital/rdsa-utils.git
cd rdsa-utils
```

### Set Up the Development Environment

We use a traditional `setup.py` approach for managing dependencies. To set up your development environment, first, ensure you have Python 3.8 to 3.13 installed.

Then, to install the package in editable mode along with all development dependencies, run the following command:

```bash
pip3 install -e .[dev]
```

The `-e` (or `--editable`) option is used to install the package in a way that allows you to modify the source code and see the changes directly without having to reinstall the package. This is particularly useful for development.

### Running Tests

To run tests, ensure you're in the top-level directory of the project and execute:

```bash
pytest
```

This will run all the tests using the configurations set in the project.

### Installing Pre-commit Hooks in Your Development Environment

Pre-commit hooks are used to automate checks and formatting before commits. Follow these steps to set them up:

#### Installation Steps

1. **Install pre-commit**: If you haven't already, install the pre-commit package:

```bash
pip install pre-commit
```

2. **Install pre-commit hooks**: Install the hooks defined in `.pre-commit-config.yaml`:

```bash
pre-commit install
```

   This sets up the hooks to run automatically before each commit.

#### Usage

The pre-commit hooks will automatically run on your modified files whenever you commit. To manually run all hooks on all files, use:

```bash
pre-commit run --all-files
```

This can be useful for checking your codebase.

By following these steps, your development environment for `rdsa-utils` will be ready, and you can start contributing to the project with ease.
