# Contribution Guide

We welcome contributions. To contribute, please follow these guidelines:

## Pull Requests

- All pull requests should be made to the `dev` branch, named `dev_<feature_name>`.
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

## Issues

If you find a bug or would like to request a feature, please open an issue on the project's [GitHub page](https://github.com/ONSdigital/rdsa-utils/issues). When opening an issue, please provide as much detail as possible, including:

- A clear and descriptive title.
- A description of the problem you're experiencing, including steps to reproduce it.
- Any error messages or logs related to the issue.
- Your operating system and Python version (if relevant).

Please search through the existing issues before opening a new one to avoid duplicates. If you find an existing issue that covers your problem, please add any additional information as a comment. Issues will be triaged and prioritized by the project maintainers.

If you would like to contribute to the project by fixing an existing issue, please leave a comment on the issue to let the maintainers know that you are working on it.

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

We use a traditional `setup.py` approach for managing dependencies. To set up your development environment, first, ensure you have Python 3.8 to 3.10 installed.

Then, to install the package in editable mode along with all development dependencies, run the following command:

```bash
pip3 install -e .[dev]
```

The `-e` (or `--editable`) option is used to install the package in a way that allows you to modify the source code and see the changes directly without having to reinstall the package. This is particularly useful for development.

### Activate the Virtual Environment

It's recommended to use a virtual environment to manage dependencies. If you are using `venv` or a similar tool, activate your virtual environment as usual.

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
