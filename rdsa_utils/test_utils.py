"""Functions and fixtures used with test suites."""

import datetime
import logging
from typing import List, Optional, Tuple

import pandas as pd
import pytest
from _pytest.mark.structures import MarkDecorator
from pyspark.sql import SparkSession


def suppress_py4j_logging():
    """Suppress spark logging."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="module")
def spark_session():
    """Set up spark session fixture."""
    suppress_py4j_logging()

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("rdsa_test_context")
        .config("spark.sql.shuffle.partitions", 1)
        # This stops progress bars appearing in the console whilst running
        .config("spark.ui.showConsoleProgress", "false")
        # .config('spark.sql.execution.arrow.enabled', 'true')
        .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
        .config("spark.workerEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
        .enableHiveSupport()
        .getOrCreate()
    )
    yield spark
    spark.stop()


class Case:
    """Container for a test case, with optional test ID.

    The Case class is to be used in conjunction with `parameterize_cases`.

    Attributes
    ----------
        label
            Optional test ID. Will be displayed for each test when
            running `pytest -v`.
        marks
            Optional pytest marks to denote any tests to skip etc.
        kwargs
            Parameters used for the test cases.

    Examples
    --------
    ```python
    >>> Case(label="some test name", foo=10, bar="some value")

    >>> Case(
    >>>     label="some test name",
    >>>     marks=pytest.mark.skip(reason='not implemented'),
    >>>     foo=10,
    >>>     bar="some value"
    >>> )
    ```

    See Also
    --------
    Modified from https://github.com/ckp95/pytest-parametrize-cases to allow
    pytest mark usage.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        marks: Optional[MarkDecorator] = None,
        **kwargs,
    ):
        """Initialise objects."""
        self.label = label
        self.kwargs = kwargs
        self.marks = marks
        # Makes kwargs accessible with dot notation.
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        """Return string."""
        return f"Case({self.label!r}, **{self.kwargs!r})"


def parametrize_cases(*cases: Case):
    """More user friendly parameterize cases testing.

    Utilise as a decorator on top of test function.

    Examples
    --------
    ```python
    @parameterize_cases(
        Case(
            label="some test name",
            foo=10,
            bar="some value"
        ),
        Case(
            label="some test name #2",
            foo=20,
            bar="some other value"
        ),
    )
    def test(foo, bar):
        ...
    ```

    See Also
    --------
    Source: https://github.com/ckp95/pytest-parametrize-cases
    """
    all_args = set()
    for case in cases:
        if not isinstance(case, Case):
            msg = f"{case!r} is not an instance of Case"
            raise TypeError(msg)

        all_args.update(case.kwargs.keys())

    argument_string = ",".join(sorted(all_args))

    case_list = []
    ids_list = []
    for case in cases:
        case_kwargs = case.kwargs.copy()
        args = case.kwargs.keys()

        # Make sure all keys are in each case, otherwise initialise with None.
        diff = {k: None for k in set(all_args) - set(args)}
        case_kwargs.update(diff)

        case_tuple = tuple(value for key, value in sorted(case_kwargs.items()))

        # If marks are given, wrap the case tuple.
        if case.marks:
            case_tuple = pytest.param(*case_tuple, marks=case.marks)

        case_list.append(case_tuple)
        ids_list.append(case.label)

    if len(all_args) == 1:
        # otherwise it gets passed to the test function as a singleton tuple
        case_list = [i[0] for i in case_list]

    return pytest.mark.parametrize(
        argnames=argument_string,
        argvalues=case_list,
        ids=ids_list,
    )


def create_dataframe(data: List[Tuple[str]], **kwargs) -> pd.DataFrame:
    """Create pandas df from tuple data with a header."""
    return pd.DataFrame.from_records(data[1:], columns=data[0], **kwargs)


def to_date(dt: str) -> datetime.date:
    """Convert date string to datetime.date type."""
    return pd.to_datetime(dt).date()


def to_datetime(dt: str) -> datetime.datetime:
    """Convert datetime string to datetime.datetime type."""
    return pd.to_datetime(dt).to_pydatetime()


@pytest.fixture()
def create_spark_df(spark_session):
    """Create Spark DataFrame from tuple data with first row as schema.

    Example:
    -------
    create_spark_df([
        ('column1', 'column2', 'column3'),
        ('aaaa', 1, 1.1)
    ])

    Can specify the schema alongside the column names:
    create_spark_df([
        ('column1 STRING, column2 INT, column3 DOUBLE'),
        ('aaaa', 1, 1.1)
    ])
    """

    def _(data):
        return spark_session.createDataFrame(data[1:], schema=data[0])

    return _


@pytest.fixture()
def to_spark(spark_session):
    """Convert pandas df to spark."""

    def _(df: pd.DataFrame, *args, **kwargs):
        return spark_session.createDataFrame(df, *args, **kwargs)

    return _
