import pytest
from chispa import assert_approx_df_equality

from rdsa_utils.methods.averaging_methods import *


@pytest.fixture()
def input_df(create_spark_df):
    """Fixture containing input data for tests."""
    return create_spark_df(
        [
            ("price", "quantity", "weight"),
            (0.7, 1, 0.090909091),
            (1.0, 5, 0.454545455),
            (1.5, 3, 0.272727273),
            (1.4, 2, 0.181818182),
        ],
    )


class TestWeightedArithmeticAverage:
    """Test for the weighted arithmetic average."""

    def test_expected(self, input_df, create_spark_df):
        """Test expected output."""
        actual = input_df.agg(
            weighted_arithmetic_average("price", "weight").alias("average"),
        )
        expected = create_spark_df(
            [
                ("average",),
                (1.1818182,),
            ],
        )
        assert_approx_df_equality(
            actual,
            expected,
            precision=1e-7,
            ignore_nullable=True,
        )


class TestWeightedGeometricAverage:
    """Test for the weighted geometric average."""

    def test_expected(self, input_df, create_spark_df):
        """Test expected output."""
        actual = input_df.agg(
            weighted_geometric_average("price", "weight").alias("average"),
        )
        expected = create_spark_df(
            [
                ("average",),
                (1.1495070,),
            ],
        )
        assert_approx_df_equality(
            actual,
            expected,
            precision=1e-7,
            ignore_nullable=True,
        )


class TestWUnweightedArithmeticAverage:
    """Test for the unweighted arithmetic average."""

    def test_expected(self, input_df, create_spark_df):
        """Test expected output."""
        actual = input_df.agg(unweighted_arithmetic_average("price").alias("average"))
        expected = create_spark_df(
            [
                ("average",),
                (1.15,),
            ],
        )
        assert_approx_df_equality(
            actual,
            expected,
            precision=1e-7,
            ignore_nullable=True,
        )


class TestWUnweightedGeometricAverage:
    """Test for the unweighted geometric average."""

    def test_expected(self, input_df, create_spark_df):
        """Test expected output."""
        actual = input_df.agg(unweighted_geometric_average("price").alias("average"))
        expected = create_spark_df(
            [
                ("average",),
                (1.1011065,),
            ],
        )
        assert_approx_df_equality(
            actual,
            expected,
            precision=1e-7,
            ignore_nullable=True,
        )


@pytest.mark.skip(reason="test shell")
class TestGetWeightShares:
    """Tests for the _get_weight_shares function."""

    def test_expected(self):
        """Test expected functionality."""
        input_df = create_spark_df(
            [
                ("group", "quantity"),
                ("first", 1),
                ("first", 5),
                ("first", 3),
                ("first", 2),
                ("second", 2),
                ("second", 6),
                ("second", 4),
                ("second", 3),
            ],
        )

        expected = create_spark_df(
            [
                ("group", "quantity", "weight"),
                ("first", 1, 0.090909091),
                ("first", 5, 0.454545455),
                ("first", 3, 0.272727273),
                ("first", 2, 0.181818182),
                ("second", 2, 0.133333333),
                ("second", 6, 0.4),
                ("second", 4, 0.266666666),
                ("second", 3, 0.2),
            ],
        )

        weights = get_weight_shares(
            weights="quantity",
            levels="group",
        )
        actual = input_df.withColumn("weight", weights)
        assert_approx_df_equality(
            actual,
            expected,
            precision=1e-7,
            ignore_nullable=True,
        )
