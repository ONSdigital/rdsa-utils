"""Weighted and unweighted averaging functions."""

from typing import Optional, Sequence, Union

from pyspark.sql import Column as SparkCol
from pyspark.sql import functions as F

from rdsa_utils.helpers.pyspark import get_window_spec


def weighted_arithmetic_average(val: str, weight: str) -> SparkCol:
    """Calculate the weighted arithmetic average."""
    return F.sum(F.col(val) * F.col(weight))


def weighted_geometric_average(val: str, weight: str) -> SparkCol:
    """Calculate the weighted geometric average."""
    return F.exp(F.sum(F.log(F.col(val)) * F.col(weight)))


def unweighted_arithmetic_average(val: str) -> SparkCol:
    """Calculate the unweighted arithmetic average."""
    return F.mean(val)


def unweighted_geometric_average(val: str) -> SparkCol:
    """Calculate the unweighted geometric average."""
    return F.exp(F.mean(F.log(val)))


def get_weight_shares(
    weights: str,
    levels: Optional[Union[str, Sequence[str]]] = None,
) -> SparkCol:
    """Divide weights by sum of weights for each group."""
    return F.col(weights) / F.sum(F.col(weights)).over(get_window_spec(levels))
