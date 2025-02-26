"""Weighted and unweighted averaging functions."""

from typing import Optional, Sequence, Union

from pyspark.sql import Column as SparkCol
from pyspark.sql import functions as F

from rdsa_utils.helpers.pyspark import get_window_spec


def weighted_arithmetic_average(val: str, weight: str) -> SparkCol:
    """Calculate the weighted arithmetic average.

    This uses convex arithmetic averaging, for the weighted column the values
    must sum to a value of 1.0. Example below:

    mean_class1 = 80
    mean_class2 = 90

    size_class1 = 20
    size_class2 = 30

    x = size_class1 / size_class1+size_class2 = 0.4
    y = size_class2 / size_class1+size_class2 = 0.6

    weighted_average = (x * mean_class1) + (y * mean_class1)

    Parameters
    ----------
    val
        The column name containing the values.
    
    weight
        The column name containing values summing to 1.0.

    Returns
    -------
    SparkCol
    """
    return F.sum(F.col(val) * F.col(weight))


def weighted_geometric_average(val: str, weight: str) -> SparkCol:
    """Calculate the weighted geometric average.
    
    The weighted is the exponent of the natural logarithm of the weighted average.
    
    Example below:

    mean_class1 = 80
    mean_class2 = 90

    size_class1 = 20
    size_class2 = 30

    x = size_class1 / size_class1+size_class2 = 0.4
    y = size_class2 / size_class1+size_class2 = 0.6

    geometric_weighted_average = exp(Log(x * mean_class1) + Log(y * mean_class1))

    Parameters
    ----------
    val
        The column name containing the values.
    
    weight
        The column name containing the weights summing to 1.0.
    
    Returns
    -------
    SparkCol
    """
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
