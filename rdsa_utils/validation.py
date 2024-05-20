"""Functions that support the use of pydantic validators."""

import json
import logging
import warnings
from typing import Any, Callable, Mapping, Optional

import pandas as pd
from pydantic import BaseModel, validator

from rdsa_utils.helpers.python import list_convert

logger = logging.getLogger(__name__)


def apply_validation(
    config: Mapping[str, Any],
    Validator: Optional[BaseModel],  # noqa: N803
) -> Mapping[str, Any]:
    """Apply validation model to config.

    If no Validator is passed, then a warning will be logged and the input
    config returned without validation. This mechanism is to allow the use of
    this function to aid in tracking config sections that are unvalidated.

    Parameters
    ----------
    config
        The config for validating.
    Validator, optional
        Validator class for the config.

    Returns
    -------
    Mapping[str, Any]
        The input config after being passed through the validator.
    """
    if not Validator:
        msg = "No validator provided, config contents unvalidated."
        logger.warning(msg)
        warnings.warn(msg, stacklevel=2)
        return config

    validated_config = Validator(**config).model_dump(exclude_unset=True)

    logger.info(
        f"""Validated config using {Validator.__name__}:
    {json.dumps(validated_config, indent=4)}
    """,
    )
    return validated_config


def list_convert_validator(*args, **kwargs) -> Callable:  # noqa: ANN002, ANN003
    """Wrapper to set kwargs for list_convert validator."""  # noqa: D401
    decorator = validator(
        *args,
        **kwargs,
        pre=True,  # Run before any other validation.
        always=True,  # Apply even when value is not specified (i.e. None).
        allow_reuse=True,  # Allow validator to be used in many fields/models.
    )
    decorated = decorator(list_convert)
    return decorated


def allowed_date_format(date: str) -> str:
    """Ensure that the date string can be converted to a useable datetime.

    Parameters
    ----------
    date
        The specified date string.

    Returns
    -------
    str
        The input date.

    Raises
    ------
    ValueError
        If the date is not one of the predefined allowed formats.
    """
    pd.to_datetime(date)

    return date
