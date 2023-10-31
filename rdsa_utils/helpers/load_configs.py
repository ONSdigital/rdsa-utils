"""A function for loading config file from local."""
import json
import logging

import yaml

from rdsa_utils.typing import Config, PathLike


logger = logging.getLogger()


def load_config_from_local(config_path: PathLike) -> Config:
    """Load a yaml configuration file from within repo.

    Parameters
    ----------
    config_path
        The path of the config file in a yaml format.

    Returns
    -------
    Config
        The loaded yaml file in a dictionary format.
    """
    logger.info(f"""loading config from file: {config_path}""")

    with open(config_path, 'r') as f:
        config_file = yaml.safe_load(f)

    logger.info(json.dumps(config_file, indent=4))
    return config_file
