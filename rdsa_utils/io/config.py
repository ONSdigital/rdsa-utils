"""Module for code relating to loading config files."""

import copy
import json
import logging
from pathlib import Path
from typing import Callable, Dict, Literal, Optional, Union

from cloudpathlib import CloudPath
from pydantic import BaseModel

from rdsa_utils.exceptions import ConfigError
from rdsa_utils.helpers.python import overwrite_dictionary
from rdsa_utils.io.input import parse_json, parse_toml, parse_yaml, read_file
from rdsa_utils.typing import Config
from rdsa_utils.validation import apply_validation

logger = logging.getLogger(__name__)


class LoadConfig:
    """Class for loading and storing a configuration file.

    Attributes
    ----------
    config
        The loaded config stored as a dictionary.
    config_dir
        The logical parent directory of loaded `config_path`.
    config_file
        The file name of the loaded `config_path`.
    config_original
        The configuration dictionary as initially loaded, prior to applying any
        overrides or validation.
    config_overrides
        The configuration override dictionary, if provided.
    config_path
        The path of the loaded config file.
    config_type
        The file type of the loaded config file.
    config_validators
        The validators used to validate the loaded config, if provided.
    **attrs
        Every top level key in the loaded config is also set as an attribute to
        allow simpler access to each config section.
    """

    def __init__(
        self: "LoadConfig",
        config_path: Union[CloudPath, Path],
        config_overrides: Optional[Config] = None,
        config_type: Optional[Literal["json", "toml", "yaml"]] = None,
        config_validators: Optional[Dict[str, BaseModel]] = None,
    ) -> None:
        """Init method.

        Parameters
        ----------
        config_path
            The path of the config file to be loaded.
        config_overrides, optional
            A dictionary containing a subset of the keys and values of the
            config file that is initially loaded, by default None. If values
            are provided that are not in the initial config then a ConfigError
            is raised.
        config_type, optional
            The file type of the config file being loaded, by default None. If
            not specified then this is inferred from the `config_path`.
        config_validators, optional
            A dictionary made up of key, value pairs where the keys refer to
            the top level sections of the loaded config, and the values are a
            pydantic validation class for the section, by default None. If only
            some of the keys are specified with validators, warnings are raised
            to alert that they have not been validated.
        """
        self.config_path = config_path
        self.config_file = config_path.name
        self.config_dir = config_path.parent

        self.config_overrides = config_overrides
        self.config_type = config_type
        self.config_validators = config_validators

        if not self.config_type:
            self.config_type = self.config_path.suffix.lstrip(".")

        logger.info(f"Loading config from file: {self.config_path}")
        self._config_contents = read_file(self.config_path)

        self.config = self._get_config_parser()(self._config_contents)

        # Save the original config prior to mutating it.
        self.config_original = copy.deepcopy(self.config)
        logger.debug(
            self._print_config_string(
                "loaded config",
                self.config_original,
            ),
        )

        if self.config_overrides:
            try:
                self.config = overwrite_dictionary(
                    self.config,
                    self.config_overrides,
                )
            except ValueError as e:
                raise ConfigError(e) from e

            logger.debug(
                self._print_config_string(
                    "config after applying overrides",
                    self.config,
                ),
            )

        if self.config_validators:
            self._apply_config_validators()
            logger.debug(
                self._print_config_string(
                    "config after applying validation",
                    self.config,
                ),
            )

        logger.info(self._print_config_string("using config", self.config))

        # Assign every key in the config as an attribute in the class. This
        # means all config sections can be accessed via Config.<key>.
        for key, value in self.config.items():
            setattr(self, key, value)

    def _get_config_parser(self: "LoadConfig") -> Callable[[str], Config]:
        """Return the appropriate config parsing function.

        Returns
        -------
        Callable[[str], Config]
            Function that will parse a string into a config object.

        Raises
        ------
        ConfigError
            If the specified config file type does not have a config parser
            implemented.
        """
        readers = {
            "json": parse_json,
            "toml": parse_toml,
            "yaml": parse_yaml,
        }

        if config := readers.get(self.config_type):
            return config
        else:
            msg = f"No config parser present for file type = {self.config_type}"

            logger.error(msg)
            raise ConfigError(msg)

    def _apply_config_validators(self: "LoadConfig") -> None:
        """Apply validators to every key in config.

        Warnings are raised if a key doesn't have a validator.
        """
        for key, values in self.config.items():
            self.config[key] = apply_validation(
                config=values,
                Validator=self.config_validators.get(key),
            )

    def _print_config_string(
        self: "LoadConfig",
        text: str,
        data: Config,
    ) -> str:
        """Build string to use for printing config dictionary.

        Parameters
        ----------
        text
            Any text to be printed before the config.
        data
            The config to be printed

        Returns
        -------
        str
            String with nicely formatted dictionary.
        """
        return f"\n{text}\n{json.dumps(data, indent=4)}"
