"""
Utility functions for preprocessing data to create source files for the explorers.
"""

from pathlib import Path
from typing import Any
from collections.abc import Hashable
from agoradatatools.etl import load, utils

DEFAULT_MODEL_AD_SOURCE_FOLDER_ID = "syn51498054"

DEFAULT_AGORA_CONFIG_FILE = str(Path("configs", "agora_prod.yaml"))
DEFAULT_MODEL_AD_CONFIG_FILE = str(Path("configs", "model_ad_prod.yaml"))

# Output defaults to the staging/ directory unless overridden by config
DEFAULT_STAGING_PATH = "staging"

# Sentinel object used to represent a missing key in nested dictionary searches.
_MISSING_KEY = object()


def get_staging_path(config_path: str) -> str:
    """
    Get the staging path for preprocessing. Defaults to DEFAULT_STAGING_PATH unless overridden by
    the configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        str: The staging folder for output.

    Raises:
        ValueError: If config_path is not provided.
    """
    if not config_path:
        raise ValueError("config_path must be provided")

    config = utils._get_config(config_path=config_path)
    staging_path = config.get("staging_path") or DEFAULT_STAGING_PATH
    load.create_temp_location(staging_path=staging_path)

    return staging_path


def get_nested_dict_item(
    dictionary: dict[Any, Any], key: Hashable, default: Any = None
) -> Any:
    """
    Recursively search for a key in a nested dictionary and return its value. Child values that are dicts or
    lists/tuples of dicts are searched, while other values are ignored.

    This function requires that the query key only exists once in the nested structure. It searches the whole nested
    structure and, if duplicate keys exist, an error will be raised.

    Returns the default value if the key is not found.

    Example:
        nested_dict = {
            "a": {"b": {"c": 10}},
            "d": [
                {"e": [{"f": 20}, {"g": "val1"}]},
                {"h": 30}
            ]
        }
        get_nested_dict_item(nested_dict, "b")  # Returns {'c': 10}
        get_nested_dict_item(nested_dict, "c")  # Returns 10
        get_nested_dict_item(nested_dict, "g")  # Returns "val1"

    Args:
        dictionary (dict[Any, Any]): The nested dictionary to search.
        key (Hashable): The key to search for.
        default (Any, optional): The value to return if the key is not found. Defaults to None.

    Returns:
        Any: The value associated with the key if found, otherwise the default value.

    Raises:
        TypeError: If the 'dictionary' argument is not of type dict.
        ValueError: If duplicate keys are found in the nested dictionary.
    """
    if not isinstance(dictionary, dict):
        raise TypeError(
            f"The 'dictionary' argument must be of type dict, got {type(dictionary).__name__}"
        )

    found_values = []

    # Look at the current set of keys first
    if key in dictionary:
        found_values.append(dictionary[key])

    # Then, recursively search children
    for value in dictionary.values():
        if isinstance(value, dict):
            children = [value]
        elif isinstance(value, list | tuple):
            children = [item for item in value if isinstance(item, dict)]
        else:
            continue

        for child in children:
            # Pass in the _MISSING_KEY object instead of `default` so that we can differentiate
            # between a key not found and a key with a value equal to the default.
            result = get_nested_dict_item(child, key, _MISSING_KEY)
            if result is not _MISSING_KEY:
                found_values.append(result)

    # If we have found any values, found_values should be a list of length 1. If it's longer, there
    # were duplicate keys and an error should be raised. Otherwise, return the single value.
    if found_values:
        if len(found_values) > 1:
            raise ValueError(f"Duplicate keys found for '{key}'")
        return found_values[0]

    # If no children, or key not found in any children, return the default value.
    return default
