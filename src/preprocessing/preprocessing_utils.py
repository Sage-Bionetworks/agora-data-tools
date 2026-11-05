"""
Utility functions for preprocessing data to create source files for the explorers.
"""

import pathlib
from typing import Any

DEFAULT_MODEL_AD_SOURCE_FOLDER_ID = "syn51498054"

DEFAULT_AGORA_CONFIG_FILE = pathlib.Path("configs", "agora_prod.yaml")
DEFAULT_MODEL_AD_CONFIG_FILE = pathlib.Path("configs", "model_ad_prod.yaml")

# Output defaults to the staging/preprocessing directory
DEFAULT_OUTPUT_PATH = pathlib.Path("staging", "preprocessing")


def get_nested_dict_item(
    dictionary: dict[Any, Any], key: str, default: Any = None
) -> Any:
    """
    Recursively search for a key in a nested dictionary and return its value. This function assumes that each child
    item is also a dictionary (lists of dictionaries are not searched). It also assumes that the key only exists once in
    the nested structure. Duplicate keys are not handled and this function will return the first occurrence it finds.

    Returns the default value if the key is not found.

    Example:
        nested_dict = {"a": {"b": {"c": 42}}}
        get_nested_dict_item(nested_dict, "b")  # Returns {'c': 42}
        get_nested_dict_item(nested_dict, "c")  # Returns 42

    Args:
        dictionary (dict[Any, Any]): The nested dictionary to search.
        key (str): The key to search for.
        default (Any, optional): The value to return if the key is not found. Defaults to None.

    Returns:
        Any: The value associated with the key if found, otherwise the default value.
    """
    if key in dictionary:
        return dictionary[key]

    # Use a distinct object to represent a missing value so that we can differentiate between a key
    # not found and a key with a value equal to the default.
    missing = object()

    for value in dictionary.values():
        if isinstance(value, dict):
            result = get_nested_dict_item(value, key, missing)
            if result is not missing:
                return result

    return default
