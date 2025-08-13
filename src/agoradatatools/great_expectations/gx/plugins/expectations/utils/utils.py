from typing import List, Dict, Any
import json


def safe_parse(value: str) -> List[Dict[str, Any]]:
    """
    Load a JSON string and return a list of dictionaries.
    If the input is not a valid JSON, return an empty list.

    Parameters:
        value[str]: the json string to be parsed. If input is not a valid json string, return an empty list

    Returns:
        Parsed json object or fall back to an empty list
    """
    # Fallback if it's "null"
    if value == "null":
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        else:
            return []
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Invalid JSON string: {value}") from e
