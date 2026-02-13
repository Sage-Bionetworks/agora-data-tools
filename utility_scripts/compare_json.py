#!/usr/bin/env python3
"""
Compare two JSON files for content equality, ignoring order.

This utility script compares two JSON files and determines if they contain the same
data, regardless of the ordering of dictionary keys or list items. This is particularly
useful for validating ETL pipeline outputs where data may be processed in different
orders but should contain identical information.

Features:
    - Normalizes JSON structures for order-independent comparison
    - Intelligently sorts lists of dictionaries by common identifier keys
    - Provides detailed statistics about both files
    - Shows specific differences when files don't match
    - Handles nested data structures recursively

Usage:
    # Modify the file1 and file2 paths in the __main__ block, then run:
    python compare_json.py

    # Exit codes:
    # 0 - Files are identical
    # 1 - Files differ

Example:
    file1 = "output/data_v1.json"
    file2 = "output/data_v2.json"
    result = compare_json_files(file1, file2)
    # Prints detailed comparison results and statistics
"""

import json
import sys
from typing import Any


def normalize_json(obj: Any) -> Any:
    """
    Recursively normalize JSON object for order-independent comparison.

    This function transforms a JSON object (dict, list, or primitive) into a normalized
    form where dictionaries are sorted by keys and lists are sorted in a consistent manner.
    This allows for accurate comparison of JSON data regardless of the order in which
    elements appear.

    Args:
        obj: Any JSON-serializable object (dict, list, str, int, float, bool, None)

    Returns:
        A normalized version of the input object:
        - Dicts: Recursively sorted by keys
        - Lists of dicts: Sorted by common identifier keys (e.g., ensembl_gene_id, id)
        - Lists of primitives: Kept in original order (order may be meaningful)
        - Primitives: Returned unchanged

    Sorting Strategy for Lists of Dicts:
        1. First, tries to find a common identifier key that exists in all items
           (checks: ensembl_gene_id, id, name, tissue, age in that order)
        2. If found, sorts the list by that key's string representation
        3. If no common key exists, sorts by the JSON string representation of each item
        4. If sorting fails (e.g., unhashable types), keeps original order

    Examples:
        >>> normalize_json({"b": 2, "a": 1})
        {'a': 1, 'b': 2}

        >>> normalize_json([{"id": 2, "val": "x"}, {"id": 1, "val": "y"}])
        [{'id': 1, 'val': 'y'}, {'id': 2, 'val': 'x'}]
    """
    if isinstance(obj, dict):
        # Sort dictionary items by key for consistent ordering
        # Recursively normalize all values
        return {k: normalize_json(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        # Handle lists based on their content type
        # For lists of dicts, try to sort by a consistent key if possible
        if all(isinstance(item, dict) for item in obj):
            # Try to find a consistent identifier key for sorting
            try:
                # Common keys that might uniquely identify entries in our data
                # These are checked in priority order
                for key in ["ensembl_gene_id", "id", "name", "tissue", "age"]:
                    if all(key in item for item in obj):
                        # Found a common key - sort by this key's value
                        return sorted(
                            [normalize_json(item) for item in obj],
                            key=lambda x: str(x.get(key, "")),
                        )
                # If no consistent key found, convert each item to JSON string
                # and sort by the string representation
                return sorted(
                    [json.dumps(normalize_json(item), sort_keys=True) for item in obj]
                )
            except (TypeError, KeyError):
                # Sorting failed - fall through to return normalized but unsorted list
                pass
        # For non-dict lists or if sorting failed, just normalize each item
        # but keep the original order (order might be semantically meaningful)
        return [normalize_json(item) for item in obj]
    else:
        # Primitive types (str, int, float, bool, None) are returned as-is
        return obj


def compare_json_files(file1: str, file2: str) -> bool:
    """
    Compare two JSON files for content equality, ignoring order differences.

    This function performs a comprehensive comparison of two JSON files, providing:
    - Basic statistics about each file (type, length, structure)
    - Order-independent content comparison
    - Detailed difference reporting when files don't match

    The comparison process:
    1. Loads both JSON files
    2. Displays statistics about each file's structure
    3. Performs quick sanity checks (same type, same length for lists)
    4. Normalizes both data structures for order-independent comparison
    5. Compares normalized structures
    6. If different, attempts to identify specific differences

    Args:
        file1: Path to the first JSON file to compare
        file2: Path to the second JSON file to compare

    Returns:
        bool: True if files contain identical data (ignoring order), False otherwise

    Prints:
        - Loading progress messages
        - File statistics (type, length, sample keys)
        - Comparison results (SUCCESS or MISMATCH)
        - Detailed difference information when files differ

    Raises:
        FileNotFoundError: If either file doesn't exist
        json.JSONDecodeError: If either file contains invalid JSON

    Examples:
        >>> compare_json_files("output1.json", "output2.json")
        Loading output1.json...
        Loading output2.json...
        File 1 stats:
          Type: <class 'list'>
          Length: 100 items
        ...
        SUCCESS: Files are identical!
        True
    """
    # Load the first JSON file
    print(f"Loading {file1}...")
    with open(file1, "r") as f1:
        data1 = json.load(f1)

    # Load the second JSON file
    print(f"Loading {file2}...")
    with open(file2, "r") as f2:
        data2 = json.load(f2)

    # Display statistics about the first file
    print("\nFile 1 stats:")
    print(f"  Type: {type(data1)}")
    if isinstance(data1, list):
        print(f"  Length: {len(data1)} items")
        if data1:
            print(
                f"  First item keys: {list(data1[0].keys()) if isinstance(data1[0], dict) else 'N/A'}"
            )

    # Display statistics about the second file
    print("\nFile 2 stats:")
    print(f"  Type: {type(data2)}")
    if isinstance(data2, list):
        print(f"  Length: {len(data2)} items")
        if data2:
            print(
                f"  First item keys: {list(data2[0].keys()) if isinstance(data2[0], dict) else 'N/A'}"
            )

    # Quick sanity check: both files must have the same root type
    if type(data1) is not type(data2):
        print(f"\nMISMATCH: Different types - {type(data1)} vs {type(data2)}")
        return False

    # For lists, check that they have the same number of items
    if isinstance(data1, list) and len(data1) != len(data2):
        print(f"\nMISMATCH: Different lengths - {len(data1)} vs {len(data2)}")
        return False

    # Normalize both data structures for order-independent comparison
    print("\nNormalizing and comparing content...")
    norm1 = normalize_json(data1)
    norm2 = normalize_json(data2)

    # Convert normalized structures to JSON strings for exact comparison
    json1 = json.dumps(norm1, sort_keys=True)
    json2 = json.dumps(norm2, sort_keys=True)

    # Compare the JSON strings
    if json1 == json2:
        print("\nSUCCESS: Files are identical!")
        return True
    else:
        print("\nMISMATCH: Files have different content")

        # If both are lists, provide detailed difference analysis
        if isinstance(data1, list) and isinstance(data2, list):
            print("\nDetailed comparison of list items:")
            # Create sets of normalized JSON strings for set-based comparison
            set1 = {json.dumps(normalize_json(item), sort_keys=True) for item in data1}
            set2 = {json.dumps(normalize_json(item), sort_keys=True) for item in data2}

            # Find items that appear in one file but not the other
            only_in_1 = set1 - set2
            only_in_2 = set2 - set1

            # Report items unique to file 1
            if only_in_1:
                print(f"  Items only in file1: {len(only_in_1)}")
                if len(only_in_1) <= 3:
                    # Show up to 3 examples (truncated to 200 chars)
                    for item in list(only_in_1)[:3]:
                        print(f"    {item[:200]}...")

            # Report items unique to file 2
            if only_in_2:
                print(f"  Items only in file2: {len(only_in_2)}")
                if len(only_in_2) <= 3:
                    # Show up to 3 examples (truncated to 200 chars)
                    for item in list(only_in_2)[:3]:
                        print(f"    {item[:200]}...")

        return False


if __name__ == "__main__":
    # Configuration: Specify the two JSON files to compare
    # Modify these paths as needed for your comparison
    file1 = "staging/rna_de_individual/rna_de_individual.json"
    file2 = "staging/rna_de_individual_CORRECT.json"

    # Perform the comparison
    result = compare_json_files(file1, file2)

    # Exit with appropriate status code
    # 0 = success (files are identical)
    # 1 = failure (files differ)
    sys.exit(0 if result else 1)
