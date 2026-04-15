#!/usr/bin/env python3
"""
Compare two JSON files for content equality, ignoring order.

This utility script compares two JSON files and determines if they contain the same
data, regardless of the ordering of dictionary keys or list items. This is particularly
useful for validating ETL pipeline outputs where data may be processed in different
orders but should contain identical information.

Features:
    - Fast hash-based comparison optimized for large files (100k+ objects)
    - Provides detailed statistics about both files
    - Shows specific differences when files don't match
    - Memory-efficient comparison using MD5 hashes

Usage:
    python compare_json.py <file1> <file2>

    # Exit codes:
    # 0 - Files are identical
    # 1 - Files differ

Example:
    python compare_json.py output/data_v1.json output/data_v2.json
"""

import argparse
import json
import sys
import hashlib
from typing import Any


def object_hash(obj: Any) -> str:
    """
    Create a hash for a JSON object for fast comparison.

    This function creates an MD5 hash of the JSON representation of an object,
    with keys sorted for consistency. This allows for fast comparison of objects
    without needing to normalize entire data structures.

    Args:
        obj: Any JSON-serializable object (dict, list, str, int, float, bool, None)

    Returns:
        str: MD5 hash of the JSON representation of the object

    Examples:
        >>> object_hash({"b": 2, "a": 1})
        'some_hash_value'

        >>> object_hash({"a": 1, "b": 2})  # Same hash as above
        'some_hash_value'
    """
    return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def compare_json_files(file1: str, file2: str) -> bool:
    """
    Compare two JSON files for content equality using efficient hash-based comparison.

    This function performs a comprehensive comparison of two JSON files, optimized
    for large files (hundreds of thousands of objects). It uses MD5 hashing for
    fast comparison without needing to normalize entire data structures.

    The comparison process:
    1. Loads both JSON files
    2. Displays statistics about each file's structure
    3. Performs quick sanity checks (same type, same length for lists)
    4. Creates hash sets for efficient comparison
    5. Compares hash sets to identify differences
    6. Reports detailed statistics and sample differences

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
        print(f"  Length: {len(data1):,} items")
        if data1:
            print(
                f"  First item keys: {list(data1[0].keys()) if isinstance(data1[0], dict) else 'N/A'}"
            )

    # Display statistics about the second file
    print("\nFile 2 stats:")
    print(f"  Type: {type(data2)}")
    if isinstance(data2, list):
        print(f"  Length: {len(data2):,} items")
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
        print(f"\nMISMATCH: Different lengths - {len(data1):,} vs {len(data2):,}")
        return False

    # For non-list types (dict, primitives), do direct comparison
    if not isinstance(data1, list):
        print("\nComparing content...")
        if data1 == data2:
            print("\nSUCCESS: Files are identical!")
            return True
        else:
            print("\nMISMATCH: Files have different content")
            return False

    # For lists, use hash-based comparison for efficiency
    print("\nCreating hashes for comparison...")
    print(f"  Hashing file1 ({len(data1):,} items)...")
    data1_hashes = {object_hash(obj): obj for obj in data1}

    print(f"  Hashing file2 ({len(data2):,} items)...")
    data2_hashes = {object_hash(obj): obj for obj in data2}

    print(f"\nUnique objects in file1: {len(data1_hashes):,}")
    print(f"Unique objects in file2: {len(data2_hashes):,}")

    # Find differences using set operations
    only_in_1_keys = set(data1_hashes.keys()) - set(data2_hashes.keys())
    only_in_2_keys = set(data2_hashes.keys()) - set(data1_hashes.keys())
    common_keys = set(data1_hashes.keys()) & set(data2_hashes.keys())

    # Report results
    print("\n=== COMPARISON RESULTS ===")
    print(f"Identical objects: {len(common_keys):,}")
    print(f"Objects only in file1: {len(only_in_1_keys):,}")
    print(f"Objects only in file2: {len(only_in_2_keys):,}")

    if len(only_in_1_keys) == 0 and len(only_in_2_keys) == 0:
        print("\nSUCCESS: Files are identical!")
        print("Percentage match: 100.00%")
        return True
    else:
        print("\nMISMATCH: Files have different content")
        print(f"Percentage identical: {len(common_keys)/len(data1)*100:.2f}%")
        print(f"Percentage different: {len(only_in_1_keys)/len(data1)*100:.2f}%")

        # Show sample differences
        if only_in_1_keys:
            print("\n=== SAMPLE OBJECTS ONLY IN FILE1 (showing up to 3) ===")
            for i, hash_key in enumerate(list(only_in_1_keys)[:3]):
                obj = data1_hashes[hash_key]
                print(f"\nObject {i+1}:")
                # Show key fields if it's a dict
                if isinstance(obj, dict):
                    key_fields = [
                        "ensembl_gene_id",
                        "gene_symbol",
                        "tissue",
                        "model_group",
                        "name",
                        "id",
                        "matched_control",
                    ]
                    for key in key_fields:
                        if key in obj:
                            print(f"  {key}: {obj[key]}")
                else:
                    print(f"  {str(obj)[:200]}...")

        if only_in_2_keys:
            print("\n=== SAMPLE OBJECTS ONLY IN FILE2 (showing up to 3) ===")
            for i, hash_key in enumerate(list(only_in_2_keys)[:3]):
                obj = data2_hashes[hash_key]
                print(f"\nObject {i+1}:")
                # Show key fields if it's a dict
                if isinstance(obj, dict):
                    key_fields = [
                        "ensembl_gene_id",
                        "gene_symbol",
                        "tissue",
                        "model_group",
                        "name",
                        "id",
                        "matched_control",
                    ]
                    for key in key_fields:
                        if key in obj:
                            print(f"  {key}: {obj[key]}")
                else:
                    print(f"  {str(obj)[:200]}...")

        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare two JSON files for content equality, ignoring order."
    )
    parser.add_argument("file1", help="Path to the first JSON file")
    parser.add_argument("file2", help="Path to the second JSON file")
    args = parser.parse_args()

    result = compare_json_files(args.file1, args.file2)

    # Exit with appropriate status code
    # 0 = success (files are identical)
    # 1 = failure (files differ)
    sys.exit(0 if result else 1)
