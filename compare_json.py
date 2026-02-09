#!/usr/bin/env python3
"""Compare two JSON files for content equality, ignoring order."""

import json
import sys
from typing import Any


def normalize_json(obj: Any) -> Any:
    """Recursively normalize JSON object for comparison."""
    if isinstance(obj, dict):
        # Sort dict items by key
        return {k: normalize_json(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        # For lists of dicts, try to sort by a consistent key if possible
        # Otherwise keep as-is since order might matter
        if all(isinstance(item, dict) for item in obj):
            # Try to find a consistent key for sorting
            try:
                # Common keys that might uniquely identify entries
                for key in ["ensembl_gene_id", "id", "name", "tissue", "age"]:
                    if all(key in item for item in obj):
                        return sorted(
                            [normalize_json(item) for item in obj],
                            key=lambda x: str(x.get(key, "")),
                        )
                # If no consistent key, convert to tuple of sorted items
                return sorted(
                    [json.dumps(normalize_json(item), sort_keys=True) for item in obj]
                )
            except (TypeError, KeyError):
                pass
        return [normalize_json(item) for item in obj]
    else:
        return obj


def compare_json_files(file1: str, file2: str) -> bool:
    """Compare two JSON files for content equality."""
    print(f"Loading {file1}...")
    with open(file1, "r") as f1:
        data1 = json.load(f1)

    print(f"Loading {file2}...")
    with open(file2, "r") as f2:
        data2 = json.load(f2)

    print("\nFile 1 stats:")
    print(f"  Type: {type(data1)}")
    if isinstance(data1, list):
        print(f"  Length: {len(data1)} items")
        if data1:
            print(
                f"  First item keys: {list(data1[0].keys()) if isinstance(data1[0], dict) else 'N/A'}"
            )

    print("\nFile 2 stats:")
    print(f"  Type: {type(data2)}")
    if isinstance(data2, list):
        print(f"  Length: {len(data2)} items")
        if data2:
            print(
                f"  First item keys: {list(data2[0].keys()) if isinstance(data2[0], dict) else 'N/A'}"
            )

    # Quick check: same type and length
    if type(data1) != type(data2):
        print(f"\n❌ MISMATCH: Different types - {type(data1)} vs {type(data2)}")
        return False

    if isinstance(data1, list) and len(data1) != len(data2):
        print(f"\n❌ MISMATCH: Different lengths - {len(data1)} vs {len(data2)}")
        return False

    # Normalize and compare
    print("\nNormalizing and comparing content...")
    norm1 = normalize_json(data1)
    norm2 = normalize_json(data2)

    # Convert to JSON strings for comparison
    json1 = json.dumps(norm1, sort_keys=True)
    json2 = json.dumps(norm2, sort_keys=True)

    if json1 == json2:
        print("\n✅ SUCCESS: Files are identical!")
        return True
    else:
        print("\n❌ MISMATCH: Files have different content")

        # Try to find differences
        if isinstance(data1, list) and isinstance(data2, list):
            print("\nDetailed comparison of list items:")
            # Create sets for comparison
            set1 = {json.dumps(normalize_json(item), sort_keys=True) for item in data1}
            set2 = {json.dumps(normalize_json(item), sort_keys=True) for item in data2}

            only_in_1 = set1 - set2
            only_in_2 = set2 - set1

            if only_in_1:
                print(f"  Items only in file1: {len(only_in_1)}")
                if len(only_in_1) <= 3:
                    for item in list(only_in_1)[:3]:
                        print(f"    {item[:200]}...")

            if only_in_2:
                print(f"  Items only in file2: {len(only_in_2)}")
                if len(only_in_2) <= 3:
                    for item in list(only_in_2)[:3]:
                        print(f"    {item[:200]}...")

        return False


if __name__ == "__main__":
    file1 = "staging/rna_de_individual/rna_de_individual.json"
    file2 = "staging/rna_de_individual_CORRECT.json"

    result = compare_json_files(file1, file2)
    sys.exit(0 if result else 1)
