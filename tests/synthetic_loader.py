"""Load synthetic CSV fixtures shared by the RNA differential expression suites."""

import os

import pandas as pd


def load_synthetic_test_data(
    data_files_path: str,
    data_files: list[str],
    file_to_key_mapping: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """Load CSV fixtures from data_files_path/input.

    file_to_key_mapping renames a fixture to the dataset key the transform expects.
    Any other CSV keeps its file name without the extension.
    """
    datasets = {}
    input_path = os.path.join(data_files_path, "input")
    for file_name in data_files:
        if file_name.endswith(".json"):
            df = pd.read_json(os.path.join(input_path, file_name))
        elif file_name.endswith(".csv"):
            df = pd.read_csv(os.path.join(input_path, file_name))
        else:
            continue

        default_name = file_name.replace(".csv", "").replace(".json", "")
        key = file_to_key_mapping.get(file_name, default_name)
        datasets[key] = df
    return datasets
