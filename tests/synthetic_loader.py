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
        if not file_name.endswith(".csv"):
            continue
        df = pd.read_csv(os.path.join(input_path, file_name))
        key = file_to_key_mapping.get(file_name, file_name.replace(".csv", ""))
        datasets[key] = df
    return datasets
