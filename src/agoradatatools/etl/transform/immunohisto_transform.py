"""
This module contains the transformation logic for the biomarkers and pathology datasets.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List, Any

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    convert_numpy_types,
)


REQUIRED_INPUT = {
    "biomarkers": [
        "model",
        "type",
        "measurement",
        "units",
        "age_death",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
    "pathology": [
        "model",
        "type",
        "measurement",
        "units",
        "age_death",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
}


def prepare_immunohisto_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function prepares the biomarker and pathology dataframes for the Model AD project.
    It performs the following transformations:
    1. Fill missing values with an empty string.
    2. Capitalize 'sex' and 'tissue' columns in the DataFrame.
    3. Replace 'beta' with '&beta;' in the 'type' column.
    4. Rename 'type' column to 'evidence_type' and 'measurement' to 'value'.
    5. Append "months" to age values
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Fill missing values and transform text fields
    df = df.fillna("")
    df["sex"] = df["sex"].str.title()
    df["tissue"] = df["tissue"].str.title()

    # Replace 'beta' with '&beta;' in biomarker types
    df["type"] = df["type"].str.replace("beta", "&beta;")

    # Rename columns
    df = df.rename(
        columns={"type": "evidence_type", "measurement": "value", "age_death": "age"}
    )

    # Append "months" to age values
    df["age"] = df["age"].astype(str) + " months"

    return df


def immunohisto_transform(
    datasets: Dict[str, pd.DataFrame],
    dataset_name: str,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
    group_columns: List[str] = [
        "model",
        "evidence_type",
        "tissue",
        "age",
        "units",
    ],
    extra_columns: List[str] = ["genotype", "sex", "individual_id", "value"],
    extra_column_name: str = "data",
) -> List[Dict[str, Any]]:
    """
    Takes a dictionary of dataset DataFrames, extracts the 'dataset_name'
    DataFrame, and transforms it into a DataFrame grouped by group_columns.
    Will include extra_columns in the group.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        dataset_name (str): The name of the dataset to transform.
        group_columns (List[str], optional): List of columns to group by. Defaults to ['model', 'evidence_type', 'tissue', 'age', 'units'].
        extra_columns (List[str], optional): List of columns to include in the group. Defaults to ['genotype', 'sex', 'individual_id', 'value'].
        extra_column_name (str, optional): Name of the column containing the extra columns. Defaults to 'data'.

    Returns:
        pd.DataFrame: A DataFrame grouped by the group_columns.
    """

    # Filter required_input to only include datasets that are present
    filtered_required_input = {
        key: value for key, value in required_input.items() if key in datasets
    }

    # Ensure at least one of "biomarkers" or "pathology" is present
    if not any(key in datasets for key in ["biomarkers", "pathology"]):
        raise ValueError(
            "At least one of 'biomarkers' or 'pathology' must be present in the datasets"
        )

    check_required_datasets_and_columns(datasets, filtered_required_input)

    dataset = prepare_immunohisto_data(datasets[dataset_name].fillna("none"))

    data_rows = []

    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))
        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    data_rows = convert_numpy_types(data_rows)

    return data_rows
