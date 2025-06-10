"""
This module contains the transformation logic for the biomarkers and pathology datasets.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List

from agoradatatools.etl.utils import check_required_datasets_and_columns


def prepare_immunohisto_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function prepares the biomarker and pathology dataframes for the Model AD project.
    It performs the following transformations:
    1. Fill missing values with an empty string.
    2. Capitalize 'sex' and 'tissue' columns in the DataFrame.
    3. Replace 'beta' with '&beta;' in the 'type' column.
    4. Rename 'type' column to 'evidence_type' and 'measurement' to 'value'.
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
    return df.rename(columns={"type": "evidence_type", "measurement": "value"})


def immunohisto_transform(
    datasets: Dict[str, pd.DataFrame],
    dataset_name: str,
    group_columns: List[str] = ["model", "type", "age_death", "tissue", "units"],
    extra_columns: List[str] = ["genotype", "measurement", "sex"],
    extra_column_name: str = "points",
) -> pd.DataFrame:
    """
    Takes a dictionary of dataset DataFrames, extracts the 'dataset_name'
    DataFrame, and transforms it into a DataFrame grouped by group_columns.
    Will include extra_columns in the group.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        dataset_name (str): The name of the dataset to transform.
        group_columns (List[str], optional): List of columns to group by. Defaults to ['model', 'type', 'age_death', 'tissue', 'units'].
        extra_columns (List[str], optional): List of columns to include in the group. Defaults to ['genotype', 'measurement', 'sex'].
        extra_column_name (str, optional): Name of the column containing the extra columns. Defaults to 'points'.

    Returns:
        pd.DataFrame: A DataFrame grouped by the group_columns.
    """
    check_required_datasets_and_columns(
        datasets, {dataset_name: group_columns + extra_columns}
    )

    dataset = datasets[dataset_name].fillna("none")

    data_rows = []

    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))
        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    return pd.DataFrame(data_rows)
