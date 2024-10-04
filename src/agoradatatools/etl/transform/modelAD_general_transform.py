"""
This module contains the transformation logic for the biomarkers and pathology datasets.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List


def modelAD_general_transform(
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
    dataset = datasets[dataset_name]

    missing_columns = [
        col for col in group_columns + extra_columns if col not in dataset.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{dataset_name} dataset missing columns: {', '.join(missing_columns)}"
        )

    dataset = dataset.fillna("none")
    data_rows = []

    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))
        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    return pd.DataFrame(data_rows)
