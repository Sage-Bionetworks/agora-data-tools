"""
This module contains the transformation logic for the biomarkers dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict


def transform_biomarkers(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Takes a dictionary of dataset DataFrames, extracts the biomarkers
    DataFrame, and transforms it into a DataFrame grouped by
    'model', 'type', 'age_death', 'tissue', and 'units'.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing biomarker data modeled after intended final structure.
    """
    biomarkers_dataset = datasets["biomarkers"]
    group_columns = ["model", "type", "age_death", "tissue", "units"]
    point_columns = ["genotype", "measurement", "sex"]

    missing_columns = [
        col
        for col in group_columns + point_columns
        if col not in biomarkers_dataset.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Biomarker dataset missing columns: {', '.join(missing_columns)}"
        )

    biomarkers_dataset = biomarkers_dataset.fillna("none")
    data_rows = []

    grouped = biomarkers_dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))
        entry["points"] = group[point_columns].to_dict("records")
        data_rows.append(entry)

    return pd.DataFrame(data_rows)
