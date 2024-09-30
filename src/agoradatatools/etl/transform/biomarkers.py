"""
This module contains the transformation logic for the biomarkers dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List, Any


def transform_biomarkers(datasets: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """
    Takes dictionary of dataset DataFrames, extracts the biomarkers
    DataFrame, and transforms it into a list of dictionaries grouped by
    'model', 'type', 'ageDeath', 'tissue', and 'units'.

    Args:
        datasets (Dict[str, pd.DataFrame]): dictionary of dataset names mapped to their DataFrame

    Returns:
        List[Dict[str, Any]]: a list of dictionaries containing biomarker data modeled after intended final JSON structure
    """
    biomarkers_dataset = datasets["biomarkers"]
    group_columns = ["model", "type", "ageDeath", "tissue", "units"]
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
    data_as_list = []

    grouped = biomarkers_dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))
        entry["points"] = group[point_columns].to_dict("records")
        data_as_list.append(entry)

    return data_as_list
