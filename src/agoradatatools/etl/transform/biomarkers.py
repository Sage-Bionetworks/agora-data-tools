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
    if "biomarkers" not in datasets:
        raise ValueError("Biomarkers dataset not found in datasets dictionary")
    biomarkers_dataset = datasets["biomarkers"]
    expected_columns = [
        "model",
        "type",
        "ageDeath",
        "tissue",
        "units",
        "genotype",
        "measurement",
        "sex",
    ]
    if not set(expected_columns).issubset(set(biomarkers_dataset.columns)):
        missing_columns = [
            s for s in set(expected_columns) if s not in biomarkers_dataset.columns
        ]
        raise ValueError(
            f"Biomarker dataset does not contain expected columns. Missing column(s): {missing_columns}"
        )
    biomarkers_dataset = biomarkers_dataset.fillna("none")
    data_as_list = []
    grouped = biomarkers_dataset.groupby(
        ["model", "type", "ageDeath", "tissue", "units"]
    )

    for (model, type_, ageDeath, tissue, units), group in grouped:
        # Create the base structure for each group
        entry = {
            "model": model,
            "type": type_,
            "ageDeath": ageDeath,
            "tissue": tissue,
            "units": units,
            "points": [],
        }

        # Append the measurement, genotype, and sex for each row
        for _, row in group.iterrows():
            point = {
                "genotype": row["genotype"],
                "measurement": row["measurement"],
                "sex": row["sex"],
            }
            entry["points"].append(point)

        # Add the entry to the list
        data_as_list.append(entry)

    return data_as_list
