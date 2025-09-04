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
        "name",
        "evidence_type",
        "value",
        "units",
        "age",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
    "pathology": [
        "name",
        "evidence_type",
        "value",
        "units",
        "age",
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
    3. Replace 'beta' with '&beta;' in the 'evidence_type' column.
    4. Append "months" to age values
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Fill missing values and transform text fields
    df = df.fillna("")
    df["sex"] = df["sex"].str.title()
    df["tissue"] = df["tissue"].str.title()

    # Replace 'beta' with '&beta;' in biomarker evidence_type
    df["evidence_type"] = df["evidence_type"].str.replace("beta", "&beta;")

    # Append "months" to age values
    df["age"] = df["age"].astype(str) + " months"

    return df


def immunohisto_transform(
    datasets: Dict[str, pd.DataFrame],
    dataset_name: str,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
    group_columns: List[str] = [
        "name",
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
    The output is a list of dictionaries, where each dictionary represents a unique combination
    of the group_columns (by default: name, evidence_type, tissue, age, units). Each dictionary
    contains these group-by fields as keys, and an additional key (by default "data") whose value
    is a list of dictionaries. Each dictionary in this list corresponds to an individual measurement
    and contains the extra_columns (by default: genotype, sex, individual_id, value).

    Example output structure:
    [
      {
        "name": "3xTg-AD",
        "evidence_type": "Insoluble A&beta;40",
        "tissue": "Cerebral Cortex",
        "age": "12 months",
        "units": "pg/mg",
        "data": [
          {
            "genotype": "3xTg-AD",
            "sex": "Male",
            "individual_id": "4041",
            "value": 3.816854093
          },
          ...
        ]
      },
      ...
    ]

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        dataset_name (str): The name of the dataset to transform.
        group_columns (List[str], optional): List of columns to group by. Defaults to ['name', 'evidence_type', 'tissue', 'age', 'units'].
        extra_columns (List[str], optional): List of columns to include in the group. Defaults to ['genotype', 'sex', 'individual_id', 'value'].
        extra_column_name (str, optional): Name of the column containing the extra columns. Defaults to 'data'.

    Returns:
        pd.DataFrame: DataFrame containing all group_columns, plus an extra column named as specified
        in extra_column_name. This extra column contains all information from extra_columns, collapsed
        into a single dictionary.
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

    dataset = prepare_immunohisto_data(datasets[dataset_name])

    data_rows = []

    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        # This for loop iterates over each group produced by grouping the dataset by the specified group_columns.
        # For each group:
        #   - It creates a dictionary (entry) by zipping group_columns (list of column names) with the values from group_key (tuple containing the actual values for those columns) for this group.
        #   - It then adds a new key to this dictionary, named according to extra_column_name (default 'data'), whose value is a list of dictionaries.
        #     Each dictionary in this list corresponds to a row in the group, containing only the columns specified in extra_columns
        #     (by default: ['genotype', 'sex', 'individual_id', 'value']).
        #   - This entry is then appended to the data_rows list.
        # The result is that data_rows will contain one dictionary per group, with group-level metadata and a list of per-individual data.
        entry = dict(zip(group_columns, group_key))
        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    # Ensure data completeness by filling in missing age combinations
    # This section handles cases where some combinations of (name, evidence_type, tissue)
    # don't have data for all available ages in the dataset. We create placeholder entries
    # with empty data arrays to maintain consistent structure across all age groups.

    # Get all unique ages that exist in the dataset
    available_ages = list(set([x["age"] for x in data_rows]))

    # Group by the key dimensions that should have consistent age coverage
    # (excluding 'age' and 'units' since we're checking for missing ages)
    missing_ages_group_columns = ["name", "evidence_type", "tissue"]
    grouped_missing_ages = dataset.groupby(missing_ages_group_columns)

    # For each unique combination of (name, evidence_type, tissue)
    for group_key, group in grouped_missing_ages:
        # Create a dictionary with the group's key values
        entry = dict(zip(missing_ages_group_columns, group_key))

        # Get the ages that currently exist for this group
        group_ages = group["age"].unique().tolist()

        # Find which ages are missing from this group
        missing_ages = [age for age in available_ages if age not in group_ages]

        # If there are missing ages, create placeholder entries for each missing age
        if len(missing_ages) > 0:
            for age in missing_ages:
                data_rows.append(
                    {
                        "name": entry["name"],
                        "evidence_type": entry["evidence_type"],
                        "tissue": entry["tissue"],
                        "age": age,
                        "units": "",  # Empty units since there's no actual data
                        "data": [],  # Empty data array since there are no measurements for this age
                    }
                )

    # Sort data_rows by the numeric value in the "age" field (e.g., "6 months" -> 6)
    def extract_age_num(entry):
        """
        Sorts the data_rows list by the numeric value in the "age" field (e.g., "6 months" -> 6)

        Args:
            entry (dict): A dictionary containing the "age" field.

        Returns:
            int: The numeric value in the "age" field.
        """
        age_str = entry.get("age", "")
        try:
            return int(age_str.split()[0])
        except (ValueError, IndexError, AttributeError):
            return float("inf")
    data_rows.sort(key=extract_age_num)

    data_rows = convert_numpy_types(data_rows)

    return data_rows
