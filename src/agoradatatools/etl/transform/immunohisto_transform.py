"""
This module contains the transformation logic for the biomarkers and pathology datasets.
This is for the Model AD project.
"""

import pandas as pd
import numpy as np
import math
from typing import Dict, List, Any, Union, Tuple

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    nest_fields,
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

    # Append "months" to age values if not already present
    df["age"] = df["age"].astype(str)
    df["age"] = df["age"].apply(lambda x: x if x.endswith("months") else x + " months")

    return df


def round_y_axis_max(y_axis_max: Union[int, float, str]) -> float:
    """
    This function rounds the y_axis_max value to the nearest sensible nice round number.

    Logic:
    - If max == 0, then y_axis_max == 10
    - Else, round UP to the next "nice" number where the second digit is 0 or 5

    Examples:
    - 0.0021 rounds up to 0.0025
    - 0.0004 rounds up to 0.00045
    - 0.329486078 rounds up to 0.35
    - 0.089 rounds up to 0.090
    - 1094 rounds up to 1500
    - 1322498 rounds up to 1500000
    - 728591 rounds up to 750000
    - 3973 rounds up to 4000
    - 1.616 rounds up to 2.0
    """
    # Convert to float if it's a string or other type
    try:
        y_axis_max = float(y_axis_max)
    except (ValueError, TypeError):
        # If conversion fails, return 10 (default case)
        return 10.0

    # Handle special cases: zero or negative values
    if y_axis_max <= 0:
        return 10.0 if y_axis_max == 0 else 0.0

    # Find the order of magnitude of the number
    magnitude = int(math.floor(math.log10(y_axis_max)))

    # Scale the number so the first digit is in the ones place
    scaled = y_axis_max / (
        10 ** (magnitude - 1)
    )  # After scaling, the first significant digit is in the tens place and the second in the ones place
    # Round UP to the next 5 or 0 (add small epsilon to ensure we always round up)
    rounded_scaled = 5 * math.ceil((scaled + 1e-10) / 5)

    # Put back to the right magnitude
    result = rounded_scaled * (10 ** (magnitude - 1))

    # Remove float precision issues
    result = round(result, 15)

    # Ensure we always return a float, even for whole numbers
    return float(result)


def _calculate_y_axis_max_map(
    dataset: pd.DataFrame,
) -> Dict[Tuple[str, str, str], float]:
    """
    Calculate final y_axis_max values for each combination of (name, evidence_type, tissue) across all ages.

    This function finds the raw maximum values from the dataset and applies rounding
    to get the final y_axis_max values using round_y_axis_max().

    Args:
        dataset: The prepared dataset. It must have columns "name", "evidence_type", "tissue", and "value".

    Returns:
        Dictionary mapping (name, evidence_type, tissue) tuples to their final rounded y_axis_max values
    """
    key_dimensions = ["name", "evidence_type", "tissue"]
    y_axis_max_map = {}

    for key, group in dataset.groupby(key_dimensions):
        # Convert value column to numeric, coercing errors to NaN, then drop NaN values
        numeric_values = pd.to_numeric(group["value"], errors="coerce").dropna()
        if len(numeric_values) > 0:
            raw_max = numeric_values.max()
            y_axis_max_map[tuple(key)] = round_y_axis_max(raw_max)
        else:
            y_axis_max_map[tuple(key)] = round_y_axis_max(0)

    return y_axis_max_map


def _add_missing_age_entries(data_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Add placeholder entries for missing age combinations to ensure data completeness.

    This function ensures that all tissue/evidence_type combinations have data entries
    for all available ages in the dataset, filling in missing time points with empty
    placeholder entries.

    Example:
        If the dataset has ages 4, 8, and 12 months but one tissue is missing 8 month
        data, this function adds rows to fill in the missing time point with
        entries containing empty data arrays and units.

    Args:
        data_rows: DataFrame with grouped data including columns for name, evidence_type,
                   tissue, age, units, y_axis_max, and data

    Returns:
        Updated DataFrame with missing age entries added
    """
    # Get all unique ages that exist in the data
    available_ages = list(data_rows["age"].drop_duplicates())

    # All unique combinations of groups (name, evidence_type, tissue, y_axis_max)
    fill_df = (
        data_rows[["name", "evidence_type", "tissue", "y_axis_max"]]
        .copy()
        .drop_duplicates()
    )

    # Make an "age" column where each entry is a list of all possible ages
    fill_df["age"] = [available_ages] * fill_df.shape[0]

    # "explode" makes one row per age + group. Then merge back into the data to create new rows for missing ages
    fill_df = fill_df.explode("age").merge(
        data_rows, how="outer", validate="one_to_one"
    )

    # Fill NA values for units. Can't use fillna to make an empty list so we add an extra line
    fill_df = fill_df.fillna({"units": ""})
    fill_df["data"] = fill_df["data"].apply(lambda x: [] if x is np.nan else x)

    return fill_df


def _extract_age_num(age_str: str) -> float:
    """
    Extract numeric age value for sorting purposes.

    Args:
        age_str: A string containing the age value (e.g., "12 months")

    Returns:
        float: The numeric age value, or inf for invalid ages
    """
    try:
        numeric_value = float(age_str.split()[0])
        return numeric_value
    except (ValueError, IndexError, AttributeError):
        # For invalid ages, use a large number for consistent sorting
        return float("inf")


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

    # Handle empty datasets - return empty list immediately
    if dataset.empty:
        return []

    # Calculate final y_axis_max values for all combinations
    y_axis_max_map = _calculate_y_axis_max_map(dataset)

    # Create initial data rows from groups using nest_fields
    # We need to drop columns that are not in group_columns or extra_columns
    columns_to_keep = set(group_columns + extra_columns)
    columns_to_drop = [col for col in dataset.columns if col not in columns_to_keep]

    data_rows = nest_fields(
        dataset.copy(),
        grouping=group_columns,
        new_column=extra_column_name,
        drop_columns=group_columns + columns_to_drop,
    )

    data_rows["y_axis_max"] = data_rows.apply(
        lambda entry: y_axis_max_map.get(
            (entry["name"], entry["evidence_type"], entry["tissue"]),
            round_y_axis_max(0),
        ),
        axis=1,
    )

    # Add missing age entries for completeness
    data_rows = _add_missing_age_entries(data_rows)

    # Sort by age (convert age to numeric for sorting)
    data_rows["age_numeric"] = data_rows["age"].apply(_extract_age_num)
    data_rows = data_rows.sort_values(["age_numeric", "age", "evidence_type"]).drop(
        columns="age_numeric"
    )

    # Reorder columns to match expected output: group_columns + y_axis_max + extra_column_name
    column_order = group_columns + ["y_axis_max", extra_column_name]
    data_rows = data_rows[column_order]

    # Convert to list of dicts at the very end
    return data_rows.to_dict("records")
