"""
This module contains the transformation logic for the biomarkers and pathology datasets.
This is for the Model AD project.
"""

import pandas as pd
import math
from typing import Dict, List, Any, Union, Tuple

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
    )  # 1st and 2nd digits are now in the tens and 1s place
    # Round UP to the next 5 or 0 (add small epsilon to ensure we always round up)
    rounded_scaled = 5 * math.ceil((scaled + 1e-10) / 5)

    # Put back to the right magnitude
    result = rounded_scaled * (10 ** (magnitude - 1))

    # remove float precision issues
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


def _create_data_rows_from_groups(
    dataset: pd.DataFrame,
    group_columns: List[str],
    extra_columns: List[str],
    extra_column_name: str,
    y_axis_max_map: Dict[Tuple[str, str, str], float],
) -> List[Dict[str, Any]]:
    """
    Create data rows by grouping the dataset and adding y_axis_max values.

    Groups the dataset by the specified columns and creates dictionary entries
    where each entry represents a unique group. Each data row contains the group
    key values, a y_axis_max value calculated for that group, and aggregated
    data from the extra columns.

    Args:
        dataset: The prepared dataset to group and process
        group_columns: Columns to group by (e.g., ['name', 'evidence_type', 'tissue'])
        extra_columns: Other columns in addition to group_columns to include in the returned data
        extra_column_name: Name of the key in each returned dictionary that will contain the aggregated extra_columns data
        y_axis_max_map: Pre-calculated final y_axis_max values for each group combination

    Returns:
        List of dictionary entries (data rows), where each entry contains:
        - Group key values from group_columns
        - y_axis_max value for the group
        - Aggregated data from extra_columns under the key specified by extra_column_name
    """
    data_rows = []
    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        entry = dict(zip(group_columns, group_key))

        # Get the final y_axis_max value for this combination of (name, evidence_type, tissue)
        key_for_y_axis = (entry["name"], entry["evidence_type"], entry["tissue"])
        entry["y_axis_max"] = y_axis_max_map.get(key_for_y_axis, round_y_axis_max(0))

        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    return data_rows


def _add_missing_age_entries(
    data_rows: List[Dict[str, Any]],
    dataset: pd.DataFrame,
    y_axis_max_map: Dict[Tuple[str, str, str], float],
) -> List[Dict[str, Any]]:
    """
    Add placeholder entries for missing age combinations to ensure data completeness.

    This function ensures that all tissue/evidence_type combinations have data entries
    for all available ages in the dataset, filling in missing time points with empty
    placeholder entries.

    Example:
        If the dataset has ages 4, 8, and 12 months but one tissue is missing 8 month
        data, this function adds to data_rows to fill in the missing time point with
        an entry containing empty data arrays and units.

    Args:
        data_rows: Existing data rows
        dataset: The original dataset
        y_axis_max_map: Pre-calculated final y_axis_max values for each group combination

    Returns:
        Updated data_rows with missing age entries added
    """
    # Get all unique ages that exist in the dataset
    available_ages = list(set([x["age"] for x in data_rows]))

    # Group by the key dimensions that should have consistent age coverage
    missing_ages_group_columns = ["name", "evidence_type", "tissue"]
    grouped_missing_ages = dataset.groupby(missing_ages_group_columns)

    # For each unique combination of (name, evidence_type, tissue)
    for group_key, group in grouped_missing_ages:
        entry = dict(zip(missing_ages_group_columns, group_key))

        # Get the ages that currently exist for this group
        group_ages = group["age"].unique().tolist()

        # Find which ages are missing from this group
        missing_ages = [age for age in available_ages if age not in group_ages]

        # Get the y_axis_max for this combination
        key_for_y_axis = (entry["name"], entry["evidence_type"], entry["tissue"])
        y_axis_max = y_axis_max_map.get(key_for_y_axis, 0)

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
                        "y_axis_max": y_axis_max,
                        "data": [],  # Empty data array since there are no measurements for this age
                    }
                )

    return data_rows


def _extract_age_num(entry: Dict[str, Any]) -> Tuple[float, str, str]:
    """
    Extract numeric age value for sorting purposes.

    Args:
        entry: A dictionary containing the "age" field.

    Returns:
        Tuple[float, str, str]: (numeric_value, age_string, evidence_type) for consistent sorting
    """
    age_str = entry.get("age", "")
    evidence_type = entry.get("evidence_type", "")
    try:
        numeric_value = float(age_str.split()[0])
        return (numeric_value, age_str, evidence_type)
    except (ValueError, IndexError, AttributeError):
        # For invalid ages, use a large number and the age string for consistent sorting
        # Include evidence_type to ensure deterministic ordering when ages are invalid
        return (float("inf"), age_str, evidence_type)


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

    # Calculate final y_axis_max values for all combinations
    y_axis_max_map = _calculate_y_axis_max_map(dataset)

    # Create initial data rows from groups
    data_rows = _create_data_rows_from_groups(
        dataset, group_columns, extra_columns, extra_column_name, y_axis_max_map
    )

    # Add missing age entries for completeness
    data_rows = _add_missing_age_entries(data_rows, dataset, y_axis_max_map)

    # Sort data_rows by the numeric value in the "age" field
    data_rows.sort(key=_extract_age_num)

    data_rows = convert_numpy_types(data_rows)

    return data_rows
