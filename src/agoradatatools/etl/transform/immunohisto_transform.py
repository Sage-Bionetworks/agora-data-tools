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


def round_y_axis_max(y_axis_max: float) -> float:
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
    import math

    # Special case: if max is 0, return 10
    if y_axis_max == 0:
        return 10.0

    # Handle negative values (though they shouldn't occur in this context)
    if y_axis_max < 0:
        return 0.0

    # Find the order of magnitude of the number
    magnitude = int(math.floor(math.log10(y_axis_max)))

    # Scale the number so the first digit is in the ones place
    scaled = y_axis_max / (10**magnitude)

    # Extract first digit (leftmost) and second digit
    first_digit = int(scaled)

    # Use string method to avoid floating point precision issues
    scaled_str = f"{scaled:.10f}"
    if "." in scaled_str:
        decimal_part = scaled_str.split(".")[1]
        if len(decimal_part) >= 1:
            second_digit = int(decimal_part[0])
        else:
            second_digit = 0
    else:
        second_digit = 0

    # Always round UP to the next "nice" number
    # Nice numbers have second digit of 0 or 5
    if second_digit == 0:
        # Already a nice number, but we need to round UP
        # So we go to the next nice number
        rounded_second = 5
    elif second_digit <= 5:
        # Round up to 5
        rounded_second = 5
    else:
        # Round up to next first digit with 0
        rounded_second = 0
        first_digit += 1

    # Handle edge case where first digit rounded up to 10
    if first_digit >= 10:
        first_digit = 1
        magnitude += 1

    # Construct the result
    result = (first_digit + rounded_second / 10.0) * (10**magnitude)

    return result


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

    # First, calculate y_axis_max for each combination of (name, evidence_type, tissue) across all ages
    key_dimensions = ["name", "evidence_type", "tissue"]
    y_axis_max_map = {}
    for key, group in dataset.groupby(key_dimensions):
        y_axis_max_map[tuple(key)] = group["value"].max() if len(group) > 0 else 0

    grouped = dataset.groupby(group_columns)

    for group_key, group in grouped:
        # This for loop iterates over each group produced by grouping the dataset by the specified group_columns.
        # For each group:
        #   - It creates a dictionary (entry) by zipping group_columns (list of column names) with the values from group_key (tuple containing the actual values for those columns) for this group.
        #   - It gets the y_axis_max value from the pre-calculated map
        #   - It then adds a new key to this dictionary, named according to extra_column_name (default 'data'), whose value is a list of dictionaries.
        #     Each dictionary in this list corresponds to a row in the group, containing only the columns specified in extra_columns
        #     (by default: ['genotype', 'sex', 'individual_id', 'value']).
        #   - This entry is then appended to the data_rows list.
        # The result is that data_rows will contain one dictionary per group, with group-level metadata and a list of per-individual data.
        entry = dict(zip(group_columns, group_key))

        # Get the y_axis_max for this combination of (name, evidence_type, tissue)
        key_for_y_axis = (entry["name"], entry["evidence_type"], entry["tissue"])
        raw_y_axis_max = y_axis_max_map.get(key_for_y_axis, 0)
        entry["y_axis_max"] = round_y_axis_max(raw_y_axis_max)

        entry[extra_column_name] = group[extra_columns].to_dict("records")
        data_rows.append(entry)

    # Ensure data completeness by filling in missing age combinations
    # This section handles cases where some combinations of (name, evidence_type, tissue)
    # don't have data for all available ages in the dataset. We create placeholder entries
    # with empty data arrays to maintain consistent structure across all age groups.

    # Get all unique ages that exist in the dataset
    available_ages = list(set([x["age"] for x in data_rows]))

    # Create a lookup map for y_axis_max values from already processed entries
    y_axis_max_lookup = {}
    for entry in data_rows:
        key = (entry["name"], entry["evidence_type"], entry["tissue"])
        y_axis_max_lookup[key] = entry["y_axis_max"]

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

        # Get the y_axis_max for this combination of (name, evidence_type, tissue)
        # We can reuse the already calculated value from the main loop
        key_for_y_axis = (entry["name"], entry["evidence_type"], entry["tissue"])
        y_axis_max = y_axis_max_lookup.get(key_for_y_axis, 0)

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

    data_rows = convert_numpy_types(data_rows)

    return data_rows
