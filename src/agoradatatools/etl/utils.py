from typing import Union, Dict, Any, List

import numpy as np
import pandas as pd
import synapseclient
import yaml


# TODO remove "_" - these utils functions are not only used internally
def _login_to_synapse(token: str = None) -> synapseclient.Synapse:
    """Logs into Synapse python client, returns authenticated Synapse session.

    Args:
        authtoken (str, optional): Synapse authentication token. Defaults to None.

    Returns:
        synapseclient.Synapse: authenticated Synapse client session
    """
    agent_str = "agora-data-tools/0.0.0"
    syn = synapseclient.Synapse(user_agent=agent_str)
    if token is None:
        syn.login()
    else:
        syn.login(authToken=token)
    return syn


def _get_config(
    config_path: str = None,
) -> Dict[str, Any]:
    """Takes config_path and opens yaml file path points to, loads configuration from file.
    If no config_path is supplied, defaults to "./configs/agora_prod.yaml"

    Args:
        config_path (str, optional): Path to config file. Defaults to None.

    Returns:
        dict: Dictionary containing configuration from yaml file
    """
    if config_path is None:
        config_path = "./configs/agora_prod.yaml"

    file = None
    config = None

    try:
        file = open(config_path, "r")
        config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        raise FileNotFoundError("File not found.  Please provide a valid path.")
    except yaml.parser.ParserError:
        raise yaml.parser.ParserError(
            "YAML file unable to be parsed.  Please provide a valid YAML file."
        )
    except yaml.scanner.ScannerError:
        raise yaml.scanner.ScannerError(
            "YAML file unable to be scanned.  Please provide a valid YAML file."
        )
    if not isinstance(config, dict):
        raise ValueError(
            "YAML file must be loaded as a single dictionary.  Please reformat your YAML file correctly."
        )
    return config


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Takes in a dataframe replaces problematic characters in column names
    and makes column names all lowercase characters

    Args:
        df (pd.DataFrame): DataFrame with columns to be standardized

    Returns:
        pd.DataFrame: New dataframe with cleaned column names
    """

    df.columns = df.columns.str.replace(
        "[#@&*^?()%$#!/]", "", regex=True
    )  # the commas were unnessesary and were breaking the prelacement of '-' characters
    df.columns = df.columns.str.replace("[ -.]", "_", regex=True)
    df.columns = map(str.lower, df.columns)

    return df


def standardize_values(df: pd.DataFrame) -> pd.DataFrame:
    """Finds non-compliant values and corrects them
    *if more data cleaning options need to be added to this,
    this needs to be refactored to another function

    Args:
        df (pd.DataFrame): DataFrame with values to be standardized

    Returns:
        pd.DataFrame: Resulting DataFrame with standardized values
    """
    try:
        # Use a more precise regex that only matches exact N/A values
        # This ensures we don't replace N/A substrings within other text
        df.replace(
            [r"^n/a$", r"^N/A$", r"^n/A$", r"^N/a$"], np.nan, regex=True, inplace=True
        )
    except TypeError:  # I could not get this to trigger without mocking replace
        print("Error comparing types.")

    return df


def rename_columns(
    data: Union[pd.DataFrame, list[dict], dict], column_map: dict[str, str]
) -> Union[pd.DataFrame, list[dict], dict]:
    """Takes in a dataframe, list of dictionaries, or dictionary and renames columns according to the mapping provided.
    If the input type is a dictionary or a list of dictionaries, the input is modified in place.

    Args:
        data (pd.DataFrame, list, dict): DataFrame, list of dictionaries, or dictionary with columns to be renamed
        column_map (dict): Dictionary mapping original column names to new columns

    Returns:
        pd.DataFrame, list, dict: DataFrame, list of dictionaries, or dictionary with new columns names
    """

    def _rename_dict(d: dict, column_map: dict) -> None:
        """
        Rename keys in a dictionary in place and will therefore return None.
        If the old key is not in the dictionary to be renamed, the dictionary is not modified.
        If the old key is in the dictionary to be renamed, the key is removed and the new key is added.
        The order of the keys in the dictionary is not preserved.

        Args:
            d (dict): Dictionary to be renamed
            column_map (dict): Dictionary mapping original column names to new columns

        Returns:
            None
        """
        for old_key, new_key in column_map.items():
            if old_key in d:
                d[new_key] = d.pop(old_key)

    if not isinstance(column_map, dict):
        print("Column mapping must be a dictionary.")
        return data

    if not all(isinstance(key, str) for key in column_map.keys()):
        print("Column mapping must be a dictionary with string keys.")
        return data
    if not all(
        isinstance(value, str) and value is not None for value in column_map.values()
    ):
        print(
            "Column mapping must be a dictionary with string values that are not None."
        )
        return data

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                raise TypeError("List must contain dictionaries.")
            _rename_dict(item, column_map)
    elif isinstance(data, dict):
        _rename_dict(data, column_map)
    elif isinstance(data, pd.DataFrame):
        data.rename(columns=column_map, inplace=True)
    else:
        raise TypeError(
            "Data must be a pandas DataFrame, list of dictionaries, or dictionary."
        )

    return data


def nest_fields(
    df: pd.DataFrame,
    grouping: Union[str, List[str]],
    new_column: str,
    drop_columns: list = [],
    nested_field_is_list: bool = True,
) -> pd.DataFrame:
    """Collapses the provided DataFrame by grouping and nesting fields.

    The DataFrame is grouped by <grouping> (one or more columns, e.g. by Ensembl ID or by [name, tissue]).
    For all rows belonging to each group, each row is turned into a dictionary where the keys are column
    names and values are the values in that row. The dictionaries are then put into a list and the list
    becomes a single entry in the new column. If there is only one dictionary for every grouping (rather
    than multiple possible rows per group), this function provides the option to put the dict in this
    column instead of a list with the single dict in it. See the nested_field_is_list arg.

    Args:
        df (pd.DataFrame): DataFrame to be collapsed
        grouping (str or list of str): The column(s) that you want to group by
        new_column (str): the new column created to contain the nested dictionaries created
        drop_columns (list, optional): List of columns to leave out of the new nested dictionary. Defaults to [].
        nested_field_is_list (bool, optional): if True (default), each nested field will be a list of dicts. This
                        applies to data sets where there may be multiple rows to collapse, e.g. multiple biodomain
                        rows for a single Ensembl ID. If False, each nested field will be a single dict. This applies
                        to data sets where there is only one row to collapse, e.g. one row of Ensembl info for one
                        Ensembl ID.

    Returns:
        pd.DataFrame: New DataFrame with grouping column(s) and a column containing nested dictionaries
    """
    nested = (
        df.groupby(grouping)
        .apply(
            lambda row: row.replace({np.nan: None})
            .drop(columns=drop_columns)
            .to_dict("records")
        )
        .reset_index()
        .rename(columns={0: new_column})
    )

    if nested_field_is_list:
        return nested

    # nested_field_is_list == False
    lengths = nested[new_column].apply(len)
    if all(lengths == 1):
        nested[new_column] = nested[new_column].apply(lambda row: row[0])
        return nested
    else:
        raise ValueError(
            "nested_field_is_list cannot be False when there are multiple rows to nest per "
            + grouping
        )


def calculate_distribution(
    df: pd.DataFrame, grouping: Union[str, list], distribution_column: str
) -> pd.DataFrame:
    """Takes a pandas DataFrame and calculates the distribution of a specific column, grouped by
    a column or set of columns.

    Args:
        df (pd.DataFrame): the DataFrame to calculate distribution for
        grouping (str or list of str): the column(s) to group the data frame on (example: "tissue" or ["tissue", "model"])
        distribution_column (str): the name of the column to calculate distribution on (example: "logfc")

    Returns:
        pd.DataFrame: a Dataframe containing columns <grouping>, "min", "max", "first_quartile",
                      "median", and "third_quartile", with the statistics calculated on
                      distribution_column. The "min" and "max" values are not the true min/max,
                      but are instead adjusted to be:
                        min = first_quartile - 1.5*IQR and
                        max = third_quartile + 1.5*IQR, where
                        IQR = third_quartile - first_quartile.
    """
    df = df.groupby(grouping).agg("describe")[distribution_column].reset_index()

    if isinstance(grouping, str):
        grouping = [grouping]
    columns_keep = grouping + ["min", "max", "25%", "50%", "75%"]

    df = df[columns_keep]

    df.rename(
        columns={"25%": "first_quartile", "50%": "median", "75%": "third_quartile"},
        inplace=True,
    )

    df["IQR"] = df["third_quartile"] - df["first_quartile"]
    df["min"] = df["first_quartile"] - (1.5 * df["IQR"])
    df["max"] = df["third_quartile"] + (1.5 * df["IQR"])

    for col in ["min", "max", "median", "first_quartile", "third_quartile"]:
        df[col] = np.around(df[col], 4)

    df.drop("IQR", axis=1, inplace=True)

    return df


def check_required_datasets_and_columns(
    datasets: Dict[str, pd.DataFrame], required_input: Dict[str, List[str]]
) -> None:
    """
    Check if all required datasets and columns are present in the input dictionary.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary containing dataset names as keys and their corresponding pandas DataFrames as values.
        required_input (Dict[str, List[str]]): Dictionary containing dataset names as keys and their corresponding required columns as values.

    Raises:
        ValueError: If any required columns are missing.
    """
    # Check for missing datasets
    missing_datasets = [
        dataset for dataset in required_input.keys() if dataset not in datasets
    ]
    if missing_datasets:
        raise ValueError(
            f"Missing required datasets: {', '.join(missing_datasets)}. "
            "Please ensure all required datasets are provided {required_datasets}"
        )

    # Check for missing columns
    for dataset_name, columns in required_input.items():
        dataset_columns = datasets[dataset_name].columns
        missing_columns = [col for col in columns if col not in dataset_columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in {dataset_name} dataset: {', '.join(missing_columns)}. "
                f"Please ensure the {dataset_name} dataset contains all required columns. Columns found: {', '.join(dataset_columns)}."
            )


def flatten_list(lst: List[Any]) -> List[Any]:
    """
    Recursively flattens a nested list into a single list of values.

    Args:
        lst (List[Any]): A list which may contain nested lists at arbitrary depth.

    Returns:
        List[Any]: A new flattened list containing all the non-list elements from the input.

    Example:
        flatten(['A', ['B', 'C'], [['D'], 'E']])
        ['A', 'B', 'C', 'D', 'E']
    """
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result


def remove_duplicates_keep_order(lst: List[Any]) -> List[Any]:
    """
    Remove duplicate elements from a list while preserving the original order.

    Args:
        lst (List[Any]): The input list from which to remove duplicates.

    Returns:
        List[Any]: A new list with duplicates removed, maintaining the order of first occurrence.

    Example:
        remove_duplicates(['a', 'b', 'c', 'b'])
        ['a', 'b', 'c']
    """
    result = []
    for item in lst:
        if item not in result:
            result.append(item)
    return result


def convert_numpy_types(obj: Any) -> Any:
    """Convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj


def input_validation_model_info(df: pd.DataFrame) -> None:
    """
    Validates that each model has consistent matched_controls and model_type values.

    Args:
        df (pd.DataFrame): DataFrame containing model information with columns 'name',
                          'matched_controls', and 'model_type'

    Raises:
        ValueError: If any model has inconsistent matched_controls or model_type values
    """
    # Group by model and check for consistency
    for model, group in df.groupby("name"):
        # Check matched_controls consistency
        unique_matched_controls = group["matched_controls"].unique()
        if len(unique_matched_controls) > 1:
            raise ValueError(
                f"Model {model} has inconsistent matched_controls values: {unique_matched_controls}"
            )

        # Check model_type consistency
        unique_model_types = group["model_type"].unique()
        if len(unique_model_types) > 1:
            raise ValueError(
                f"Model {model} has inconsistent model_type values: {unique_model_types}"
            )


def normalize_zero(value: float) -> float:
    """
    Convert -0.0 to 0.0 while preserving other values.

    Args:
        value (float): The value to normalize

    Returns:
        float: The normalized value

    Example:
        normalize_zero(-0.0)
        0.0
        normalize_zero(1.0)
        1.0
    """
    # Using a tolerance of 1e-15 to account for floating point precision issues
    return 0.0 if abs(value) < 1e-15 else value
