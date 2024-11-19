from typing import Union
import logging

import numpy as np
import pandas as pd
import synapseclient
import yaml

logger = logging.getLogger(__name__)

# TODO remove "_" - these utils functions are not only used internally
def _login_to_synapse(token: str = None) -> synapseclient.Synapse:
    """Logs into Synapse python client, returns authenticated Synapse session.

    Args:
        authtoken (str, optional): Synapse authentication token. Defaults to None.

    Returns:
        synapseclient.Synapse: authenticated Synapse client session
    """
    syn = synapseclient.Synapse()
    if token is None:
        syn.login()
    else:
        syn.login(authToken=token)
    return syn


def _get_config(config_path: str = None) -> list:
    """Takes config_path and opens yaml file path points to, loads configuration from file.
    If no config_path is supplied, defaults to "./config.yaml"

    Args:
        config_path (str, optional): Path to config file. Defaults to None.

    Returns:
        list: list of dictionaries containing configuration information for run.
    """
    if config_path is None:
        config_path = "./config.yaml"

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
        df.replace(["n/a", "N/A", "n/A", "N/a"], np.nan, regex=True, inplace=True)
    except TypeError:  # I could not get this to trigger without mocking replace
        print("Error comparing types.")

    return df


def rename_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Takes in a dataframe and renames columns according to the mapping provided

    Args:
        df (pd.DataFrame): DataFrame with columns to be renamed
        column_map (dict): Dictionary mapping original column names to new columns

    Returns:
        pd.DataFrame: DataFrame with new columns names
    """
    try:
        df.rename(columns=column_map, inplace=True)
    except TypeError:
        print("Column mapping must be a dictionary")
        return df

    return df


def nest_fields(
    df: pd.DataFrame,
    grouping: str,
    new_column: str,
    drop_columns: list = [],
    nested_field_is_list: bool = True,
) -> pd.DataFrame:
    """Collapses the provided DataFrame into 2 columns:
    1. The grouping column
    2. A column containing a nested dictionary with the data from the rest of the DataFrame. This works as follows:
        The data frame is grouped by <grouping> (for example, by Ensembl ID). For all rows belonging to each group,
        each row is turned into a dictionary where the keys are column names and values are the values in that row.
        The dictionaries are then put into a list and the list becomes a single entry in this new data frame. If there
        is only one dictionary for every grouping (rather than multiple possible rows per group), this function
        provides the option to put the dict in this column instead of a list with the single dict in it. See
        the nested_field_is_list arg.

    Args:
        df (pd.DataFrame): DataFrame to be collapsed
        grouping (str): The column that you want to group by
        new_column (str): the new column created to contain the nested dictionaries created
        drop_columns (list, optional): List of columns to leave out of the new nested dictionary. Defaults to [].
        nested_field_is_list (bool, optional): if True (default), each nested field will be a list of dicts. This
                        applies to data sets where there may be multiple rows to collapse, e.g. multiple biodomain
                        rows for a single Ensembl ID. If False, each nested field will be a single dict. This applies
                        to data sets where there is only one row to collapse, e.g. one row of Ensembl info for one
                        Ensembl ID.

    Returns:
        pd.DataFrame: New 2 column DataFrame with group and nested dictionaries
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


def rename_unknown_column(
    df: pd.DataFrame, known_column_name: str, unknown_column_rename: str
) -> pd.DataFrame:
    """
    Renames the column in a DataFrame that is not known_column_name to unknown_column_rename.

    Parameters:
        df (pd.DataFrame): The DataFrame with one known column known_column_name and one unknown column.
        known_column_name (str): The name of the known column in the DataFrame.
        unknown_column_rename (str): The new name for the unknown column.

    Returns:
        pd.DataFrame: A new DataFrame with the unknown column renamed to unknown_column_rename.
    """
    # Identify the column that is not known_column_name
    unknown_column = [col for col in df.columns if col != known_column_name]
    if len(unknown_column) != 1:
        raise ValueError(
            f"The DataFrame must contain exactly two columns (one of which is {known_column_name}) in order to use the rename_unknown_column() function. Columns found: {df.columns}"
        )

    # Rename the unknown column to unknown_column_rename
    logger.info(f"Renaming column {unknown_column[0]} to {unknown_column_rename}")
    df = df.rename(columns={unknown_column[0]: unknown_column_rename})
    return df
