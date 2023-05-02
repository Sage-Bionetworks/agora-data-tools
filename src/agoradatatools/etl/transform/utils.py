import numpy as np
import pandas as pd


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
    df: pd.DataFrame, grouping: str, new_column: str, drop_columns: list = []
) -> pd.DataFrame:
    """Collapses the provided DataFrame into 2 columns:
    1. The grouping column
    2. A column containing a nested dictionary with the data from the rest of the DataFrame

    Args:
        df (pd.DataFrame): DataFrame to be collapsed
        grouping (str): The column that you want to group by
        new_column (str): the new column created to contain the nested dictionaries created
        drop_columns (list, optional): List of columns to leave out of the new nested dictionary. Defaults to [].

    Returns:
        pd.DataFrame: New 2 column DataFrame with group and nested dictionaries
    """
    return (
        df.groupby(grouping)
        .apply(
            lambda row: row.replace({np.nan: None})
            .drop(columns=drop_columns)
            .to_dict("records")
        )
        .reset_index()
        .rename(columns={0: new_column})
    )
