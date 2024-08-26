"""Function for transforming proteomics data. This function is called on all three proteomics
data sets, although currently it only affects the LFQ data set as it is the only one with "CON__"
entries.
"""

import pandas as pd


def transform_proteomics(df: pd.DataFrame) -> pd.DataFrame:
    """Filters out rows that have "CON__" in their uniqid. This label indicates that the protein
    is a known contaminant and should be removed from the final data set. Rows with an NA uniqid
    are also removed.

    Args:
        df (pd.DataFrame]): pandas DataFrame containing proteomics data. Must contain a column
                            called "uniqid".

    Returns:
        pd.DataFrame: a DataFrame that is identical to the input DataFrame but with rows containing
                      "CON__" in the uniqid removed.
    """
    # Using "na=True" causes rows with NA uniqids to be set to True so they get removed
    remove_rows = df["uniqid"].str.contains("CON__", na=True)
    df = df.drop(df.index[remove_rows])
    return df
