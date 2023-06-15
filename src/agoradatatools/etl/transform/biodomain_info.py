import pandas as pd


def transform_biodomain_info(datasets: dict) -> pd.DataFrame:
    """Takes dictionary of dataset DataFrames, extracts the genes_biodomains
    DataFrame, gets a unique list of biodomain names, and outputs the list as
    a single-column DataFrame with column "name".

    Args:
        datasets (dict[str, pd.DataFrame]): dictionary of dataset names mapped to their DataFrame

    Returns:
        pd.DataFrame: 1-column DataFrame with column "name"
    """
    genes_biodomains = datasets["genes_biodomains"]
    biodomain_info = (
        genes_biodomains["name"]
        .dropna()
        .drop_duplicates()
        .reset_index()
        .drop(columns="index")
        .sort_values(by="name", ignore_index=True)
    )

    return biodomain_info
