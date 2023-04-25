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


def calculate_distribution(df: pd.DataFrame, col: str, is_scored, upper_bound) -> dict:
    if is_scored:
        df = df[df[is_scored] == "Y"]  # df does not have the isscored
    else:
        df = df[df.isin(["Y"]).any(axis=1)]

    if df[col].dtype == object:
        df = df.copy()  # Necessary to prevent SettingWithCopy warning
        df[col] = df[col].astype(float)

    obj = {}

    # In order to smooth out the bins and make sure the entire range from 0
    # to the theoretical maximum value has been found, we create a copy of the
    # column with both 0 and that maximum value added to it.  We use the copy to calculate
    # distributions and bins, and subtract the values at the end

    distribution = pd.concat([df[col], pd.Series([0, upper_bound])], ignore_index=True)

    obj["distribution"] = list(
        pd.cut(
            distribution, bins=10, precision=3, include_lowest=True, right=True
        ).value_counts(sort=False)
    )
    obj["distribution"][
        0
    ] -= 1  # since this was calculated with the artificial 0 value, we subtract it
    obj["distribution"][
        -1
    ] -= 1  # since this was calculated with the artificial upper_bound, we subtract it

    discard, obj["bins"] = list(
        pd.cut(distribution, bins=10, precision=3, retbins=True)
    )
    obj["bins"] = np.around(obj["bins"].tolist()[1:], 2)
    base = [0, *obj["bins"][:-1]]
    obj["bins"] = zip(base, obj["bins"])
    obj["bins"] = list(obj["bins"])

    obj["min"] = np.around(df[col].min(), 4)
    obj["max"] = np.around(df[col].max(), 4)
    obj["mean"] = np.around(df[col].mean(), 4)
    obj["first_quartile"] = np.around(
        df[col].quantile(q=0.25, interpolation="midpoint")
    )
    obj["third_quartile"] = np.around(
        df[col].quantile(q=0.75, interpolation="midpoint")
    )

    return obj


def count_grouped_total(
    df: pd.DataFrame, grouping: [str, list], input_colname: str, output_colname: str
) -> pd.DataFrame:
    """For each unique item/combination in the column(s) specified by grouping,
    counts the number of unique items in the column [input_colname] that
    correspond to that grouping. The calculated counts are put in a new
    column and named with [output_colname].
    Args:
        df (pd.DataFrame): contains columns listed in grouping and
                           input_colname. May contain other columns as well, but
                           these will be dropped from the returned data frame.
        grouping (str or list): a string with a single column name, or a list of
                                strings for multiple column names
        input_colname (str): the name of the column to count
        output_colname (str): the name of the new column with calculated counts
    Returns:
        pd.DataFrame: a data frame containing the grouping column(s) and a
                      new column for output_colname, which contains the count of
                      unique items in input_colname for each grouping item.
    """
    df = (
        df.groupby(grouping)[input_colname]
        .nunique()
        .reset_index()
        .rename(columns={input_colname: output_colname})
    )
    return df


def transform_overall_scores(df: pd.DataFrame) -> pd.DataFrame:
    interesting_columns = [
        "ensg",
        "hgnc_gene_id",
        "overall",
        "geneticsscore",
        "omicsscore",
        "literaturescore",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics", "isscored_lit"]
    mapping = dict(zip(interesting_columns[3:], scored))

    for field, is_scored in mapping.items():
        df.loc[lambda row: row[is_scored] == "N", field] = np.nan

    # LiteratureScore is a string in the source file, so convert to numeric
    df["literaturescore"] = pd.to_numeric(df["literaturescore"])

    # Remove identical rows (see AG-826)
    return df[interesting_columns].drop_duplicates()


def join_datasets(left: pd.DataFrame, right: pd.DataFrame, how: str, on: str):
    return pd.merge(left=left, right=right, how=how, on=on)
