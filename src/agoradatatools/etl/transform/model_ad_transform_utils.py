"""
This file contains utility functions that may be used across multiple transforms related to Model-AD.

Functions:
    process_genetic_info - process a gene information DataFrame into a dictionary for model details/overview
    build_gene_expression_url - build a URL linking to the gene comparison table for a given study
"""

from typing import Union
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np

from agoradatatools.etl.utils import delim_string_to_list


def process_genetic_info(
    human_transgene_allele_map_df: pd.DataFrame, allele_info_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge the allele_info data frame with the human_transgene_allele_map dataframe to fill in human gene information for
    transgenic alleles. This is necessary because some models have human transgenes, and we want to display the human
    Ensembl ID and gene symbol instead of the mouse versions.

    Args:
        human_transgene_allele_map_df (pd.DataFrame): The DataFrame containing the human transgene allele information.
        allele_info_df (pd.DataFrame): The DataFrame containing the model allele information.

    Returns:
        pd.DataFrame: The processed allele_info dataframe with mouse values overridden by human values where applicable.
    """

    # Normalize gene columns to uppercase for consistent merging
    allele_info_df["gene_upper"] = allele_info_df["modified_gene"].str.upper()
    human_transgene_allele_map_df["gene_upper"] = human_transgene_allele_map_df[
        "gene_symbol"
    ].str.upper()

    # Merge on mgi_allele_id and gene_upper to ensure we preserve different alleles
    merged_df = allele_info_df.merge(
        human_transgene_allele_map_df, on=["mgi_allele_id", "gene_upper"], how="left"
    )

    # Only override ensembl_id if we have a valid human_ensembl_id
    merged_df["ensembl_gene_id"] = merged_df["human_ensembl_id"].fillna(
        merged_df["gene_ensembl_id"]
    )

    # Only override gene_symbol if we have a valid human_ensembl_id
    merged_df["modified_gene"] = merged_df["gene_symbol"].fillna(
        merged_df["modified_gene"]
    )

    # Drop duplicates to ensure we don't have exact duplicates of the same allele
    merged_df = merged_df.drop_duplicates(
        subset=["name", "modified_gene", "allele", "mgi_allele_id"]
    )

    # Change NaN to empty strings and remove the columns we added in this function, plus the now-unused gene_ensembl_id
    # column, which was replaced by "ensembl_gene_id".
    return merged_df.drop(
        columns=["gene_upper", "human_ensembl_id", "gene_ensembl_id", "gene_symbol"]
    ).fillna("")


def build_gene_expression_url(model_row: pd.Series) -> Union[str, None]:
    """
    Creates the link-url to the gene comparison table for a given model. The default string is
    "comparison/expression?models=<model_name>".

    However, this default isn't an appropriate value for every model. By default, the gene comparison table loads with
    tissue = Hemibrain by default, and only Jax studies have hemibrain samples. For models without hemibrain data, we
    add a "categories=..." string to the URL that sets tissue = Hippocampus.

    Additionally, some UCI studies have 4 associated genotypes (2 sets of case vs control differential expression
    results), and the gene comparison table should load results for both sets of DE data. For those studies, we add
    two (or more) model names to the "models=..." part of the string.

    The exact values that should go in "categories=..." and "models=..." are pulled from columns in model_row. If the
    url_categories_value is "", the "categories=..." string is not added. If the url_models_value is "", the
    "models=..." string defaults to "models=<model_name>".

    The final url can have two different formats:
        "comparison/expression?models=..."
        "comparison/expression?categories=...&models=..."
    where:
        the models "..." could be a single model name or a comma-separated list of models, and
        the categories "..." is a string like
            "RNA%2520-%2520DIFFERENTIAL%2520EXPRESSION,Tissue%2520-%2520Hippocampus,Sex%2520-%2520Females%2520%2526%2520Males"

    The url will be None if there is no gene expression data for this model.

    Args:
        model_row (pd.Series): A single row from the model_info data frame, which must contain columns "name",
            "gene_expression", "url_categories_value", and "url_models_value". The latter two columns may be blank or
            contain strings. "gene_expression" must be True, False, or None (which is interpreted as False).

    Returns:
        a string with the completed URL, or None if there is no gene expression data for the model
    """

    # Safety check: Empty strings should be treated as None, and any NaN values should be converted to None. These
    # values should already be correct if model_row comes from the output of preprocess_model_info, but we pre-emptively
    # fix values to avoid issues with calling this function outside of that.
    model_row = model_row.replace({"": None, np.nan: None})

    categories_value = (
        # Contains the "&" at the end to separate it from the models=... statement
        f"categories={model_row['url_categories_value']}&"
        if pd.notna(model_row["url_categories_value"])
        else ""  # Only adds to URL if the url_categories_value is specified
    )
    models_value = (
        model_row["url_models_value"]  # A comma-separated list, if specified
        if pd.notna(model_row["url_models_value"])
        else model_row["name"]  # A single model name if url_models_value is blank
    )
    url = (
        f"comparison/expression?{categories_value}models={models_value}"
        if model_row["gene_expression"]
        else None
    )
    return url


def preprocess_model_info(
    model_info_df: pd.DataFrame, model_results_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Multiple transforms require the model_info and model_results_info datasets to be merged together, and perform some
    of the same preprocessing steps on the merged data (e.g. filling NaN values, adjusting jax_id formatting). We use
    this function to perform those common steps in one place.

    Both data sets have one row per model. This function merges the data on the model name ("name" column), and there
    should be a 1:1 relationship between model names in the datasets. After the merge, any models that don't have
    entries in model_results_info will have NaN values for the columns that came from that dataset, so we fill those
    NaN values with False so all values in the columns are boolean.

    We also adjust the jax_id column to preserve leading zeros, and convert the matched_controls and aliases columns
    from comma-delimited strings to lists. These operations aren't needed for every transform that uses this function
    but ensures consistency across transforms that do need these adjustments.

    Args:
        model_info_df (pd.DataFrame): The model_info dataset as a DataFrame
        model_results_df (pd.DataFrame): the model_results_info dataset DataFrame

    Returns:
        pd.DataFrame: The merged DataFrame with model information and results information combined.
    """
    merged_df = pd.merge(
        model_info_df, model_results_df, how="left", on="name", validate="one_to_one"
    )

    # Ensure jax_id preserves leading zeros by converting to string with proper formatting
    merged_df["jax_id"] = zero_pad_jax_ids(merged_df["jax_id"])

    boolean_columns = [
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ]
    merged_df[boolean_columns] = merged_df[boolean_columns].fillna(False)

    # rrid and alzforum_id should be empty strings where data is missing
    merged_df[["rrid", "alzforum_id"]] = merged_df[["rrid", "alzforum_id"]].fillna("")

    # Replace all other NaN values in other columns with None
    merged_df = merged_df.replace({np.nan: None})

    # Convert matching controls and aliases from comma-delimited strings to lists
    for col_name in ["matched_controls", "aliases"]:
        merged_df[col_name] = merged_df[col_name].apply(delim_string_to_list, delim=",")

    return merged_df


def zero_pad_jax_ids(jax_id: pd.Series) -> pd.Series:
    """
    Convert Jax IDs to strings with leading zeros preserved. Jax IDs are typically a string of numbers, so pandas reads
    them as integers (int64), which results in loss of leading zeros. This function converts the values back to strings
    and adds leading zeros if necessary.

    If any Jax IDs were missing in the input file, the column becomes a "float64" with NaN values. This causes
    undesirable behavior, because the float conversion turns the values into decimals that persist in the string
    (e.g. instead of "1234" it becomes "1234.0"). If this is the case, we first cast the column to Int64, which removes
    the decimal so the string conversion works as intended.

    Args:
        jax_id (pd.Series): A pandas Series containing Jax IDs, which may be integers or strings.

    Returns:
        pd.Series: A pandas Series containing the converted Jax IDs as strings with leading zeros preserved. Missing,
        NA, or all-whitespace values are set to "" (empty string).
    """
    if is_numeric_dtype(jax_id):
        jax_id = jax_id.astype("Int64")

    return jax_id.apply(
        lambda x: (
            str(x).strip().zfill(6) if pd.notna(x) and str(x).strip() != "" else ""
        )
    )
