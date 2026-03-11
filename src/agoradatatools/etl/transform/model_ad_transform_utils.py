"""
This file contains utility functions that may be used across multiple transforms related to Model-AD.

Functions:
    process_genetic_info - process a gene information DataFrame into a dictionary for model details/overview
    build_gene_expression_url - build a URL linking to the gene comparison table for a given study
"""

from typing import Any, Dict, List, Union
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np

from agoradatatools.etl.utils import delim_string_to_list, normalize_null_values


def process_genetic_info(
    human_transgene_allele_map_df: pd.DataFrame,
    model_alleles: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Processes the gene information DataFrame. If the allele is a human transgene,
    replace the ensembl_id with the human one. Each model's alleles are processed independently.
    Multiple entries are preserved for different alleles of the same gene.

    Args:
        human_transgene_allele_map_df (pd.DataFrame): The DataFrame containing the human transgene allele information.
        model_alleles (pd.DataFrame): The DataFrame containing the model allele information.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the processed gene information.
    """
    # Copy dataframes to avoid modifying originals
    # Using copy() to avoid warning: A value is trying to be set on a copy of a slice from a DataFrame.
    # Warning appears even if using .loc to set the value.
    model_alleles = model_alleles.copy()
    human_transgene_allele_map_df = human_transgene_allele_map_df.copy()

    # Normalize gene columns to uppercase for consistent merging
    model_alleles["gene_upper"] = model_alleles["modified_gene"].str.upper()
    human_transgene_allele_map_df["gene_upper"] = human_transgene_allele_map_df[
        "gene_symbol"
    ].str.upper()

    # Merge on mgi_allele_id and gene_upper to ensure we preserve different alleles
    merged_df = model_alleles.merge(
        human_transgene_allele_map_df[
            ["mgi_allele_id", "gene_upper", "human_ensembl_id", "gene_symbol"]
        ],
        on=["mgi_allele_id", "gene_upper"],
        how="left",
    )

    # Only override ensembl_id if we have a valid human_ensembl_id
    merged_df["ensembl_gene_id"] = merged_df.apply(
        lambda row: (
            row["human_ensembl_id"]
            if pd.notna(row["human_ensembl_id"])
            else row["gene_ensembl_id"]
        ),
        axis=1,
    )

    # Only override gene_symbol if we have a valid human_ensembl_id
    merged_df["modified_gene"] = merged_df.apply(
        lambda row: (
            row["gene_symbol"]
            if pd.notna(row["human_ensembl_id"])
            else row["modified_gene"]
        ),
        axis=1,
    )

    # Drop duplicates to ensure we don't have exact duplicates of the same allele
    merged_df = merged_df.drop_duplicates(
        subset=["modified_gene", "allele", "mgi_allele_id"]
    )

    return merged_df[
        ["modified_gene", "ensembl_gene_id", "allele", "allele_type", "mgi_allele_id"]
    ].to_dict(orient="records")


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
    model_info_df: pd.DataFrame, model_results_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Multiple transforms load the model_info data frame, often merged with the model_results_info dataset, and perform
    some of the same preprocessing steps on the merged data (e.g. filling NaN values, adjusting jax_id formatting). We
    use this function to perform those common steps in one place.

    Merging the model_results_df is optional. If model_results_df is provided, this function merges the data on the
    model name ("name" column), and there should be a 1:1 relationship between model names in the datasets.

    Other adjustments after (optional) merge:
        1. NaN values are filled with either None, "", or False, depending on column type. If a merge occurred, there
           will be extra boolean columns with NaNs that should be filled with False so every value in those columns is
           boolean.
        2. Change the jax_id column to strings and pad with leading zeros up to a string length of 6 characters
        3. Convert the matched_controls and aliases columns from comma-separated strings to lists

    These operations aren't needed for every transform that uses model_info but ensures consistency across transforms
    that do need these adjustments.

    Args:
        model_info_df (pd.DataFrame): The model_info dataset as a DataFrame
        model_results_df (pd.DataFrame): Optional: the model_results_info dataset DataFrame. Defaults to None.

    Returns:
        pd.DataFrame: The (optionally merged) DataFrame with adjusted and normalized values. If model_results_df was
        provided, the data from that df will be included in the output DataFrame.
    """
    if any(model_info_df["name"].duplicated()):
        duplicates = model_info_df["name"][
            model_info_df["name"].duplicated()
        ].drop_duplicates()
        raise ValueError(
            f"model_info has duplicated rows for model(s): {list(duplicates)}"
        )

    if model_results_df is not None:
        merged_df = pd.merge(
            model_info_df,
            model_results_df,
            how="left",
            on="name",
            validate="one_to_one",
        )
    else:
        merged_df = model_info_df.copy()

    # Ensure jax_id preserves leading zeros by converting to string with proper formatting
    if "jax_id" in merged_df.columns:
        merged_df["jax_id"] = zero_pad_jax_ids(merged_df["jax_id"])

    # Boolean columns from model_results_df
    boolean_columns = [
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ]

    # rrid and alzforum_id should be empty strings where data is missing
    string_columns = ["rrid", "alzforum_id"]

    merged_df = normalize_null_values(
        merged_df,
        # Fill NAs in these 4 columns with False, if they exist in the merged_df
        boolean_columns=[col for col in boolean_columns if col in merged_df.columns],
        empty_string_columns=[
            col for col in string_columns if col in merged_df.columns
        ],
    )

    # Convert matching controls and aliases from comma-delimited strings to lists
    for col_name in ["matched_controls", "aliases"]:
        if col_name in merged_df.columns:
            merged_df[col_name] = merged_df[col_name].apply(
                delim_string_to_list, delim=","
            )

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
