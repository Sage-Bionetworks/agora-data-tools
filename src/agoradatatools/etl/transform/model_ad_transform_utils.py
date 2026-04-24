"""
This file contains utility functions that may be used across multiple transforms related to Model-AD.

Functions:
    process_genetic_info - process a gene information DataFrame into a dictionary for model details/overview
    build_transcriptomics_url - build a URL linking to the gene comparison table for a given study
    zero_pad_jax_ids - convert Jax IDs to strings with leading zeros preserved, and handle missing values appropriately
"""

from typing import Any, Dict, List, Union
import pandas as pd


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


def build_transcriptomics_url(model_row: pd.Series) -> Union[str, None]:
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
    categories_value = (
        # Contains the "&" at the end to separate it from the models=... statement
        f"categories={model_row['url_categories_value']}&"
        if pd.notna(model_row["url_categories_value"])
        and len(model_row["url_categories_value"]) > 0  # must not be ""
        else ""  # Only adds to URL if the url_categories_value is specified
    )
    models_value = (
        model_row["url_models_value"]  # A comma-separated list, if specified
        if pd.notna(model_row["url_models_value"])
        and len(model_row["url_models_value"]) > 0  # must not be ""
        else model_row["name"]  # A single model name if url_models_value is blank
    )
    url = (
        f"comparison/expression?{categories_value}models={models_value}"
        if bool(model_row["transcriptomics"])
        else None
    )
    return url


def zero_pad_jax_ids(jax_id: pd.Series) -> pd.Series:
    """
    Convert Jax IDs to strings with leading zeros preserved. Jax IDs are typically a string of numbers, so pandas reads
    them as integers (int64), which results in loss of leading zeros. This function converts the values back to strings
    and adds leading zeros if necessary.

    If any Jax IDs were missing in the input file, the column becomes a "float64" with NaN values. This causes
    undesirable behavior, because the float conversion turns the values into decimals that persist in the string
    (e.g. instead of "1234" it becomes "1234.0"). To solve this, we first cast the column to Int64, which removes
    the decimal so the string conversion works as intended. Int64 is a nullable integer type that can handle missing
    values (NaN or None) without converting the entire column to float.

    This function assumes that all Jax IDs are either integers, strings with integer values, or missing (NaN or None),
    which can all be cast to Int64.

    Args:
        jax_id (pd.Series): A pandas Series containing Jax IDs, which may be integers, strings of integers, or NaN/None.

    Returns:
        pd.Series: A pandas Series containing the converted Jax IDs as strings with leading zeros preserved. None or NaN
        values are set to "" (empty string).
    """
    return (
        jax_id.astype("Int64")
        .apply(lambda x: (str(x).zfill(6) if pd.notna(x) else ""))
        .astype("O")  # Force object type to prevent empty columns remaining as Int64
    )
