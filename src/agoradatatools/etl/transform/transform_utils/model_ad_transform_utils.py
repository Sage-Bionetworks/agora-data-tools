"""
This file contains utility functions that may be used across multiple transforms related to Model-AD.

Functions:
    process_genetic_modifications - process a genetic modifications DataFrame by updating the "ensembl_gene_id" and
        "modified_gene" columns based on human and mouse gene information
    build_transcriptomics_url - build a URL linking to the gene comparison table for a given study
    zero_pad_jax_ids - convert Jax IDs to strings with leading zeros preserved, and handle missing values appropriately
    remap_sex_labels - convert any plural sex values to singular form for consistent display
"""

from typing import Union
import pandas as pd

from agoradatatools.etl.utils import normalize_null_values


def process_genetic_modifications(
    model_genetic_modifications: pd.DataFrame,
) -> pd.DataFrame:
    """
    Processes the gene modifications DataFrame by creating a new column, "ensembl_gene_id". If the allele is a human
    transgene, "ensembl_gene_id" is filled with the value in "human_ensembl_id". Otherwise, the value is filled from
    "mouse_ensembl_id". The "modified_gene" column is updated the same way, using the human gene symbol for human
    transgenes and the mouse symbol for mouse genes.

    Each model's alleles are processed independently. Multiple entries are preserved for different alleles of the same
    gene.

    This function assumes the input data has been pre-validated so that model_genetic_modifications
    does not have any missing/NaN/none values in the "modified_gene" or "mouse_ensembl_id" columns.
    Missing values in other columns of the DataFrame are allowed, and will be normalized to None.

    Args:
        model_genetic_modifications (pd.DataFrame): The DataFrame containing the allele and
            transgene information

    Returns:
        pd.DataFrame: The processed genetic modifications DataFrame.
    """
    # Normalize missing values to None.
    model_genetic_modifications = normalize_null_values(model_genetic_modifications)

    # Only override ensembl_id if we have a valid human_ensembl_id
    model_genetic_modifications["ensembl_gene_id"] = model_genetic_modifications.apply(
        lambda row: (
            row["human_ensembl_id"]
            if pd.notna(row["human_ensembl_id"])
            else row["mouse_ensembl_id"]
        ),
        axis=1,
    )

    # Only override gene_symbol if we have a valid human_ensembl_id
    model_genetic_modifications["modified_gene"] = model_genetic_modifications.apply(
        lambda row: (
            row["human_gene_symbol"]
            if pd.notna(row["human_ensembl_id"])
            else row["modified_gene"]
        ),
        axis=1,
    )

    # Drop duplicates for the same model to ensure we don't have exact duplicates of the same allele
    model_genetic_modifications = model_genetic_modifications.drop_duplicates(
        subset=["name", "ensembl_gene_id", "modified_gene", "allele", "mgi_allele_id"]
    )

    return model_genetic_modifications[
        [
            "name",
            "modified_gene",
            "ensembl_gene_id",
            "allele",
            "allele_type",
            "mgi_allele_id",
        ]
    ]


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
    url_categories_value is None or "", the "categories=..." string is not added. If the url_models_value is None or "",
    the "models=..." string defaults to "models=<model_name>".

    The final url can have two different formats:
        "comparison/expression?models=..."
        "comparison/expression?categories=...&models=..."
    where:
        the models "..." could be a single model name or a comma-separated list of models, and
        the categories "..." is a string like
            "RNA%2520-%2520DIFFERENTIAL%2520EXPRESSION,Tissue%2520-%2520Hippocampus,Sex%2520-%2520Females%2520%2526%2520Males"

    The url will be None if there is no transcriptomics data for this model.

    Args:
        model_row (pd.Series): A single row from the model_info data frame, which must contain columns "name",
            "transcriptomics", "url_categories_value", and "url_models_value". The latter two columns may be None or
            contain strings. "transcriptomics" must be True or False. It is assumed that normalize_null_values has
            already been called on this data so that all missing values used in this function are None, not NA or empty
            strings.

    Returns:
        a string with the completed URL, or None if there is no transcriptomics data for the model
    """
    categories_value = (
        # Contains the "&" at the end to separate it from the models=... statement
        f"categories={model_row['url_categories_value']}&"
        if model_row["url_categories_value"]  # must not be "" or None
        else ""  # Only adds to URL if the url_categories_value is specified
    )
    models_value = (
        model_row["url_models_value"]  # A comma-separated list, if specified
        if model_row["url_models_value"]  # must not be "" or None
        else model_row["name"]  # A single model name if url_models_value is blank
    )
    url = (
        f"comparison/expression?{categories_value}models={models_value}"
        if model_row["transcriptomics"]
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

    Raises:
        ValueError: If any input Jax IDs can not be cast to Int64
        ValueError: If any zero-padded Jax IDs contain non-digit characters, or missing values are not empty strings.
    """
    jax_id = (
        jax_id.astype("Int64")
        # Prevent .apply from casting back to float
        .astype("object").apply(lambda x: (str(x).zfill(6) if pd.notna(x) else ""))
    )

    # Check that non-missing values contain only digits and are at least 6 characters long
    validate_jax_ids(jax_id)

    return jax_id


def validate_jax_ids(jax_id: pd.Series) -> None:
    """
    Validates all Jax IDs in a pandas Series are in the correct format. A valid Jax ID should contain only digits and be
    at least 6 characters long, or be an empty string ("") to represent missing values.

    Args:
        jax_id (pd.Series): A pandas Series containing Jax ID strings to validate.

    Returns:
        None

    Raises:
        ValueError: If any Jax IDs contain non-digit characters, are less than 6 characters long, or are not empty
        strings.
    """
    # Regex description:
    # \d{6}\d* matches strings that contain only digits and are at least 6 characters long
    # ^$ matches empty strings
    if jax_id.isna().any() or not jax_id.str.fullmatch(r"\d{6}\d*|^$").all():
        raise ValueError(
            "Jax IDs must be strings that contain only digits and are at least 6 characters long, or must be empty strings"
        )
    return None


def remap_sex_labels(sex: pd.Series) -> pd.Series:
    """
    Converts plural sex values ("Females" or "Males") to singular form ("Female" or "Male"). Sex values that are
    already singular, and any other value, are not modified.

    Args:
        sex (pd.Series): A pandas Series containing sex labels that may need to be converted.

    Returns:
        pd.Series: A pandas Series containing sex labels in only the singular form.
    """
    return sex.copy().replace({"Females": "Female", "Males": "Male"})
