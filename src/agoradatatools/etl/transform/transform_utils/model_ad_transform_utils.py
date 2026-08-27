"""
This file contains utility functions that may be used across multiple transforms related to Model-AD.

Functions:
    process_genetic_info - process a gene information DataFrame into a dictionary for model details/overview
    build_results_url - build a URL linking to the gene/protein comparison table for a given study
    zero_pad_jax_ids - convert Jax IDs to strings with leading zeros preserved, and handle missing values appropriately
    remap_sex_labels - convert any plural sex values to singular form for consistent display
"""

from typing import Any, Dict, List, Union
import pandas as pd

from agoradatatools.etl.utils import normalize_null_values


def process_genetic_info(
    model_genetic_modifications: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Processes the gene information DataFrame. If the allele is a human transgene,
    replace the ensembl_id with the human one. Each model's alleles are processed independently.
    Multiple entries are preserved for different alleles of the same gene.

    This function assumes the input data has been pre-validated so that model_genetic_modifications
    does not have any missing/NaN/none values in the "modified_gene" or "mouse_ensembl_id" columns.
    Missing values in other columns of the DataFrame are allowed, and will be normalized to None.

    Args:
        model_genetic_modifications (pd.DataFrame): The DataFrame containing the allele and
            transgene information

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the processed gene information.
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

    # Drop duplicates to ensure we don't have exact duplicates of the same allele
    model_genetic_modifications = model_genetic_modifications.drop_duplicates(
        subset=["modified_gene", "allele", "mgi_allele_id"]
    )

    return model_genetic_modifications[
        ["modified_gene", "ensembl_gene_id", "allele", "allele_type", "mgi_allele_id"]
    ].to_dict(orient="records")


def build_expression_results_url(
    model_row: pd.Series, result_type: str = "transcriptomics"
) -> Union[str, None]:
    """
    Creates the link-url to the comparison table for a given model and result type. Currently supported result
    types are "transcriptomics" and "proteomics", where the default is "transcriptomics".

    The URL base is "comparison/expression?" with 'categories' and 'models' query parameters. The final URL format will
    look something like this:
        "comparison/expression?categories=...&models=..."

    The 'categories' parameter uses the model's url_categories_value if specified; otherwise a default value is used.
    For both transcriptomics and proteomics, the default values include "Tissue - Hemibrain", but this may not be
    appropriate for all models. For example, only JAX models currently have Hemibrain data, so for non-JAX models we
    use 'categories' to specify the tissue type to Hippocampus instead.

    The 'models' parameter always includes the model name. Additional model names can be included if specified by the
    model's url_models_value. For example,some UCI studies have 4 associated genotypes (2 sets of case vs control
    differential expression results), and the comparisons table should load results for both sets of DE data. For those
    studies, we add two (or more) model names to the 'models' query parameter.

    The url will be None if the result_type is unsupported or there is no result data for this model.

    Args:
        model_row (pd.Series): A single row from the model_info data frame, which must contain columns:
            * name
            * <result_type> (must be True or False)
            * <result_type>_url_categories_value (must be None or non-empty string)
            * <result_type>_url_models_value (must be None or non-empty string)
            It is assumed that normalize_null_values has already been called so that all missing values are None, not
            NA or empty strings.
        result_type (str): The type of result data to build the URL for (default: "transcriptomics")

    Returns:
        a string with the completed URL, or None if there is no data for the model or if the result_type is unsupported
    """
    default_categories = {
        "transcriptomics": "RNA%2520-%2520DIFFERENTIAL%2520EXPRESSION,Tissue%2520-%2520Hemibrain",
        "proteomics": "PROTEIN%2520-%2520DIFFERENTIAL%2520EXPRESSION,Tissue%2520-%2520Hemibrain",
    }

    # Return None for unsupported result types instead of raising an error.
    if result_type not in default_categories:
        return None

    if not model_row[result_type]:
        return None

    categories_value = (
        model_row[f"{result_type}_url_categories_value"]
        if model_row[f"{result_type}_url_categories_value"]  # must not be "" or None
        else default_categories[result_type]
    )

    # Combine the model name with any additional models specified, but only keep the unique values. For the additional
    # models, ignore any leading/trailing whitespace, and empty values. For best test reproducibility, sort the names
    # so that the order is consistent.
    other_models_to_list = model_row.get(f"{result_type}_url_models_value") or ""
    models_group = {model_row["name"]} | {
        m.strip() for m in other_models_to_list.split(",") if m.strip()
    }
    models_value = ",".join(sorted(models_group))
    return f"comparison/expression?categories={categories_value}&models={models_value}"


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
