"""
Shared utility functions for RNA-seq data transformations.

This module contains common functionality used by multiple RNA-seq transforms
including rna_de_aggregate and rna_de_individual.
"""

import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def filter_mouse_genes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to keep only mouse genes (ENSMUSG*), excluding human genes (ENSG*).

    Args:
        df: DataFrame with an 'ensembl_gene_id' column

    Returns:
        Filtered DataFrame containing only mouse genes
    """
    return df[df["ensembl_gene_id"].str.startswith("ENSMUSG")]


def map_jax_tissue_name(tissue: str) -> str:
    """
    Map JAX-specific tissue names to standard names.

    Currently maps "Right Cerebral Hemisphere" to "Hemibrain".

    Args:
        tissue: Original tissue name

    Returns:
        Mapped tissue name
    """
    return "Hemibrain" if tissue == "Right Cerebral Hemisphere" else tissue


def validate_and_sort_age_entries(
    age_entries: Dict,
    ensembl_gene_id: str,
    model: str,
    tissue: str,
    sex: str,
) -> Dict:
    """
    Validates and sorts age entries by their numeric value.

    Age entries are expected to be in the format 'N months' where N is an integer.
    This function validates that all age strings are properly formatted and not empty,
    then sorts them numerically.

    Args:
        age_entries: Dictionary with age strings as keys
        ensembl_gene_id: Gene identifier for error reporting
        model: Model name for error reporting
        tissue: Tissue type for error reporting
        sex: Sex category for error reporting

    Returns:
        Dictionary of age entries sorted by numeric age value

    Raises:
        ValueError: If any age string is empty, whitespace-only, or not in 'N months' format
    """
    # Validate that no age strings are empty or whitespace-only
    for age in age_entries.keys():
        age_stripped = age.strip()
        if not age_stripped:
            raise ValueError(
                f"Empty or whitespace-only age value found in data for gene '{ensembl_gene_id}', "
                f"model '{model}', tissue '{tissue}', sex '{sex}'. "
                f"Expected 'N months' format but found: '{age}'"
            )

    # Sort age entries by numeric value with error handling for format validation
    try:
        sorted_ages = dict(
            sorted(age_entries.items(), key=lambda x: int(x[0].split()[0]))
        )
    except (ValueError, IndexError) as e:
        raise ValueError(
            f"Invalid age format in data for gene '{ensembl_gene_id}', "
            f"model '{model}', tissue '{tissue}', sex '{sex}'. "
            f"Expected 'N months' format but found: {list(age_entries.keys())}. "
            f"Original error: {e}"
        ) from e

    return sorted_ages


def validate_model_group_consistency(
    genotype_label_map_df: pd.DataFrame,
) -> None:
    """
    Validate that each model has consistent model_group values.

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Raises:
        ValueError: If any model has inconsistent model_group values
    """
    inconsistent_models = (
        genotype_label_map_df.groupby("model")["model_group"]
        .nunique()
        .pipe(lambda x: x[x > 1].index.tolist())
    )
    if inconsistent_models:
        raise ValueError(
            f"Each model must have a consistent model_group value in rnaseq_genotype_label_map. "
            f"Models with inconsistent model_group values: {inconsistent_models}"
        )


def create_gene_metadata_dict(mouse_gene_metadata_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create a lookup dictionary mapping Ensembl gene IDs to gene symbols.

    Args:
        mouse_gene_metadata_df: DataFrame with 'ensembl_gene_id' and 'gene_symbol' columns

    Returns:
        Dictionary mapping ensembl_gene_id to gene_symbol
    """
    return mouse_gene_metadata_df.set_index("ensembl_gene_id")["gene_symbol"].to_dict()


def create_model_group_dict(genotype_label_map_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create a lookup dictionary mapping model names to model_group values.

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Returns:
        Dictionary mapping model to model_group
    """
    return genotype_label_map_df.groupby("model")["model_group"].first().to_dict()


def create_label_map_dict(genotype_label_map_df: pd.DataFrame) -> Dict[tuple, str]:
    """
    Create a lookup dictionary mapping (model, genotype) tuples to display labels.

    Args:
        genotype_label_map_df: DataFrame with 'model', 'genotype', and 'display_label' columns

    Returns:
        Dictionary mapping (model, genotype) to display_label
    """
    return genotype_label_map_df.set_index(["model", "genotype"])[
        "display_label"
    ].to_dict()


def log_file_processing_info(
    file_name: str,
    file_index: int,
    total_files: int,
    data_file: pd.DataFrame,
) -> None:
    """
    Log information about a file being processed.

    Args:
        file_name: Name of the file being processed
        file_index: Current file index (0-based)
        total_files: Total number of files to process
        data_file: DataFrame being processed
    """
    logger.info(
        f"Processing {file_name} ({file_index+1}/{total_files}): {len(data_file)} rows, "
        f"{len(data_file.columns)} columns, "
        f"{data_file.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
    )


def validate_data_file_not_empty(file_name: str, data_file: pd.DataFrame) -> None:
    """
    Validate that a data file is not empty.

    Args:
        file_name: Name of the file being validated
        data_file: DataFrame to validate

    Raises:
        ValueError: If the data file is empty
    """
    if len(data_file) == 0:
        raise ValueError(f"Data file {file_name} is empty")


def normalize_model_group_value(model_group: str) -> str or None:
    """
    Normalize model_group value by converting empty strings to None.

    Args:
        model_group: Model group string value

    Returns:
        None if model_group is empty string, otherwise returns model_group
    """
    return None if model_group == "" else model_group


def extract_common_metadata(
    ensembl_gene_id: str,
    tissue: str,
    gene_metadata_dict: Dict[str, str],
) -> Dict[str, Any]:
    """
    Extract common metadata fields used by both RNA transforms.

    This function handles:
    - Gene symbol lookup
    - JAX tissue name mapping

    Args:
        ensembl_gene_id: Ensembl gene identifier
        tissue: Tissue name
        gene_metadata_dict: Dictionary mapping ensembl_gene_id to gene_symbol

    Returns:
        Dictionary with extracted metadata:
            - 'ensembl_gene_id': str
            - 'gene_symbol': str (empty if not found)
            - 'tissue': str (mapped tissue name)
    """
    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": gene_metadata_dict.get(ensembl_gene_id, ""),
        "tissue": map_jax_tissue_name(tissue),
    }
