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


def validate_model_group_consistency(
    genotype_label_map_df: pd.DataFrame,
) -> None:
    """
    Validate that each model has consistent model_group values.
    Each model should map to exactly one unique model_group; having multiple
    different model_group values for the same model indicates a data quality issue.

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

    Note: This function (and other similar lookup dict functions below) creates a
    dictionary to speed up processing by avoiding repeated DataFrame lookups during
    iteration over large datasets.

    Args:
        mouse_gene_metadata_df: DataFrame with 'ensembl_gene_id' and 'gene_symbol' columns

    Returns:
        Dictionary mapping ensembl_gene_id to gene_symbol
    """
    return mouse_gene_metadata_df.set_index("ensembl_gene_id")["gene_symbol"].to_dict()


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
