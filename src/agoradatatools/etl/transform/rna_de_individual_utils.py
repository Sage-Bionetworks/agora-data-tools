"""
RNA DE Individual Transform Utility Functions

This module contains utility functions extracted from the rna_de_individual transform
for better code organization. These functions are currently used exclusively by the
rna_de_individual transform, but are structured in a way that allows for potential
future reuse by other RNA-seq transforms if needed.

Key Functions:
    filter_mouse_genes: Filter DataFrame to keep only mouse genes (ENSMUSG*)
    map_jax_tissue_name: Map JAX-specific tissue names to standard names and apply sentence case
    validate_model_group_consistency: Validate that each model has consistent model_group values
    create_gene_metadata_dict: Create a lookup dictionary mapping Ensembl gene IDs to gene symbols
    prepare_genotype_label_map_df: Enrich the genotype label map DataFrame with effective_model_group
    log_file_processing_info: Log information about a file being processed
    validate_data_file_not_empty: Validate that a data file is not empty
"""

import pandas as pd
from typing import Dict, List
import logging

from agoradatatools.etl.utils import check_required_datasets_and_columns

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
    Map JAX-specific tissue names to standard names and apply sentence case.

    First applies special mappings (e.g., "Right Cerebral Hemisphere" to "Hemibrain"),
    then converts the result to sentence case via str.capitalize().

    Note: str.capitalize() lowercases every character after the first, so only
    single-word tissue names (or already-mapped names like "Hemibrain") are safe
    to pass through. Any new multi-word tissue name that needs capitalisation on
    each word should be added to the special-mapping block above.

    Args:
        tissue: Original tissue name

    Returns:
        Mapped and sentence-cased tissue name

    Examples:
        >>> map_jax_tissue_name("Right Cerebral Hemisphere")
        "Hemibrain"
        >>> map_jax_tissue_name("hippocampus")
        "Hippocampus"
        >>> map_jax_tissue_name("CORTEX")
        "Cortex"
    """
    if tissue == "Right Cerebral Hemisphere":
        return "Hemibrain"

    return tissue.capitalize()


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

    Note: This function creates a dictionary to speed up processing by avoiding
    repeated DataFrame lookups during iteration over large datasets.

    Args:
        mouse_gene_metadata_df: DataFrame with 'ensembl_gene_id' and 'gene_symbol' columns

    Returns:
        Dictionary mapping ensembl_gene_id to gene_symbol
    """
    return mouse_gene_metadata_df.set_index("ensembl_gene_id")["gene_symbol"].to_dict()


def prepare_genotype_label_map_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the genotype label map DataFrame with an effective_model_group column.

    Computes effective_model_group as model_group when a non-empty value is present,
    otherwise falls back to the model name. Fills all remaining NaN values with empty
    strings and casts result_order to int.

    Args:
        df: Raw rnaseq_genotype_label_map DataFrame with columns: model, genotype,
            display_label, model_group, result_order

    Returns:
        Enriched DataFrame with an added effective_model_group column, NaN values
        replaced by empty strings, and result_order cast to int.

    Examples:
        >>> df = pd.DataFrame({
        ...     'model': ['Model_A', 'Model_B'],
        ...     'genotype': ['Tg', 'Carrier'],
        ...     'display_label': ['Transgenic', 'Model_B'],
        ...     'model_group': [None, 'GroupX'],
        ...     'result_order': [2, 1],
        ... })
        >>> result = prepare_genotype_label_map_df(df)
        >>> result['effective_model_group'].tolist()
        ['Model_A', 'GroupX']
    """
    df = df.copy()
    # Replace empty strings with NaN before computing effective_model_group so that
    # both NaN and "" model_group values fall back to the model name.
    df["effective_model_group"] = (
        df["model_group"].replace("", pd.NA).fillna(df["model"])
    )
    df = df.fillna("")
    df["result_order"] = df["result_order"].astype(int)
    return df


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


def preprocess_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    file_index: int,
    total_files: int,
    data_file_required_columns: List[str],
) -> pd.DataFrame:
    """
    Preprocess a single data file with common validation and transformation steps.

    Returns the preprocessed DataFrame so callers can accumulate and concatenate
    results across multiple files before further processing.

    Args:
        file_name: Name of the file being processed
        data_file: DataFrame to preprocess
        file_index: Index of this file in the processing sequence (0-based)
        total_files: Total number of files being processed
        data_file_required_columns: List of column names that must be present

    Returns:
        Preprocessed DataFrame with mouse genes only and numeric values rounded to 5
        decimal places.

    Raises:
        ValueError: If the file is empty or missing required columns.
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)
    validate_data_file_not_empty(file_name, data_file)
    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )
    data_file = filter_mouse_genes(data_file)
    data_file = data_file.round(decimals=5)
    data_file["individualid"] = data_file["individualid"].astype(str)
    return data_file
