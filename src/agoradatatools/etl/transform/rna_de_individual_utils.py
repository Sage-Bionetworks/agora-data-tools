"""
RNA DE Individual Transform Utility Functions

This module contains utility functions extracted from the rna_de_individual transform
for better code organization. These functions are currently used exclusively by the
rna_de_individual transform, but are structured in a way that allows for potential
future reuse by other RNA-seq transforms if needed.

Key Functions:
    filter_mouse_genes: Filter DataFrame to keep only mouse genes (ENSMUSG*)
    convert_to_sentence_case: Convert text to sentence case (first letter capitalized)
    map_jax_tissue_name: Map JAX-specific tissue names to standard names and apply sentence case
    validate_model_group_consistency: Validate that each model has consistent model_group values
    create_gene_metadata_dict: Create a lookup dictionary mapping Ensembl gene IDs to gene symbols
    create_genotype_metadata_dict: Create a lookup dictionary mapping (model, genotype) tuples to their metadata
    log_file_processing_info: Log information about a file being processed
    validate_data_file_not_empty: Validate that a data file is not empty
    normalize_model_group_value: Normalize model_group value by converting empty strings to None
    extract_common_metadata: Extract common metadata fields for RNA transforms
"""

import pandas as pd
from typing import Dict, Any, List
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


def convert_to_sentence_case(text: str) -> str:
    """
    Convert text to sentence case (first letter capitalized, rest lowercase).

    Handles empty strings and preserves multi-word formatting.

    Args:
        text: Text to convert

    Returns:
        Text in sentence case, or empty string if input is empty

    Examples:
        >>> convert_to_sentence_case("CORTEX")
        "Cortex"
        >>> convert_to_sentence_case("hippocampus")
        "Hippocampus"
        >>> convert_to_sentence_case("M")
        "M"
    """
    if not text:
        return text
    return text.capitalize()


def map_jax_tissue_name(tissue: str) -> str:
    """
    Map JAX-specific tissue names to standard names and apply sentence case.

    First applies special mappings (e.g., "Right Cerebral Hemisphere" to "Hemibrain"),
    then converts the result to sentence case.

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
    # Apply special mapping first
    if tissue == "Right Cerebral Hemisphere":
        return "Hemibrain"

    # Then apply sentence case to the tissue name
    return convert_to_sentence_case(tissue)


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


def create_genotype_metadata_dict(
    genotype_label_map_df: pd.DataFrame,
    include_result_order: bool = False,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    """
    Create a unified lookup dictionary mapping (model, genotype) pairs to their metadata.

    This function builds a comprehensive data structure that enables efficient lookups of
    genotype information during data processing. By grouping all genotype-related metadata
    together, we avoid multiple DataFrame iterations and provide O(1) lookup time for
    genotype properties.

    The effective_model_group is computed when include_result_order=True to handle cases
    where models belong to a model_group for display purposes. When a model has a
    model_group defined, that becomes the effective grouping; otherwise, the model name
    itself serves as the grouping key.

    Args:
        genotype_label_map_df: DataFrame containing model, genotype, display_label,
            and model_group columns. If include_result_order=True, must also contain
            result_order column.
        include_result_order: Whether to include result_order and effective_model_group
            fields in the metadata. Set to True for individual transform, False for
            aggregate transform.

    Returns:
        Dictionary mapping (model, genotype) tuples to a dict containing:
            - 'display_label': str, human-readable label for the genotype
            - 'model_group': str, model group name (empty string if none)
            - 'result_order': int, ordering value for display (only if include_result_order=True)
            - 'effective_model_group': str, model_group if present, otherwise model name
              (only if include_result_order=True)

    Example:
        >>> df = pd.DataFrame({
        ...     'model': ['Model_A', 'Model_A'],
        ...     'genotype': ['Tg', 'Wt'],
        ...     'display_label': ['Transgenic', 'Wildtype'],
        ...     'model_group': ['Group1', 'Group1'],
        ...     'result_order': [2, 1]
        ... })
        >>> metadata = create_genotype_metadata_dict(df, include_result_order=True)
        >>> metadata[('Model_A', 'Tg')]
        {
            'display_label': 'Transgenic',
            'model_group': 'Group1',
            'result_order': 2,
            'effective_model_group': 'Group1'
        }
    """
    genotype_metadata = {}
    for _, row in genotype_label_map_df.iterrows():
        model = row["model"]
        genotype = row["genotype"]
        model_group = row["model_group"]

        metadata = {
            "display_label": row["display_label"],
            "model_group": model_group,
        }

        if include_result_order:
            metadata["result_order"] = int(row["result_order"])
            # Compute effective_model_group: use model_group if present, otherwise model
            metadata["effective_model_group"] = model_group if model_group else model

        genotype_metadata[(model, genotype)] = metadata

    return genotype_metadata


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


def normalize_model_group_value(model_group) -> str or None:
    """
    Normalize model_group value by converting empty strings or NaN to None.

    Args:
        model_group: Model group value (string or NaN from a pandas merge)

    Returns:
        None if model_group is an empty string or NaN, otherwise returns model_group
    """
    if pd.isna(model_group) or model_group == "":
        return None
    return model_group


def extract_common_metadata(
    ensembl_gene_id: str,
    tissue: str,
    gene_metadata_dict: Dict[str, str],
) -> Dict[str, Any]:
    """
    Extract common metadata fields for RNA transforms.

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


def preprocess_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    file_index: int,
    total_files: int,
    data_file_required_columns: List[str],
) -> pd.DataFrame:
    """
    Preprocess a single data file with common validation and transformation steps.

    Applies the same preprocessing as process_data_files (validation, filtering,
    rounding) but returns the preprocessed DataFrame directly,
    allowing callers to accumulate and concatenate multiple files before processing.

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
    return data_file
