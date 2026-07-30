"""
RNA DE Individual Transform Utility Functions

This module contains utility functions extracted from the rna_de_individual transform
for better code organization. These functions are currently used exclusively by the
rna_de_individual transform, but are structured in a way that allows for potential
future reuse by other RNA-seq transforms if needed.

Key Functions:
    filter_to_mouse_genes: Filter DataFrame to keep only mouse genes (ENSMUSG*)
    validate_model_group_consistency: Validate that each model has consistent model_group values
    create_gene_metadata_dict: Create a lookup dictionary mapping Ensembl gene IDs to gene symbols
    log_file_processing_info: Log information about a file being processed
    validate_data_file_not_empty: Validate that a data file is not empty
    preprocess_data_file: Apply common validation and transformation steps to a single data file
"""

import logging
from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    ColumnRule,
)

from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)

logger = logging.getLogger(__name__)


def filter_to_mouse_genes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to keep only mouse genes (ENSMUSG*), excluding human genes (ENSG*).

    Args:
        df: DataFrame with an 'ensembl_gene_id' column

    Returns:
        Filtered DataFrame containing only mouse genes
    """
    return df[df["ensembl_gene_id"].str.startswith("ENSMUSG")].copy()


def validate_model_group_consistency(
    genotype_label_map_df: pd.DataFrame,
) -> None:
    """
    Validate that each model has consistent model_group values.
    Each model should map to exactly one unique model_group; having multiple
    different model_group values for the same model indicates a data quality issue.

    None/NaN values are counted as a single distinct value (i.e. "no group assigned")
    rather than being excluded from the uniqueness check.

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Raises:
        ValueError: If any model has inconsistent model_group values
    """
    inconsistent_models = (
        genotype_label_map_df.groupby("model")["model_group"]
        .nunique(dropna=False)
        .pipe(lambda x: x[x > 1].index.tolist())
    )
    if inconsistent_models:
        raise ValueError(
            f"Each model must have a consistent model_group value in genotype_label_map. "
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
    data_file_column_rules: Dict[str, List[ColumnRule]],
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
        data_file_column_rules: Per-column content rules to validate via
            check_column_rules. Keys are column names; values are lists of ColumnRule
            objects. Rules are checked after required-column validation.

    Returns:
        Preprocessed DataFrame with mouse genes only, tissue names mapped and
        sentence-cased, plural sex values mapped to singular display labels, and
        numeric values rounded to 5 decimal places.

    Raises:
        ValueError: If the file is empty, missing required columns, or any column
            value rule is violated.
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)
    validate_data_file_not_empty(file_name, data_file)
    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )
    check_column_rules({file_name: data_file}, {file_name: data_file_column_rules})
    data_file = filter_to_mouse_genes(data_file)
    # Map JAX-specific names from Right Cerebral Hemisphere -> Hemibrain
    # To add a new multi-word mapping, insert another .str.replace() call in the chain.
    data_file["tissue"] = data_file["tissue"].str.replace(
        "Right Cerebral Hemisphere", "Hemibrain", regex=False
    )
    # Map plural source sex values to the singular display form
    data_file["sex"] = remap_sex_labels(data_file["sex"])
    data_file["expression"] = data_file["expression"].astype(float)
    data_file = data_file.round(decimals=5)
    data_file["individualid"] = data_file["individualid"].astype(str)
    return data_file
