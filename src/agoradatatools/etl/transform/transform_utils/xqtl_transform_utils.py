"""
xQTL Transform Utility Functions

This file contains utility functions that may be used across multiple transforms related to the xQTL Explorer.

Functions:
    resolve_cell_labels -
    build_location -
    build_gene_lookup_map -
    map_and_log_gene_ids -
"""
from typing import Dict
import logging

import pandas as pd

logger = logging.getLogger(__name__)

def resolve_cell_labels(key, label_map_df) -> Dict[str, str]:
    cell_type_labels = label_map_df.loc[
        label_map_df["cell_subclass_short"].str.lower() == key.lower()
        ].iloc[0].to_dict()
    logger.info(f"Resolved cell type labels for key {key} : {cell_type_labels}")
    return cell_type_labels

def build_location(df, chr_col, start_col, end_col) -> list[dict]:
    loc = pd.DataFrame({
        "chr":   df[chr_col].astype(str),    # string to allow X / Y
        "start": df[start_col].astype(int),
        "end":   df[end_col].astype(int),
    })
    return loc.to_dict(orient="records")

def build_gene_lookup_map(gene_metadata_df: pd.DataFrame) -> dict[str, str]:
    """Builds a unified lookup map handling list-column aliases and primary symbol fallbacks."""
    # Unnest alias list column and create alias -> ensembl_gene_id map
    alias_df = gene_metadata_df.dropna(subset=["alias"]).explode("alias")

    alias_map = (
        alias_df.assign(alias=alias_df["alias"].astype(str).str.strip())
        .drop_duplicates(subset=["alias"])
        .set_index("alias")["ensembl_gene_id"]
        .to_dict()
    )

    # Create primary gene_symbol -> ensembl_gene_id map
    primary_map = (
        gene_metadata_df.dropna(subset=["gene_symbol"])
        .assign(
            gene_symbol=gene_metadata_df["gene_symbol"]
            .astype(str)
            .str.strip()
        )
        .drop_duplicates(subset=["gene_symbol"])
        .set_index("gene_symbol")["ensembl_gene_id"]
        .to_dict()
    )

    # Primary keys overwrite alias keys on collision
    return {**alias_map, **primary_map}

def map_and_log_gene_ids(
        data_file: pd.DataFrame, lookup_map: dict[str, str]
) -> pd.DataFrame:
    """Maps gene symbols using the fallback hierarchy and logs unmapped symbols."""
    # Clean input symbols
    data_file["gene_symbol"] = (
        data_file["gene_symbol"].astype(str).str.strip()
    )

    # Map symbol (checks primary first, then alias)
    data_file["ensembl_gene_id"] = data_file["gene_symbol"].map(lookup_map)

    # Identify and log unmapped symbols
    unmapped_mask = data_file["ensembl_gene_id"].isna()
    if unmapped_mask.any():
        unmapped_symbols = (
            data_file.loc[unmapped_mask, "gene_symbol"].unique().tolist()
        )
        logger.warning(
            f"Failed to match {unmapped_mask.sum()} rows ({len(unmapped_symbols)} unique symbols). "
            f"Unmapped gene_symbol values: {unmapped_symbols}"
        )

    # TODO we can't just drop real rows with no ensembl_gene_id
    # TODO once we have a dedicated gene_metadata file for this project, make this fail?
    # see log statements for symbols that don't resolve
    data_file = data_file[~unmapped_mask]

    return data_file
