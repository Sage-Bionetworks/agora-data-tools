"""
This module contains the transformation logic for the eqtl_results dataset.
This is for the xQTL explorer.
"""
from pathlib import Path
from typing import Any, Dict, List
import logging
import hashlib

import math
import pandas as pd

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
)

logger = logging.getLogger(__name__)

REQUIRED_INPUT = {
    "gene_metadata": [
        "ensembl_gene_id",
        "gene_symbol",
        "alias",
    ],

    "eqtl_cell_label_map": [
        "cell_class_short",
        "cell_class",
        "cell_subclass_short",
        "cell_subclass",
    ],

    # TODO gene location data
}

FILE_NAME_PREFIX = "eqtl_results_"

# TODO align ui_config column order
OUTPUT_COLUMN_ORDER = [
    "variant",
    "variant_location",
    "effect_direction",
    "gene_symbol",
    "ensembl_gene_id",
    # "gene_location",
    "beta",
    "p_val",
    "maf",
    "minor_allele",
    "reference_allele",
    "cell_class",
    "cell_class_short",
    "cell_subclass",
    "cell_subclass_short",
]

# convert z to p_val TODO apply human scientist eyes here
def _zscore_to_pval_string(z) -> str:
    x = abs(float(z)) / math.sqrt(2.0)      # two-sided p = erfc(x)
    if x < 25.0:                            # erfc representable → exact
        ln_p = math.log(math.erfc(x))
    else:                                   # erfc underflows past ~x=26 → asymptotic tail
        inv = 1.0 / (x * x)
        series = 1 - 0.5*inv + 0.75*inv**2 - 1.875*inv**3
        ln_p = -x*x - math.log(x) - 0.5*math.log(math.pi) + math.log(series)
    log10_p = ln_p / math.log(10.0)
    exp = math.floor(log10_p)
    mant = 10 ** (log10_p - exp)            # mantissa in [1, 10)
    if mant >= 10.0:                        # guard rounding to 10.0
        mant /= 10.0; exp += 1
    return f"{mant:.2f}E{exp:+03d}"

# resolve cell type labels
def _resolve_cell_labels(file_name, label_map_df) -> Dict[str, str]:
    key = Path(file_name[len(FILE_NAME_PREFIX) :]).stem

    cell_type_labels = label_map_df.loc[
        label_map_df["cell_subclass_short"].str.lower() == key.lower()
        ].iloc[0].to_dict()

    logger.info(f"Resolved cell type labels: {cell_type_labels} for file: {file_name} ")
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
    # Step 2 setup: Unnest alias list column and create alias -> ensembl_gene_id map
    alias_df = gene_metadata_df.dropna(subset=["alias"]).explode("alias")

    alias_map = (
        alias_df.assign(alias=alias_df["alias"].astype(str).str.strip())
        .drop_duplicates(subset=["alias"])
        .set_index("alias")["ensembl_gene_id"]
        .to_dict()
    )

    # Step 1 setup: Create primary gene_symbol -> ensembl_gene_id map
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
    # (Guarantees Primary match is checked FIRST, Alias SECOND)
    return {**alias_map, **primary_map}

def map_and_log_gene_ids(
        data_file: pd.DataFrame, lookup_map: dict[str, str]
) -> pd.DataFrame:
    """Maps gene symbols using the fallback hierarchy and logs unmapped symbols."""
    # Clean input symbols
    data_file["gene_symbol"] = (
        data_file["gene_symbol"].astype(str).str.strip()
    )

    # Steps 1 & 2: Map symbol (checks primary first, then alias)
    data_file["ensembl_gene_id"] = data_file["gene_symbol"].map(lookup_map)

    # Step 3: Identify and log unmapped symbols
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
    # see log statements for symbols that don't resolve
    #
    data_file = data_file[~unmapped_mask]

    return data_file

def _mock_maf(key: str) -> float:
    """Deterministic mock MAF in (0, 0.5) from a stable key → 0.0001..0.4999."""
    h = int(hashlib.sha256(str(key).encode("utf-8")).hexdigest(), 16)
    return (h % 4999 + 1) / 10000

# handle results for a cell_subclass / file
def _process_single_data_file(
        file_name: str,
        data_file: pd.DataFrame,
        data_file_required_columns: List[str],
        ensembl_gene_id_lookup_map: Dict,
        label_map_df: pd.DataFrame,
        file_index: int,
        total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single eQTL data file and transforms it into output entries.
    """
    logger.info(
        f"Processing {file_name} ({file_index+1}/{total_files}): {len(data_file)} rows, "
        f"{len(data_file.columns)} columns, "
        f"{data_file.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
    )

    # Check if data file is empty (before column validation)
    if len(data_file) == 0:
        raise ValueError(f"Data file {file_name} is empty")

    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )

    # resolve ensembl_gene_id from gene_symbol
    data_file = map_and_log_gene_ids(data_file, ensembl_gene_id_lookup_map)

    # use the filename to get the cell type labels for this file's cell_subclass
    cell_labels = _resolve_cell_labels(file_name, label_map_df)
    data_file = data_file.assign(**cell_labels)

    # Compute p-value
    data_file["p_val"] = data_file["z"].apply(
        lambda z: _zscore_to_pval_string(z) if pd.notna(z) else None
    )

    # TODO use real maf values when we have them
    data_file["maf"] = data_file["variant"].map(_mock_maf)

    # add effect_direction column
    data_file["effect_direction"] = data_file["beta"].apply(
        lambda x: "Up" if x > 0 else ("Down" if x < 0 else "-")
    )

    # nest location data
    data_file["variant_location"] = build_location(data_file, "variant_chr", "variant_start", "variant_end")

    data_file = data_file.drop(columns=["variant_chr", "variant_start", "variant_end"])

    data_file = data_file.reindex(columns=OUTPUT_COLUMN_ORDER)
    return data_file.to_dict(orient="records")


def transform_eqtl_results(
        datasets: Dict[str, pd.DataFrame],
        required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the eQTL source files into a structured format for xQTL.

    Source Files: TODO

    Expected Transformations: TODO

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        list[dict[str, Any]]: A list containing dictionaries with the transformed data.

    Raises:
        ValueError: If required datasets are missing or if required columns are missing from any dataset.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Load and prepare datasets & lookup map
    gene_metadata_df = datasets["gene_metadata"]
    eqtl_cell_label_map_df = datasets["eqtl_cell_label_map"]
    ensembl_gene_id_lookup_map = build_gene_lookup_map(gene_metadata_df)

    output = []

    # Process files one at a time
    file_list = [k for k in datasets.keys() if k not in required_input]
    total_files = len(file_list)

    data_file_required_columns = [
        "variant_chr",
        "variant_start",
        "variant_end",
        "variant",
        "reference_allele",
        "minor_allele",
        "gene_symbol",
        "beta",
        "se",
        "z",
        #"maf" # TODO maf data will be provided by GENESIS, probably here
    ]

    logger.info(f"Transform eqtl_results total data files: {total_files}")
    logger.info(f"Data files list: {file_list}")

    for i, file_name in enumerate(file_list):
        # Download and process one file at a time
        data_file = datasets[file_name]
        file_output = _process_single_data_file(
            file_name,
            data_file,
            data_file_required_columns,
            ensembl_gene_id_lookup_map,
            eqtl_cell_label_map_df,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform eqtl_results total output entries: {len(output)}")
    return output


