"""
This module contains the transformation logic for the eqtl_results dataset.
This is for the xQTL explorer.
"""
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import logging
import hashlib

import math
import pandas as pd

from agoradatatools.etl.transform.transform_utils.xqtl_transform_utils import (
    resolve_cell_labels,
    build_location,
    build_gene_lookup_map,
    map_and_log_gene_ids,
)
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
        "cell_type_id",
        "cell_class_short",
        "cell_class",
        "cell_subclass_short",
        "cell_subclass",
    ],

    # TODO eventually we should drive this via the independent variant location source file
    # "variant_location_info": [
    #     "variant",
    #     "variant_chr",
    #     "variant_start",
    #     "variant_end"
    # ]

   "chromosome_features": [
       "type",
       "gene_symbol", # only on gene documents, might have to remove this and check after filtering?
       "location"
   ]
}

FILE_NAME_PREFIX = "eqtl_results_"

# TODO verify alignment with ui_config
OUTPUT_COLUMN_ORDER = [
    "variant",
    "variant_location",
    "effect_direction",
    "gene_symbol",
    "ensembl_gene_id",
    "gene_location",
    "beta",
    "adj_p_val",
    "adj_p_val_numeric",
    "maf",
    "minor_allele",
    "reference_allele",
    "cell_type_id",
    "cell_class",
    "cell_class_short",
    "cell_subclass",
    "cell_subclass_short",
]

# convert z to p_val, return as both string display value, and as log10 numeric value for sorting
# TODO we can't do this for real, we need to precalculate adjusted p-value prior to picking "top" results
def _zscore_to_pval_info(z) -> Tuple[Optional[str], Optional[float]]:
    if pd.isna(z):
        return None, None

    x = abs(float(z)) / math.sqrt(2.0)      # two-sided p = erfc(x)
    if x < 25.0:                            # erfc representable → exact
        ln_p = math.log(math.erfc(x))
    else:                                   # erfc underflows past ~x=26 → asymptotic tail
        inv = 1.0 / (x * x)
        series = 1 - 0.5 * inv + 0.75 * inv**2 - 1.875 * inv**3
        ln_p = -x * x - math.log(x) - 0.5 * math.log(math.pi) + math.log(series)

    log10_p = ln_p / math.log(10.0)
    exp = math.floor(log10_p)
    mant = 10 ** (log10_p - exp)            # mantissa in [1, 10)
    if mant >= 10.0:                        # guard rounding to 10.0
        mant /= 10.0
        exp += 1

    p_display = f"{mant:.2f}E{exp:+03d}"

    return p_display, round(log10_p, 6)


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
        chromosome_features_df: pd.DataFrame,
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

    # use the filename to get the cell type labels & id for this file's cell_subclass
    key = Path(file_name[len(FILE_NAME_PREFIX) :]).stem

    cell_labels = resolve_cell_labels(key, label_map_df)
    data_file = data_file.assign(**cell_labels)

    # TODO adapt this to leverage real adj_p_vals
    data_file[["adj_p_val", "adj_p_val_numeric"]] = data_file["z"].apply(_zscore_to_pval_info).tolist()

    # TODO integrate real maf values when we have them
    data_file["maf"] = data_file["variant"].map(_mock_maf)

    # add effect_direction column
    data_file["effect_direction"] = data_file["beta"].apply(
        lambda x: "Up" if x > 0 else ("Down" if x < 0 else "-")
    )

    # nest variant location data
    data_file["variant_location"] = build_location(data_file, "variant_chr", "variant_start", "variant_end")

    # TODO use real gene location data, not a copy of variant loc
    data_file["gene_location"] = data_file["variant_location"]

    # add gene location data
    # data_file = data_file.merge(
    #     chromosome_features_df[['gene_symbol', 'location']]
    #     .drop_duplicates(subset=['gene_symbol'])
    #     .rename(columns={'location': 'gene_location'}),
    #     on='gene_symbol',
    #     how='left'
    # )

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
    chromosome_features_df = datasets["chromosome_features"]
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
        "se", # TODO not actually required for this dataset?
        "z", # TODO expect this to be replaced by adj_p_val in the source file?
        #"maf" # TODO maf data will be provided by GENESIS, probably here? maybe in a separate variant:maf file?
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
            chromosome_features_df,
            eqtl_cell_label_map_df,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform eqtl_results total output entries: {len(output)}")
    return output


