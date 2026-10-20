"""
This module contains the transformation logic for the eqtl_phenotype dataset.
This is for the xQTL explorer.

Generate a record for each unique combination of: variant, gene, cell_subclass

eqtl_phenotype files: based on the single sample file provided, expect 1 file per variant:gene for all cell types
gene_metadats: to resolve ensembl_gene_id [why?]
eqtl_cell_label_map: to resolve cell type labels
TBD: to resolve variant and gene location data

"""
import zlib
from typing import Any, Dict, List
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    nest_fields,
    calculate_distribution,
)

logger = logging.getLogger(__name__)

REQUIRED_INPUT = {
    #  "gene_metadata": [
    #      "ensembl_gene_id",
    #      "gene_symbol",
    #      "alias",
    #  ],
    #
    "eqtl_cell_label_map": [
        "cell_type_id",
        "cell_class_short",
        "cell_class",
        "cell_subclass_short",
        "cell_subclass",
    ],
    #
    # "variant_location_info": [
    #     "variant",
    #     "variant_chr",
    #     "variant_start",
    #     "variant_end"
    # ]
    # TODO gene location data
}


# TODO validate assumption that devs can pull this info from the highlighted row
# the one sample file we have is named exp_rs2120461_rere_astro
# prefix_variant_gene_{cell_type}
FILE_NAME_PREFIX = "eqtl_pheno"

OUTPUT_COLUMN_ORDER = [
    "variant",
    "ensembl_gene_id",
    "cell_type_id",
    "expression_summaries",
    "effect_size_cell_class_data",
    "effect_size_cell_subclass_data",
]

# TODO we can't do this in prod, we need to figure out how to "roll up" subclass-level betas for gene/var/cell to class-level
# TODO or change strategy for the sidebar to show all subclasses of the relevant cell class?
# TODO or delete the mini versoin from the sidebar and only do the full 26 color drilldown?
# TODO or ask GENESIS/Panos for input
def _generate_effect_size_data(
    gene_symbol: str,
    variant: str,
    level: str,
    label: str,
    label_short: str,
    cell_type_id: str,
) -> dict:
    """Deterministically generates value, ci_left, and ci_right bounds
    seeded by gene, variant, and cell class or subclass.

    Args:
        gene_symbol (str): Gene symbol for this result.
        variant (str): Variant for this result.
        level (str): "class" or "subclass"; sets the label key names in the output,
            e.g. cell_class / cell_class_short or cell_subclass / cell_subclass_short.
        label (str): Cell class or subclass name.
        label_short (str): Cell class or subclass short name.
        cell_type_id (str): Cell type ID for this class or subclass.

    Returns:
        dict: cell_type_id, cell_{level}_short, cell_{level}, value, ci_left, and ci_right.

    Raises:
        ValueError: If level is not "class" or "subclass".
    """
    if level not in ("class", "subclass"):
        raise ValueError(f"level must be 'class' or 'subclass', got {level!r}")

    seed_str = f"{gene_symbol}:{variant}:{label}:{label_short}"
    seed = zlib.crc32(seed_str.encode("utf-8"))
    rng = np.random.default_rng(seed)

    MIN_BOUND = -0.8554149
    MAX_BOUND = 0.8736957

    # 1. Generate beta value within [-0.6, 0.6]
    value = rng.uniform(-0.6, 0.6)

    # 2. Calculate max allowable margin to keep within limits
    max_allowed_margin = min(value - MIN_BOUND, MAX_BOUND - value)
    margin = rng.uniform(0.05, max(0.05, max_allowed_margin))

    # 3. Calculate symmetrical CI bounds
    ci_left = value - margin
    ci_right = value + margin

    return {
        "cell_type_id": cell_type_id,
        f"cell_{level}_short": label_short,
        f"cell_{level}": label,
        "value": round(value, 6),
        "ci_left": round(ci_left, 6),
        "ci_right": round(ci_right, 6),
    }


# handle results for a given gene & variant (one pair per file)
def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    data_file_required_columns: List[str],
    label_map_df: pd.DataFrame,
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single eQTL phenotype data file and transforms it into output entries.
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

    # calculate box plot stats for each cell type / genotype combination
    data_file = calculate_distribution(
        df=data_file, grouping=["cell", "genotype"], distribution_column="phenotype"
    )

    # nest boxplot stats into expression_summaries
    data_file = nest_fields(
        df=data_file,
        grouping=["cell"],
        new_column="expression_summaries",
        drop_columns=["cell"],
    )

    # extract the gene and variant from the filename & assign to new columns
    stem = Path(file_name).stem
    parts = stem.split("_")
    gene_symbol = parts[-2]
    variant = parts[-1]

    data_file["gene_symbol"] = gene_symbol
    data_file["variant"] = variant

    # TODO total hack for ENSG - Suppports RERE only
    data_file["ensembl_gene_id"] = "ENSG00000142599"

    # Join to cell_label_map to get cell_type_id values TODO how does this work? class : subclass join?
    cols_to_keep = ["cell_subclass_short", "cell_type_id"]
    data_file = data_file.merge(
        label_map_df[cols_to_keep],
        left_on="cell",  # TODO assumption is that these will be cell_subclasss_short values
        right_on="cell_subclass_short",
        how="left",
    ).drop(columns=["cell_subclass_short"])

    # cell_class level effect size data
    # Extract unique cell classes from label_map_df. The class-level cell_type_id is synthesized
    # from cell_class_short (e.g. "class_astro"), not borrowed from an arbitrary subclass's
    # cell_type_id.
    unique_classes = (
        label_map_df[["cell_class", "cell_class_short"]]
        .drop_duplicates(subset=["cell_class", "cell_class_short"])
        .reset_index(drop=True)
    )

    # Generate deterministic stats for each unique class for this gene/variant
    class_stats_list = [
        _generate_effect_size_data(
            gene_symbol=gene_symbol,
            variant=variant,
            level="class",
            label=row["cell_class"],
            label_short=row["cell_class_short"],
            cell_type_id="class_" + row["cell_class_short"].lower(),
        )
        for _, row in unique_classes.iterrows()
    ]

    # Assign list of class stats dictionaries to every output record in data_file
    data_file["effect_size_cell_class_data"] = [class_stats_list] * len(data_file)

    # cell_subclass level effect size data
    # Extract unique cell subclasses from label_map_df
    unique_subclasses = (
        label_map_df[["cell_subclass", "cell_subclass_short", "cell_type_id"]]
        .drop_duplicates(subset=["cell_subclass", "cell_subclass_short"], keep="first")
        .reset_index(drop=True)
    )

    # Generate deterministic stats for each unique subclass for this gene/variant
    subclass_stats_list = [
        _generate_effect_size_data(
            gene_symbol=gene_symbol,
            variant=variant,
            level="subclass",
            label=row["cell_subclass"],
            label_short=row["cell_subclass_short"],
            cell_type_id=row["cell_type_id"],
        )
        for _, row in unique_subclasses.iterrows()
    ]

    # Assign list of subclass stats dictionaries to every output record in data_file
    data_file["effect_size_cell_subclass_data"] = [subclass_stats_list] * len(data_file)

    data_file = data_file.reindex(columns=OUTPUT_COLUMN_ORDER)
    return data_file.to_dict(orient="records")


def transform_eqtl_phenotype(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the eQTL expression source files into a structured format for xQTL.

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
    logger.info(datasets.keys)
    check_required_datasets_and_columns(datasets, required_input)
    eqtl_cell_label_map_df = datasets["eqtl_cell_label_map"]

    output = []

    # Process files one at a time
    file_list = [k for k in datasets.keys() if k not in required_input]
    total_files = len(file_list)

    logger.info(f"Transform eqtl_phenotype total data files: {total_files}")
    logger.info(f"Data files list: {file_list}")

    data_file_required_columns = [
        "gene",
        "variant",
        "cell",
        "genotype",
        "phenotype",
    ]

    for i, file_name in enumerate(file_list):
        # Download and process one file at a time
        data_file = datasets[file_name]
        file_output = _process_single_data_file(
            file_name,
            data_file,
            data_file_required_columns,
            eqtl_cell_label_map_df,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform eqtl_results total output entries: {len(output)}")
    return output
