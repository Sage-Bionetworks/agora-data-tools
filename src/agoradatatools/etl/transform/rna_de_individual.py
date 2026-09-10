"""
RNA Individual Expression Transform Module

This module transforms individual RNA expression (normalized expression) data for Model AD.
It processes multiple RNA-seq datasets and combines them into a unified output format.

The transformation includes gene metadata, genotype labels, and individual expression
values to create a structured output format grouped by model_group.

The transformation:
- Validates column values up-front via check_column_rules (required fields non-empty,
  age strings matching the '[N] months' format) before any processing begins
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Groups files by model_group so that models sharing a model_group (e.g. UCI
  models whose data is split across two input files) are combined before output creation
- Validates that each input file contains data from exactly one model; raises ValueError
  immediately if a file contains rows from more than one model, since result_order and
  matched_control cannot be computed correctly when a file spans multiple models
- Creates one output entry per (gene, tissue, model_group, age) using vectorized
  grouping via nest_fields, nesting all individual records for that combination into a "data" list
- Organizes data by model_group to support both single and multiple control display paradigms
- Enriches data with gene symbols from gene metadata
- Maps genotypes to display labels for better readability
- Maps plural sex values to singular display labels
- Applies tissue name transformations: "Right Cerebral Hemisphere" → "Hemibrain"
- Rounds numeric columns to 5 decimal places for consistency

Key Functions:
    transform_rna_de_individual: Main transformation function that orchestrates data processing
    _process_individual_data_file_core: Processes the core transformation logic for individual expression data

Required Inputs:
    - genotype_label_map: Maps models and genotypes to display labels and model_groups
    - mouse_gene_metadata: Gene symbols for Ensembl IDs
    - Data files: One or more CSV files containing individual expression results; required
      columns are defined by the DATA_FILE_REQUIRED_COLUMNS module constant, and column
      value rules are defined by DATA_FILE_COLUMN_RULES
"""

import gc
import logging
from collections import defaultdict
from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    extract_age_numeric,
    ColumnRule,
    MatchesRegexRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.rna_de_individual_utils import (
    build_model_to_model_group,
    label_genotypes,
    nest_individual_records,
    prepare_genotype_label_map,
    create_gene_metadata_dict,
    preprocess_data_file,
    validate_data_file_not_empty,
    GENOTYPE_LABEL_MAP_COLUMNS,
    GENOTYPE_LABEL_MAP_RULES,
)

logger = logging.getLogger(__name__)

UNITS = "Log2 Counts per Million"

REQUIRED_INPUT = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_COLUMNS,
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol"],
}

DATA_FILE_REQUIRED_COLUMNS = [
    "ensembl_gene_id",
    "expression",
    "model",
    "genotype",
    "age",
    "sex",
    "tissue",
    "individualid",
]

COLUMN_RULES: Dict[str, Dict[str, List[ColumnRule]]] = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_RULES,
}

DATA_FILE_COLUMN_RULES: Dict[str, List[ColumnRule]] = {
    "model": [NotEmptyRule()],
    "age": [MatchesRegexRule(value=r"\d+ months$")],
}


def _process_individual_data_file_core(
    data_file: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
    context: str = "",
) -> List[Dict[str, Any]]:
    """
    Core transformation logic for individual expression data.

    Expects preprocessed data (mouse genes only, rounded numeric values, age strings already
    validated to match r'\\d+ months$') and is called once per model_group, so the fields
    nest_individual_records resolves as scalars are constant across all rows.

    Args:
        data_file: Preprocessed DataFrame containing individual expression data with columns:
            ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        genotype_label_map_df: Genotype label map DataFrame with columns: model, genotype,
            display_label, model_group, result_order (result_order cast to int)
        context: model_group being processed, named in the error raised when none of its
            genotypes match the label map.

    Returns:
        List of output entry dictionaries, one per (gene, tissue, model_group, age)
    """
    # Enrich with genotype metadata, dropping rows with no label-map row.
    data_file = label_genotypes(data_file, genotype_label_map_df, context)

    # name_from_model because this dataset displays the model itself for the single-model
    # groups that are the common case, falling back to model_group only for multi-model
    # groups (UCI 4-genotype studies, whose data spans two input files).
    entries = nest_individual_records(
        data_file.rename(columns={"expression": "value"}),
        group_columns=["ensembl_gene_id", "tissue", "model_group", "age"],
        units=UNITS,
        name_from_model=True,
    )

    # Derived after nesting, on one row per entry rather than one per animal. age values
    # are guaranteed to match r'\d+ months$' by check_column_rules upstream, so
    # extract_age_numeric cannot return None here.
    entries["age_numeric"] = entries["age"].map(extract_age_numeric).astype(int)
    entries["gene_symbol"] = (
        entries["ensembl_gene_id"].map(gene_metadata_dict).fillna("")
    )

    output_cols = [
        "ensembl_gene_id",
        "gene_symbol",
        "tissue",
        "name",
        "model_group",
        "matched_control",
        "units",
        "age",
        "age_numeric",
        "result_order",
        "data",
    ]
    return (
        entries[output_cols]
        .sort_values(by=["ensembl_gene_id", "age_numeric"])
        .to_dict(orient="records")
    )


def transform_rna_de_individual(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
    data_file_required_columns: List[str] = DATA_FILE_REQUIRED_COLUMNS,
    column_rules: Dict[str, Dict[str, List[ColumnRule]]] = COLUMN_RULES,
    data_file_column_rules: Dict[str, List[ColumnRule]] = DATA_FILE_COLUMN_RULES,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for RNA individual expression data.

    This function orchestrates the transformation of RNA individual expression data files
    into a structured format grouped by model_group. The output supports display paradigms
    for models with single or multiple controls.

    Input files are grouped by model_group so that models whose data is split across several
    files (e.g. UCI models) are combined before output creation, while unrelated files are
    processed and freed independently. Each input file must contain data for exactly one
    model, since result_order and matched_control cannot be computed for a file spanning
    several.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'genotype_label_map': Maps genotypes to display labels and model_groups.
              Required columns: model, genotype, display_label, model_group, result_order
            - 'mouse_gene_metadata': Gene symbols for Ensembl IDs.
              Required columns: ensembl_gene_id, gene_symbol
            - One or more data files: CSV DataFrames containing individual expression
              results. Required columns are defined by DATA_FILE_REQUIRED_COLUMNS:
              ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
        required_input: Dictionary mapping required dataset names to their required columns.
            Defaults to REQUIRED_INPUT module constant.
        data_file_required_columns: List of required column names for data files.
            Defaults to DATA_FILE_REQUIRED_COLUMNS module constant.
        column_rules: Per-column content rules for static datasets (genotype_label_map).
            Defaults to COLUMN_RULES module constant.
        data_file_column_rules: Per-column content rules applied to each data file.
            Defaults to DATA_FILE_COLUMN_RULES module constant.

    Returns:
        List of dictionaries, each representing a unique combination of gene, tissue,
        model_group, and age. Each entry contains:
            - ensembl_gene_id: Mouse gene identifier (ENSMUSG*)
            - gene_symbol: Human-readable gene name (empty string if not found)
            - tissue: Tissue name (with JAX-specific mappings applied)
            - name: the model itself for a single-model model_group, otherwise the
              model_group value
            - model_group: Explicit model group for display (None if not set)
            - matched_control: Display label of the control genotype
            - units: "Log2 Counts per Million"
            - age: Age timepoint string (e.g., "3 months")
            - age_numeric: Numeric age value for sorting
            - result_order: Ordered list of genotype display labels
            - data: List of individual data points, each containing:
                - genotype: Display label
                - sex: Sex identifier
                - individual_id: Sample identifier
                - value: Expression value

    Raises:
        ValueError: If required datasets or columns are missing, if data files are empty,
            if model_group values are inconsistent for any model, or if all rows in a
            data file are dropped because none of its genotypes matched the label map.
    """
    # Validate inputs
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    # Prepare metadata DataFrames
    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"]

    # Create gene metadata lookup dictionary (Ensembl ID → gene symbol)
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    # Group files by model_group so that models sharing the same group
    # (e.g. UCI models split across two input files) are processed together, while
    # unrelated files are processed and freed independently.
    #
    # This preserves the original memory-efficient sequential processing for the
    # majority of files (which each represent their own group) while only
    # holding multiple files in memory simultaneously when they genuinely need to
    # be combined.  The alternative of concatenating ALL files first would hold
    # the full ~5+ GB in memory at once regardless of grouping need.
    file_list = [k for k in datasets.keys() if k not in required_input]
    total_files = len(file_list)
    logger.info(
        f"Transform rna_de_individual: processing {total_files} data files: {file_list}"
    )

    # Build a model → model_group lookup from the label map df
    model_to_mg: Dict[str, str] = build_model_to_model_group(genotype_label_map_df)

    # Assign each file to the model_group of its data.
    # Reading the 'model' column from the already-loaded DataFrame is cheap.
    mg_to_files: Dict[str, List[str]] = defaultdict(list)
    for file_name in file_list:
        df = datasets[file_name]

        validate_data_file_not_empty(file_name, df)

        unique_models = df["model"].unique()
        if len(unique_models) > 1:
            raise ValueError(
                f"File '{file_name}' contains rows from multiple models "
                f"({list(unique_models)}). Each input file must contain data for "
                f"exactly one model. Split this file so that each output file "
                f"contains data for only one model."
            )

        # Each file contains exactly one model's data; use the first value.
        raw_model = df["model"].iloc[0]
        mg = model_to_mg.get(raw_model, raw_model)

        mg_to_files[mg].append(file_name)

    logger.info(
        "Transform rna_de_individual: file groups by model_group: "
        + ", ".join(f"{mg}={files}" for mg, files in mg_to_files.items())
    )

    # Process one model_group at a time.
    # Groups with a single file are processed without any extra concatenation.
    # Groups with multiple files (e.g. UCI split-file models) are concatenated
    # only within that group before processing, then freed immediately after.
    output = []
    global_file_idx = 0
    for group_idx, (mg, files_in_group) in enumerate(mg_to_files.items()):
        logger.info(
            f"Transform rna_de_individual: processing group {group_idx + 1}/"
            f"{len(mg_to_files)} ({mg}): {files_in_group}"
        )

        preprocessed_dfs = []
        for file_name in files_in_group:
            preprocessed_df = preprocess_data_file(
                file_name=file_name,
                data_file=datasets[file_name],
                file_index=global_file_idx,
                total_files=total_files,
                data_file_required_columns=data_file_required_columns,
                data_file_column_rules=data_file_column_rules,
            )
            preprocessed_dfs.append(preprocessed_df)
            global_file_idx += 1

        combined_data = (
            pd.concat(preprocessed_dfs, ignore_index=True)
            if len(preprocessed_dfs) > 1
            else preprocessed_dfs[0]
        )

        group_output = _process_individual_data_file_core(
            combined_data, gene_metadata_dict, genotype_label_map_df, context=mg
        )
        output.extend(group_output)

        del preprocessed_dfs, combined_data
        gc.collect()

    logger.info(f"Transform rna_de_individual total output entries: {len(output)}")

    return output
