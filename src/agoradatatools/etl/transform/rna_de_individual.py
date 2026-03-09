"""
RNA Individual Expression Transform Module

This module transforms individual RNA expression (normalized expression) data for Model AD.
It processes multiple RNA-seq datasets and combines them into a unified output format.

The transformation includes gene metadata, genotype labels, and individual expression
values to create a structured output format grouped by effective_model_group.

The transformation:
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Groups files by effective_model_group so that models sharing a model_group (e.g. UCI
  models whose data is split across two input files) are combined before output creation
- Groups individual expression data by gene, tissue, and effective_model_group
- Creates age-based entries containing individual expression values for all genotypes
- Organizes data by effective_model_group to support both single and multiple control display paradigms
- Enriches data with gene symbols from gene metadata
- Maps genotypes to display labels for better readability
- Passes sex values through as-is from the source data
- Applies tissue name transformations: "Right Cerebral Hemisphere" → "Hemibrain" and converts all tissues to sentence case
- Rounds numeric columns to 5 decimal places for consistency

Key Functions:
    transform_rna_de_individual: Main transformation function that orchestrates the data processing
    _create_output_entry_from_group: Creates output entries from a grouped DataFrame, one entry per age
    _process_individual_data_file_core: Processes the core transformation logic for individual expression data
    _determine_result_order: Determines the ordering of display labels for genotypes in a model_group

Required Inputs:
    - rnaseq_genotype_label_map: Maps models and genotypes to display labels and model_groups
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - Data files: One or more CSV files containing individual expression results with columns:
      ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
"""

import gc
from collections import defaultdict

import pandas as pd
from typing import Dict, List, Any
import logging

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    extract_age_numeric,
)
from agoradatatools.etl.transform.rna_de_individual_utils import (
    validate_model_group_consistency,
    create_gene_metadata_dict,
    create_genotype_metadata_dict,
    normalize_model_group_value,
    extract_common_metadata,
    preprocess_data_file,
)

logger = logging.getLogger(__name__)

REQUIRED_INPUT = {
    "rnaseq_genotype_label_map": [
        "model",
        "model_group",
        "display_label",
        "genotype",
        "result_order",
    ],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
}


def _determine_result_order(
    genotype_metadata_dict: Dict[tuple[str, str], Dict[str, Any]],
    model_group: str,
) -> List[str]:
    """
    Determines the result_order (ordering of display labels) for genotypes in a model_group.

    Uses the result_order values from the rnaseq_genotype_label_map CSV file to
    determine the ordering of display labels. Handles cases where genotypes in the same
    model_group belong to different models.

    Args:
        genotype_metadata_dict: Dictionary mapping (model, genotype) tuples to metadata dicts
            containing 'display_label', 'result_order', and 'effective_model_group'
        model_group: Model group name (used as effective_model_group if present, otherwise model is used)

    Returns:
        List of display labels in the correct order based on result_order values
    """
    # Collect all genotypes for this model_group by scanning genotype_metadata_dict
    genotype_info = []
    for (model, genotype), metadata in genotype_metadata_dict.items():
        display_label = metadata.get("display_label", "")
        # Only include genotypes that belong to this model_group and have a display label
        if metadata["effective_model_group"] == model_group and display_label:
            order = metadata.get("result_order", 999)
            genotype_info.append((genotype, display_label, order))

    # Sort by the result_order value
    sorted_info = sorted(genotype_info, key=lambda x: x[2])

    # Extract just the display labels
    result_order = [display_label for _, display_label, _ in sorted_info]

    return result_order


def _create_output_entry_from_group(
    group_key: tuple[str, str, str],
    group: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    genotype_metadata_dict: Dict[tuple[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Creates output entries from a grouped DataFrame, one entry per age timepoint.

    This function takes a group of individual expression data (gene, tissue, and
    effective_model_group combination) and creates separate output entries for each
    age timepoint. Each entry contains all individual data points for that age,
    potentially spanning multiple models that share the same model_group.

    Args:
        group_key: Tuple containing (ensembl_gene_id, tissue, effective_model_group).
            effective_model_group is model_group when set, otherwise the model name.
        group: DataFrame group containing individual expression data with columns:
            genotype, genotype_display, age, sex, individualid, expression, result_order,
            model_group, effective_model_group
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        genotype_metadata_dict: Dictionary mapping (model, genotype) tuples to metadata dicts
            containing 'display_label', 'result_order', and 'effective_model_group'

    Returns:
        List of dictionaries, one per age timepoint, each containing:
            - All metadata fields (gene, tissue, model, control info)
            - age: Age timepoint string
            - age_numeric: Numeric age for sorting
            - data: List of individual data points for this age
    """
    ensembl_gene_id, tissue, effective_model_group = group_key

    # Extract common metadata (gene_symbol, tissue mapping)
    common_metadata = extract_common_metadata(
        ensembl_gene_id, tissue, gene_metadata_dict
    )

    # Get the actual model_group value from data (empty string if none defined).
    # effective_model_group equals model_group when set, or model name when not.
    model_group = group.iloc[0]["model_group"]

    # name is the effective_model_group: model_group when explicitly set,
    # otherwise the model name. This consolidates multi-file model_groups under
    # a single display name while preserving solo-model names.
    name = effective_model_group

    # Identify the matched control genotype (lowest result_order value).
    # Use genotype_display directly since the group may span multiple models.
    # Assuming "result_order" and "genotype_display" are in group.columns
    matched_control = ""
    min_order = group["result_order"].min()
    control_rows = group[group["result_order"] == min_order]
    if not control_rows.empty:
        matched_control = control_rows.iloc[0]["genotype_display"]

    # Get ordered list of display labels for this model_group
    result_order = _determine_result_order(
        genotype_metadata_dict,
        effective_model_group,
    )

    # Fields shared across all age timepoints for this (gene, tissue, model_group)
    base_entry = {
        **common_metadata,
        "name": name,
        "model_group": normalize_model_group_value(model_group),
        "matched_control": matched_control,
        "units": "Log2 Counts per Million",
        "result_order": result_order,
    }

    # Create one output entry per age timepoint directly from grouped data
    output_entries = []

    for age, age_group in group.groupby("age"):
        # Select and rename columns for output format
        age_group_subset = age_group[
            ["genotype_display", "sex", "individualid", "expression"]
        ].rename(
            columns={
                "genotype_display": "genotype",
                "individualid": "individual_id",
                "expression": "value",
            }
        )

        # Convert DataFrame to list of dictionaries
        data_points = age_group_subset.to_dict("records")

        output_entries.append(
            {
                **base_entry,
                "age": str(age),
                "age_numeric": extract_age_numeric(str(age)),
                "data": data_points,
            }
        )

    # Sort entries by numeric age value
    output_entries.sort(key=lambda x: x["age_numeric"])

    return output_entries


def _process_individual_data_file_core(
    data_file: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    genotype_metadata_dict: Dict[tuple[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Core transformation logic for individual expression data.

    This function contains the individual-transform-specific processing logic:
    1. Enriches data with genotype metadata (display labels, result_order, model_group, effective_model_group)
    2. Filters to include only valid genotype combinations for each model_group
    3. Groups data by gene, tissue, and effective_model_group
    4. Creates output entries for each group with individual measurements

    Note: This function expects preprocessed data (mouse genes only, rounded numeric values).
    Preprocessing (filtering human genes, rounding, validation) is handled by the
    preprocess_data_file function before this function is called.

    Args:
        data_file: Preprocessed DataFrame containing individual expression data with columns:
            ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        genotype_metadata_dict: Dictionary mapping (model, genotype) tuples to metadata dicts
            containing 'display_label', 'result_order', 'model_group', 'effective_model_group'

    Returns:
        List of output entry dictionaries, one per (gene, tissue, effective_model_group, age)
    """
    # Step 1: Enrich with genotype metadata using vectorized merge
    # This adds display labels, result_order, model_group, and effective_model_group to each row
    if not genotype_metadata_dict:
        raise ValueError("genotype_metadata_dict is required")

    # Convert metadata dictionary to DataFrame for efficient pandas merge
    metadata_list = [
        {
            "model": k[0],
            "genotype": k[1],
            "genotype_display": v["display_label"],
            "result_order": v["result_order"],
            "model_group": v["model_group"],
            "effective_model_group": v["effective_model_group"],
        }
        for k, v in genotype_metadata_dict.items()
    ]
    metadata_df = pd.DataFrame(metadata_list)

    # Merge to add all metadata fields
    # validate="many_to_one" ensures data integrity (each (model, genotype) has one label)
    data_file = data_file.merge(
        metadata_df, on=["model", "genotype"], how="left", validate="many_to_one"
    )

    # Handle unmapped genotypes gracefully
    data_file["genotype_display"] = data_file["genotype_display"].fillna(
        data_file["genotype"]
    )
    data_file["result_order"] = data_file["result_order"].fillna(999)
    data_file["model_group"] = data_file["model_group"].fillna("")
    data_file["effective_model_group"] = data_file["effective_model_group"].fillna(
        data_file["model"]
    )

    # Step 3: Filter to valid genotype combinations for each model_group
    # This prevents processing invalid genotype combinations that may exist in the data
    # Build set of valid (effective_model_group, genotype) pairs from metadata
    allowed_genotypes_set = {
        (metadata["effective_model_group"], genotype)
        for (model, genotype), metadata in genotype_metadata_dict.items()
    }

    # Filter: keep only rows with valid genotype combinations using the pre-computed column
    filter_mask = [
        (emg, gt) in allowed_genotypes_set
        for emg, gt in zip(data_file["effective_model_group"], data_file["genotype"])
    ]
    data_file = data_file[filter_mask]

    # Step 4: Convert types once per file before grouping
    data_file["individualid"] = data_file["individualid"].astype(str)
    data_file["expression"] = data_file["expression"].astype(float)

    # Step 5: Group and create output entries
    # Group by gene, tissue, and effective_model_group so that all models sharing
    # the same model_group (e.g. data split across multiple input files) are
    # combined into a single output entry.
    grouped = data_file.groupby(["ensembl_gene_id", "tissue", "effective_model_group"])

    output_entries = []
    for group_key, group in grouped:
        entries_for_group = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            genotype_metadata_dict,
        )
        output_entries.extend(entries_for_group)

    return output_entries


def transform_rna_de_individual(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for RNA individual expression data.

    This function orchestrates the transformation of RNA individual expression data files
    into a structured format grouped by model_group. The output supports display paradigms
    for models with single or multiple controls.

    Processing Steps:
        1. Validates required datasets and columns
        2. Creates gene and genotype metadata lookup dictionaries
        3. Validates data consistency (model_group values)
        4. Groups input files by effective_model_group so that models whose data is
           split across multiple files (e.g. UCI models) are combined before output
           creation, while unrelated files are processed and freed independently
        5. For each effective_model_group:
           - Preprocesses each file (filters to mouse genes, rounds numeric values to
             5 decimal places)
           - Concatenates preprocessed DataFrames within the group (no-op for
             single-file groups)
           - Enriches with genotype metadata
           - Filters to valid genotype combinations
           - Groups by gene, tissue, and effective_model_group
           - Creates output entries with individual data points
           - Frees memory before moving to the next group
        6. Consolidates output from all groups

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'rnaseq_genotype_label_map': Maps genotypes to display labels and model_groups.
              Required columns: model, genotype, display_label, model_group, result_order
            - 'mouse_gene_metadata': Gene symbols for Ensembl IDs.
              Required columns: ensembl_gene_id, gene_symbol, alias
            - One or more data files: CSV DataFrames containing individual expression
              results with columns: ensembl_gene_id, expression, model, genotype, age,
              sex, tissue, individualid
        required_input: Dictionary mapping required dataset names to their required columns.
            Defaults to REQUIRED_INPUT module constant.

    Returns:
        List of dictionaries, each representing a unique combination of gene, tissue,
        effective_model_group, and age. Each entry contains:
            - ensembl_gene_id: Mouse gene identifier (ENSMUSG*)
            - gene_symbol: Human-readable gene name (empty string if not found)
            - tissue: Tissue name (with JAX-specific mappings and sentence case applied)
            - name: effective_model_group value (equals model_group when explicitly set,
              otherwise equals the model name for solo models)
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
            or if model_group values are inconsistent for any model.
    """
    # Step 1: Validate inputs
    check_required_datasets_and_columns(datasets, required_input)

    # Step 2: Prepare metadata DataFrames (fill NA values with empty strings)
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map"].fillna("")
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"].fillna("")

    # Step 3: Validate data consistency
    validate_model_group_consistency(rnaseq_genotype_label_map_df)

    # Step 4: Create lookup dictionaries for efficient processing
    # Gene metadata: maps Ensembl IDs to gene symbols
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    # Genotype metadata: single source of truth for all genotype-related information
    # Includes display_label, result_order, model_group, and effective_model_group
    genotype_metadata_dict = create_genotype_metadata_dict(
        rnaseq_genotype_label_map_df, include_result_order=True
    )

    # Step 5: Define required columns for data files
    data_file_required_columns = [
        "ensembl_gene_id",
        "expression",
        "model",
        "genotype",
        "age",
        "sex",
        "tissue",
        "individualid",
    ]

    # Step 6: Group files by effective_model_group so that models sharing the same
    # group (e.g. UCI models split across two input files) are processed together,
    # while unrelated files are processed and freed independently.
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

    # Build a model → effective_model_group lookup from the metadata dict
    model_to_emg: Dict[str, str] = {
        model: metadata["effective_model_group"]
        for (model, genotype), metadata in genotype_metadata_dict.items()
    }

    # Assign each file to the effective_model_group of its data.
    # Reading the 'model' column from the already-loaded DataFrame is cheap.
    emg_to_files: Dict[str, List[str]] = defaultdict(list)
    for file_name in file_list:
        df = datasets[file_name]
        # A single file should contain only one model's data; use the first value.
        raw_model = df["model"].iloc[0] if len(df) > 0 else ""
        emg = model_to_emg.get(raw_model, raw_model)
        emg_to_files[emg].append(file_name)

    logger.info(
        "Transform rna_de_individual: file groups by effective_model_group: "
        + ", ".join(f"{emg}={files}" for emg, files in emg_to_files.items())
    )

    # Step 7: Process one effective_model_group at a time.
    # Groups with a single file are processed without any extra concatenation.
    # Groups with multiple files (e.g. UCI split-file models) are concatenated
    # only within that group before processing, then freed immediately after.
    output = []
    global_file_idx = 0
    for group_idx, (emg, files_in_group) in enumerate(emg_to_files.items()):
        logger.info(
            f"Transform rna_de_individual: processing group {group_idx + 1}/"
            f"{len(emg_to_files)} ({emg}): {files_in_group}"
        )

        preprocessed_dfs = []
        for file_name in files_in_group:
            preprocessed_df = preprocess_data_file(
                file_name=file_name,
                data_file=datasets[file_name],
                file_index=global_file_idx,
                total_files=total_files,
                data_file_required_columns=data_file_required_columns,
            )
            preprocessed_dfs.append(preprocessed_df)
            global_file_idx += 1

        combined_data = (
            pd.concat(preprocessed_dfs, ignore_index=True)
            if len(preprocessed_dfs) > 1
            else preprocessed_dfs[0]
        )

        group_output = _process_individual_data_file_core(
            combined_data, gene_metadata_dict, genotype_metadata_dict
        )
        output.extend(group_output)

        del preprocessed_dfs, combined_data
        gc.collect()

    logger.info(f"Transform rna_de_individual total output entries: {len(output)}")

    return output
