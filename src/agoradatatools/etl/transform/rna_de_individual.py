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
- Passes sex values through as-is from the source data
- Applies tissue name transformations: "Right Cerebral Hemisphere" → "Hemibrain" and
  converts all tissues to sentence case
- Rounds numeric columns to 5 decimal places for consistency

Key Functions:
    transform_rna_de_individual: Main transformation function that orchestrates data processing
    _process_individual_data_file_core: Processes the core transformation logic for individual expression data
    _determine_result_order: Determines the ordering of display labels for genotypes in a model_group

Required Inputs:
    - rnaseq_genotype_label_map: Maps models and genotypes to display labels and model_groups
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
    nest_fields,
    ColumnRule,
    MatchesRegexRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.rna_de_individual_utils import (
    validate_model_group_consistency,
    create_gene_metadata_dict,
    preprocess_data_file,
    validate_data_file_not_empty,
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
    "rnaseq_genotype_label_map": {
        "model": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "display_label": [NotEmptyRule()],
        "model_group": [NotEmptyRule()],
        "result_order": [NotEmptyRule()],
    },
}

DATA_FILE_COLUMN_RULES: Dict[str, List[ColumnRule]] = {
    "model": [NotEmptyRule()],
    "age": [MatchesRegexRule(value=r"\d+ months$")],
}


def _determine_result_order(data_file: pd.DataFrame) -> List[str]:
    """
    Determines the result_order (ordering of display labels) for genotypes in a data file.

    Operates on a data_file that has already been merged with the genotype label map
    and filtered to a single model_group, so every display_label present is guaranteed
    to exist in the actual data. Empty display_label values are guaranteed not to be
    present — check_column_rules validates the label map before processing begins.

    Args:
        data_file: DataFrame already merged with the genotype label map and filtered to
            one model_group. Must have columns: display_label, result_order.

    Returns:
        List of display labels in the correct order based on result_order values.
    """
    unique_labels = data_file[["display_label", "result_order"]].drop_duplicates()
    return unique_labels.sort_values("result_order")["display_label"].tolist()


def _process_individual_data_file_core(
    data_file: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Core transformation logic for individual expression data.

    This function contains the individual-transform-specific processing logic:
    1. Enriches data with genotype metadata (display labels, result_order, model_group)
    2. Drops rows with no label-map match (NA result_order after the left merge)
    3. Derives the name field directly from model_group
    4. Renames columns and uses nest_fields to group individual records by (gene, tissue, name, age),
       producing one output row per combination with a nested "data" list;
       model_group is restored from name after nesting
    5. Adds metadata columns (gene_symbol, age_numeric, units, result_order, matched_control) vectorially

    Note: This function expects preprocessed data (mouse genes only, rounded numeric values,
    age strings already validated to match r'\\d+ months$') and is called once per
    model_group, so result_order and matched_control are constant across all rows and
    can be computed once as scalars.

    Args:
        data_file: Preprocessed DataFrame containing individual expression data with columns:
            ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        genotype_label_map_df: Genotype label map DataFrame with columns: model, genotype,
            display_label, model_group, result_order (result_order cast to int)

    Returns:
        List of output entry dictionaries, one per (gene, tissue, model_group, age)
    """
    # Step 1: Enrich with genotype metadata using vectorized merge
    # This adds display labels, result_order, and model_group to each row
    # validate="many_to_one" ensures data integrity (each (model, genotype) has one label)
    data_file = data_file.merge(
        genotype_label_map_df,
        on=["model", "genotype"],
        how="left",
        validate="many_to_one",
    )

    # Step 2: Drop rows that had no match in the label map.
    # After a left merge, any unmatched row has NA for result_order, so dropping
    # those NAs identifies rows absent from the label map.
    data_file = data_file.dropna(subset=["result_order"])

    if data_file.empty:
        raise ValueError(
            "No rows remained after filtering to mapped genotypes — "
            "all genotypes in this file were absent from the label map."
        )

    # Step 3: Pre-calculate result_order list and matched_control.
    # This function is called once per model_group, so these values are constant across
    # all rows.
    #
    # Because data_file has already been merged with the label map and filtered to a
    # single model_group, every label returned by _determine_result_order is guaranteed
    # to exist in the data. result_order_list[0] is therefore always the control
    # (lowest result_order) that is present in this file.
    #
    # Limitation for 4-genotype UCI studies: some DE analyses pair each case genotype
    # with a *different* control (e.g., Trem2-R47H_NSS.5xFAD vs Trem2-R47H_NSS, not
    # vs C57BL/6J). In those cases, a single matched_control value is a simplification
    # — it reflects the overall reference genotype for the group (lowest result_order)
    # rather than the per-case-genotype DE pairing.
    result_order_list = _determine_result_order(data_file)
    matched_control = result_order_list[0] if result_order_list else ""

    # Step 4: Rename columns for output format.
    # Drop the raw genotype column first — it was only needed for the merge to look up
    # display_label. Removing it before the rename prevents a duplicate "genotype" column.
    data_file = data_file.drop(columns=["genotype"])
    data_file = data_file.rename(
        columns={
            "display_label": "genotype",
            "individualid": "individual_id",
            "expression": "value",
        }
    )

    # Step 5: Nest individual records by (gene, tissue, model_group, age).
    # Each combination of these grouping keys produces one output row, with all
    # individual-level columns (genotype, sex, individual_id, value) nested into "data".
    #
    # name is derived from model before nesting because model is not a grouping key
    # and would not survive as a top-level column after nest_fields. For groups with a
    # single model (the common case), name == model. For multi-model groups (e.g. UCI
    # 4-genotype studies whose data spans two input files), name falls back to model_group
    # since there is no single model value that represents the whole group.
    unique_models = data_file["model"].unique()
    name_value = (
        unique_models[0]
        if len(unique_models) == 1
        else data_file["model_group"].iloc[0]
    )
    group_cols = ["ensembl_gene_id", "tissue", "model_group", "age"]
    cols_keep = group_cols + ["genotype", "sex", "individual_id", "value"]
    age_groups = nest_fields(
        data_file[cols_keep],
        grouping=group_cols,
        new_column="data",
        drop_columns=group_cols,
    )

    # Step 6: Add metadata columns vectorially.
    # age values are guaranteed to match r'\d+ months$' by check_column_rules upstream.
    age_groups["age_numeric"] = (
        age_groups["age"].str.extract(r"(\d+) months")[0].astype(int)
    )
    age_groups["gene_symbol"] = (
        age_groups["ensembl_gene_id"].map(gene_metadata_dict).fillna("")
    )
    age_groups["units"] = "Log2 Counts per Million"
    age_groups["name"] = name_value
    # All rows share the same result_order list. The multiplication creates n
    # references to the same list object, which is safe because the list is never
    # mutated after this point — to_dict(orient="records") only reads it.
    age_groups["result_order"] = [result_order_list] * len(age_groups)
    age_groups["matched_control"] = matched_control

    # Step 7: Select output columns, sort by gene then age, and return as records
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
        age_groups[output_cols]
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

    Processing Steps:
        1. Validates required datasets and columns (check_required_datasets_and_columns)
        2. Validates column values for static datasets (check_column_rules on COLUMN_RULES)
        3. Prepares metadata DataFrames (normalizes genotype label map; loads gene metadata)
        4. Validates data consistency (model_group values)
        5. Creates gene metadata lookup dictionary (Ensembl ID → gene symbol)
        6. Groups input files by model_group so that models whose data is split across
           multiple files (e.g. UCI models) are combined before output creation, while
           unrelated files are processed and freed independently; each file is
           preprocessed using data_file_required_columns for column validation and
           data_file_column_rules for column value rules (filters to mouse genes,
           rounds numeric values to 5 decimal places); raises ValueError if any file
           contains rows from more than one model (each input file must contain data
           for exactly one model)
        7. For each model_group:
           - Concatenates preprocessed DataFrames within the group (no-op for
             single-file groups)
           - Enriches with genotype metadata
           - Drops rows with no label-map match (NA result_order after the merge)
           - Groups by gene, tissue, and model_group
           - Creates output entries with individual data points
           - Frees memory before moving to the next group
        8. Consolidates output from all groups

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'rnaseq_genotype_label_map': Maps genotypes to display labels and model_groups.
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
        column_rules: Per-column content rules for static datasets (rnaseq_genotype_label_map).
            Defaults to COLUMN_RULES module constant.
        data_file_column_rules: Per-column content rules applied to each data file.
            Defaults to DATA_FILE_COLUMN_RULES module constant.

    Returns:
        List of dictionaries, each representing a unique combination of gene, tissue,
        model_group, and age. Each entry contains:
            - ensembl_gene_id: Mouse gene identifier (ENSMUSG*)
            - gene_symbol: Human-readable gene name (empty string if not found)
            - tissue: Tissue name (with JAX-specific mappings and sentence case applied)
            - name: model_group value
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
    # Step 1: Validate inputs
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    # Step 2: Prepare metadata DataFrames
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map"].copy()
    rnaseq_genotype_label_map_df["result_order"] = rnaseq_genotype_label_map_df[
        "result_order"
    ].astype(int)
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"]

    # Step 3: Validate data consistency
    validate_model_group_consistency(rnaseq_genotype_label_map_df)

    # Step 4: Create gene metadata lookup dictionary (Ensembl ID → gene symbol)
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    # Step 5: Group files by model_group so that models sharing the same group
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
    model_to_mg: Dict[str, str] = (
        rnaseq_genotype_label_map_df.drop_duplicates("model")
        .set_index("model")["model_group"]
        .to_dict()
    )

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

    # Step 6: Process one model_group at a time.
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
            combined_data, gene_metadata_dict, rnaseq_genotype_label_map_df
        )
        output.extend(group_output)

        del preprocessed_dfs, combined_data
        gc.collect()

    logger.info(f"Transform rna_de_individual total output entries: {len(output)}")

    return output
