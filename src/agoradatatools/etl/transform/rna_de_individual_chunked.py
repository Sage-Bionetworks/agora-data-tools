"""
RNA Individual Expression Transform Module (Chunked Version)

This module transforms individual RNA expression (normalized expression) data for Model AD.
Unlike the standard rna_de_individual transform that combines multiple datasets, this chunked
version processes ONE dataset at a time, outputting each dataset to its own separate file.

The transformation includes gene metadata, genotype labels, and individual expression
values to create a structured output format grouped by model_group.

The transformation:
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Groups individual expression data by gene, tissue, model_group, and name
- Creates age-based entries containing individual expression values for all genotypes
- Organizes data by model_group to support both single and multiple control display paradigms
- Enriches data with gene symbols from gene metadata
- Maps genotypes to display labels for better readability
- Applies special tissue name transformation for JAX models ("Right Cerebral Hemisphere" -> "Hemibrain")
- Rounds numeric columns to 5 decimal places for consistency

Key Functions:
    transform_rna_de_individual_chunked: Main transformation function that orchestrates the data processing
    _create_individual_results_from_group: Creates individual_results structure with age-based grouping
    _create_output_entry_from_group: Creates output entries from a grouped DataFrame, one entry per age
    _process_single_data_file: Processes a single individual expression data file
    _determine_result_order: Determines the ordering of display labels for genotypes in a model_group

Required Inputs:
    - rnaseq_genotype_label_map_new: Maps models and genotypes to display labels and model_groups
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - Data file: ONE CSV file containing individual expression results with columns:
      ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualid
"""

import pandas as pd
from typing import Dict, List, Any
import logging
import gc

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    extract_age_numeric,
)
from agoradatatools.etl.transform.rna_shared_utils import (
    filter_mouse_genes,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    create_model_group_dict,
    create_label_map_dict,
    log_file_processing_info,
    validate_data_file_not_empty,
    normalize_model_group_value,
    extract_common_metadata,
)

logger = logging.getLogger(__name__)


def _create_result_order_dict(
    rnaseq_genotype_label_map_df: pd.DataFrame,
) -> Dict[tuple[str, str], int]:
    """
    Creates a dictionary mapping (model, genotype) tuples to result_order values.

    Args:
        rnaseq_genotype_label_map_df: DataFrame containing model, genotype, and result_order columns

    Returns:
        Dictionary mapping (model, genotype) tuples to result_order integer values
    """
    result_order_dict = {}
    for _, row in rnaseq_genotype_label_map_df.iterrows():
        model = row["model"]
        genotype = row["genotype"]
        result_order = int(row["result_order"])
        result_order_dict[(model, genotype)] = result_order
    return result_order_dict


def _create_model_genotype_map(
    rnaseq_genotype_label_map_df: pd.DataFrame,
) -> Dict[tuple[str, str], str]:
    """
    Creates a dictionary mapping (effective_model_group, genotype) tuples to model names.

    This is needed because genotypes in a model_group may belong to different models.
    For example, in the Abca7*V1599M model_group, some genotypes belong to the
    Abca7*V1599M model and others to Abca7*V1599M.5xFAD.

    Args:
        rnaseq_genotype_label_map_df: DataFrame containing model, model_group, and genotype columns

    Returns:
        Dictionary mapping (effective_model_group, genotype) tuples to model names
    """
    model_genotype_map = {}
    for _, row in rnaseq_genotype_label_map_df.iterrows():
        model = row["model"]
        model_group = row["model_group"]
        genotype = row["genotype"]
        effective_model_group = model_group if model_group else model
        model_genotype_map[(effective_model_group, genotype)] = model
    return model_genotype_map


REQUIRED_INPUT = {
    "rnaseq_genotype_label_map_new": [
        "model",
        "model_group",
        "display_label",
        "genotype",
        "result_order",
    ],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
}


def _determine_result_order(
    label_map_dict: Dict[tuple[str, str], str],
    result_order_dict: Dict[tuple[str, str], int],
    model_genotype_map: Dict[tuple[str, str], str],
    model_group: str,
    genotypes_by_model_group: Dict[str, List[str]],
) -> List[str]:
    """
    Determines the result_order (ordering of display labels) for genotypes in a model_group.

    Uses the result_order values from the rnaseq_genotype_label_map_new CSV file to
    determine the ordering of display labels. Handles cases where genotypes in the same
    model_group belong to different models.

    Args:
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels
        result_order_dict: Dictionary mapping (model, genotype) tuples to result_order values
        model_genotype_map: Dictionary mapping (effective_model_group, genotype) to model names
        model_group: Model group name (used as effective_model_group if present, otherwise model is used)
        genotypes_by_model_group: Dictionary mapping model_groups to lists of genotypes

    Returns:
        List of display labels in the correct order based on result_order values
    """
    # Get all genotypes for this model_group
    genotypes = genotypes_by_model_group.get(model_group, [])

    if not genotypes:
        return []

    # Create a list of (genotype, display_label, order) tuples
    genotype_info = []
    for genotype in genotypes:
        # Look up the model for this genotype in the model_group
        model = model_genotype_map.get((model_group, genotype))
        if not model:
            continue

        display_label = label_map_dict.get((model, genotype), "")
        order = result_order_dict.get(
            (model, genotype), 999
        )  # Default to high value if not found
        if display_label:
            genotype_info.append((genotype, display_label, order))

    # Sort by the result_order value
    sorted_info = sorted(genotype_info, key=lambda x: x[2])

    # Extract just the display labels
    result_order = [display_label for _, display_label, _ in sorted_info]

    return result_order


def _create_individual_results_from_group(
    group: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Creates individual_results structure from a grouped DataFrame.

    Groups the data by age and creates entries with all individual data points
    for each age timepoint. Uses efficient pandas methods for bulk operations.

    Args:
        group: DataFrame group containing age, genotype, sex, individualid, and expression columns.

    Returns:
        List of dictionaries, one per age timepoint, each containing:
            - 'age': str, age timepoint (e.g., '3 months')
            - 'data': List of dicts with individual data points containing:
                - 'genotype': str, genotype identifier
                - 'sex': str, sex identifier
                - 'individual_id': str, individual identifier
                - 'value': float, expression value
    """
    individual_results = []

    # Group by age to create age-based entries
    age_groups = group.groupby("age")

    for age, age_group in age_groups:
        # Select and rename columns for output format
        age_group_subset = age_group[
            ["genotype_display", "sex", "individualid", "expression"]
        ].copy()
        age_group_subset.columns = ["genotype", "sex", "individual_id", "value"]

        # Convert types efficiently
        age_group_subset["individual_id"] = age_group_subset["individual_id"].astype(
            str
        )
        age_group_subset["value"] = age_group_subset["value"].astype(float)

        # Convert DataFrame to list of dictionaries for JSON serialization
        data_points = age_group_subset.to_dict("records")

        individual_results.append(
            {
                "age": str(age),
                "data": data_points,
            }
        )

    # Sort by numeric age value
    try:
        individual_results = sorted(
            individual_results, key=lambda x: int(x["age"].split()[0])
        )
    except (ValueError, IndexError):
        # If age format is unexpected, keep original order
        pass

    return individual_results


def _create_output_entry_from_group(
    group_key: tuple[str, str, str, str],
    group: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    result_order_dict: Dict[tuple[str, str], int],
    model_genotype_map: Dict[tuple[str, str], str],
    genotypes_by_model_group: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    """
    Creates output entries from a grouped DataFrame, one entry per age group.

    Args:
        group_key: Tuple containing (ensembl_gene_id, tissue, model_group, model)
        group: DataFrame group containing individual expression data
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels
        result_order_dict: Dictionary mapping (model, genotype) tuples to result_order values
        model_genotype_map: Dictionary mapping (effective_model_group, genotype) to model names
        genotypes_by_model_group: Dictionary mapping model_groups to lists of genotypes

    Returns:
        List of dictionaries, one per age group, each containing the complete output entry
        with age and data fields (unnested from individual_results)
    """
    ensembl_gene_id, tissue, model_group, model = group_key

    # Extract common metadata (gene_symbol, tissue mapping)
    common_metadata = extract_common_metadata(
        ensembl_gene_id, tissue, gene_metadata_dict
    )

    # Create individual_results structure
    individual_results = _create_individual_results_from_group(group)

    # Determine the effective model_group for lookups
    effective_model_group = model_group if model_group else model

    # Determine matched_control from mapping file by finding the control genotype (result_order == 1)
    # for this model_group. This ensures matched_control is populated even if control data
    # is not present in this specific chunked input file.
    matched_control = ""
    genotypes = genotypes_by_model_group.get(effective_model_group, [])
    for genotype in genotypes:
        # Get the model for this genotype in the model_group
        genotype_model = model_genotype_map.get((effective_model_group, genotype))
        if genotype_model:
            # Check if this genotype has result_order == 1 (is the control)
            order = result_order_dict.get((genotype_model, genotype))
            if order == 1:
                matched_control = label_map_dict.get((genotype_model, genotype), "")
                break

    # Determine name - use model_group if it's different from model, otherwise use model
    name = model_group if (model_group and model_group != model) else model

    # Determine result_order for this model_group
    result_order = _determine_result_order(
        label_map_dict,
        result_order_dict,
        model_genotype_map,
        effective_model_group,
        genotypes_by_model_group,
    )

    # Create one output entry per age group (unnesting individual_results)
    output_entries = []
    for age_result in individual_results:
        output_entries.append(
            {
                "ensembl_gene_id": common_metadata["ensembl_gene_id"],
                "gene_symbol": common_metadata["gene_symbol"],
                "tissue": common_metadata["tissue"],
                "name": name,
                "model_group": normalize_model_group_value(model_group),
                "matched_control": matched_control,
                "units": "Log2 Counts per Million",
                "age": age_result["age"],
                "age_numeric": extract_age_numeric(age_result["age"]),
                "result_order": result_order,
                "data": age_result["data"],
            }
        )

    return output_entries


def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    data_file_required_columns: List[str],
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    result_order_dict: Dict[tuple[str, str], int],
    model_genotype_map: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    genotypes_by_model_group: Dict[str, List[str]],
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single individual expression data file.

    Applies filtering, enrichment, and grouping logic to transform raw expression
    data into the structured output format. Uses vectorized pandas operations
    for efficient processing of large datasets.

    Args:
        file_name: Name of the data file being processed
        data_file: DataFrame containing the individual expression data
        data_file_required_columns: List of required column names
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels
        result_order_dict: Dictionary mapping (model, genotype) tuples to result_order values
        model_genotype_map: Dictionary mapping (effective_model_group, genotype) to model names
        model_group_dict: Dictionary mapping model names to model_groups
        genotypes_by_model_group: Dictionary mapping model_groups to lists of genotypes
        file_index: Current file index for progress tracking
        total_files: Total number of files to process

    Returns:
        List of output entry dictionaries
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)

    # Check if data file is empty
    validate_data_file_not_empty(file_name, data_file)

    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )

    # Filter out rows with human gene ensembl IDs (ENSG*), keep only mouse (ENSMUSG*)
    data_file = filter_mouse_genes(data_file)

    # Round numeric columns to 5 decimal places for consistency
    data_file = data_file.round(decimals=5)

    # Map genotypes to display labels using vectorized merge operation
    # This is more efficient than row-by-row operations for large datasets
    if label_map_dict:
        label_map_list = [
            {"model": k[0], "genotype": k[1], "genotype_display": v}
            for k, v in label_map_dict.items()
        ]
        label_map_df = pd.DataFrame(label_map_list)

        # Add result_order to label_map_df for control identification
        result_order_list = [
            {"model": k[0], "genotype": k[1], "result_order": v}
            for k, v in result_order_dict.items()
        ]
        result_order_df = pd.DataFrame(result_order_list)
        label_map_df = label_map_df.merge(
            result_order_df, on=["model", "genotype"], how="left", validate="one_to_one"
        )

        # Perform left join to add display labels and result_order
        data_file = data_file.merge(
            label_map_df, on=["model", "genotype"], how="left", validate="many_to_one"
        )
        # Fill missing display labels with original genotype
        data_file["genotype_display"] = data_file["genotype_display"].fillna(
            data_file["genotype"]
        )
        # Fill missing result_order with high value (non-controls)
        data_file["result_order"] = data_file["result_order"].fillna(999)
    else:
        # If no label map provided, use genotype as display label
        data_file["genotype_display"] = data_file["genotype"]
        # Still add result_order for control identification
        if result_order_dict:
            result_order_list = [
                {"model": k[0], "genotype": k[1], "result_order": v}
                for k, v in result_order_dict.items()
            ]
            result_order_df = pd.DataFrame(result_order_list)
            data_file = data_file.merge(
                result_order_df,
                on=["model", "genotype"],
                how="left",
                validate="many_to_one",
            )
            data_file["result_order"] = data_file["result_order"].fillna(999)

    # Map model names to model_groups
    data_file["model_group"] = data_file["model"].map(model_group_dict).fillna("")

    # Determine the "name" field based on grouping logic
    # Uses vectorized conditional logic instead of row-by-row application
    different_from_model = (data_file["model_group"] != "") & (
        data_file["model_group"] != data_file["model"]
    )
    data_file["name"] = data_file["model_group"].where(
        different_from_model, data_file["model"]
    )

    # Filter data to only include genotypes that belong to the model_group
    # Create effective model_group for filtering
    data_file["effective_model_group"] = data_file["model_group"].where(
        data_file["model_group"] != "", data_file["model"]
    )

    # Build a set of allowed (model_group, genotype) combinations for efficient lookup
    allowed_genotypes_set = set()
    for mg, genotypes in genotypes_by_model_group.items():
        for genotype in genotypes:
            allowed_genotypes_set.add((mg, genotype))

    # Create combined key and filter using set membership (O(1) lookup)
    data_file["_filter_key"] = list(
        zip(data_file["effective_model_group"], data_file["genotype"])
    )
    data_file = data_file[data_file["_filter_key"].isin(allowed_genotypes_set)]

    # Drop temporary columns used for filtering
    data_file = data_file.drop(columns=["effective_model_group", "_filter_key"])

    # Convert repetitive string columns to categorical dtype for memory efficiency
    # This reduces memory usage significantly for large datasets with repeated values
    for col in ["model", "genotype", "tissue", "sex", "model_group", "name"]:
        if col in data_file.columns:
            data_file[col] = data_file[col].astype("category")

    # Group by gene, tissue, model_group, and name to create one entry per group
    grouped = data_file.groupby(["ensembl_gene_id", "tissue", "model_group", "name"])

    output_entries = []
    for group_key, group in grouped:
        entries_for_group = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            label_map_dict,
            result_order_dict,
            model_genotype_map,
            genotypes_by_model_group,
        )
        output_entries.extend(entries_for_group)

    # Clean up memory
    del data_file
    gc.collect()

    return output_entries


def transform_rna_de_individual_chunked(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for RNA individual expression data (chunked version).

    This function transforms a SINGLE RNA individual expression data file into a structured
    format grouped by model_group to support display paradigms for models with
    single or multiple controls. Unlike the standard rna_de_individual transform,
    this processes one dataset at a time for separate output files.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'rnaseq_genotype_label_map_new': Maps genotypes to display labels and model_groups
            - 'mouse_gene_metadata': Gene symbols for Ensembl IDs
            - One data file: CSV DataFrame containing individual expression
              results with columns: ensembl_gene_id, expression, model, genotype, age,
              sex, tissue, individualid
        required_input: Dictionary mapping required dataset names to required columns

    Returns:
        List of dictionaries, each representing a unique combination of gene, tissue,
        model_group, name, and age. Each entry contains individual expression data points
        with fields: ensembl_gene_id, gene_symbol, tissue, name, model_group,
        matched_control, units, age, age_numeric, result_order, and data (list of
        individual measurements).
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Pre-compute lookup dictionaries
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map_new"].fillna("")
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"].fillna("")

    # Create lookup dictionaries for efficient data enrichment
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    label_map_dict = create_label_map_dict(rnaseq_genotype_label_map_df)

    # Create result_order dictionary from CSV
    result_order_dict = _create_result_order_dict(rnaseq_genotype_label_map_df)

    # Create model_genotype_map to handle genotypes from different models in same model_group
    model_genotype_map = _create_model_genotype_map(rnaseq_genotype_label_map_df)

    # Validate that each model has consistent model_group values
    validate_model_group_consistency(rnaseq_genotype_label_map_df)

    model_group_dict = create_model_group_dict(rnaseq_genotype_label_map_df)

    # Build genotypes_by_model_group dictionary for filtering
    # Groups genotypes by their effective model_group for validation
    label_map_copy = rnaseq_genotype_label_map_df.copy()
    label_map_copy["effective_model_group"] = label_map_copy["model_group"].where(
        label_map_copy["model_group"] != "", label_map_copy["model"]
    )

    genotypes_by_model_group = (
        label_map_copy.groupby("effective_model_group")["genotype"]
        .apply(lambda x: list(x.unique()))
        .to_dict()
    )

    # Get the single data file (excluding metadata files)
    file_list = [k for k in datasets.keys() if k not in required_input]

    if len(file_list) != 1:
        raise ValueError(
            f"Transform rna_de_individual_chunked expects exactly 1 data file, "
            f"but got {len(file_list)}: {file_list}"
        )

    file_name = file_list[0]
    data_file = datasets[file_name]

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

    logger.info(f"Transform rna_de_individual_chunked processing file: {file_name}")

    output = _process_single_data_file(
        file_name,
        data_file,
        data_file_required_columns,
        gene_metadata_dict,
        label_map_dict,
        result_order_dict,
        model_genotype_map,
        model_group_dict,
        genotypes_by_model_group,
        file_index=0,
        total_files=1,
    )

    logger.info(
        f"Transform rna_de_individual_chunked total output entries: {len(output)}"
    )
    return output
