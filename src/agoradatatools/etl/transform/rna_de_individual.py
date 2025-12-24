"""
RNA Individual Expression Transform Module

This module transforms individual RNA expression (normalized expression) data for Model AD.
It combines multiple datasets including gene metadata, genotype labels, and individual expression
values to create a structured output format grouped by model_group.

The transformation:
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Groups individual expression data by gene, tissue, and model_group
- Creates age-based entries containing individual expression values for all genotypes
- Organizes data by model_group to support both single and multiple control display paradigms
- Enriches data with gene symbols from gene metadata
- Maps genotypes to display labels for better readability
- Applies special tissue name transformation for JAX models ("Right Cerebral Hemisphere" -> "Hemibrain")
- Rounds numeric columns to 5 decimal places for consistency
- Processes multiple data files sequentially to minimize memory usage

Key Functions:
    transform_rna_de_individual: Main transformation function that orchestrates the data processing
    _create_individual_results_from_group: Creates individual_results structure with age-based grouping
    _create_output_entry_from_group: Creates a complete output entry from a grouped DataFrame
    _process_single_data_file: Processes a single individual expression data file

Required Inputs:
    - rnaseq_genotype_label_map: Maps models and genotypes to display labels and model_groups
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - Data files: One or more CSV files containing individual expression results with columns:
      ensembl_gene_id, expression, model, genotype, age, sex, tissue, individualID
"""

import pandas as pd
from typing import Dict, List, Any
import logging
import gc

from agoradatatools.etl.utils import check_required_datasets_and_columns
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

REQUIRED_INPUT = {
    "rnaseq_genotype_label_map": ["model", "model_group", "display_label", "genotype"],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
}


def _create_individual_results_from_group(
    group: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Creates individual_results structure from a grouped DataFrame.

    Groups the data by age and creates entries with all individual data points
    for each age timepoint.

    Args:
        group: DataFrame group containing age, genotype, sex, individualID, and expression columns.

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
        data_points = []
        for row in age_group.itertuples(index=False):
            data_points.append(
                {
                    "genotype": row.genotype,
                    "sex": row.sex,
                    "individual_id": str(row.individualID),
                    "value": float(row.expression),
                }
            )

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
    matched_control_dict: Dict[str, str],
) -> Dict[str, Any]:
    """
    Creates a complete output entry from a grouped DataFrame.

    Args:
        group_key: Tuple containing (ensembl_gene_id, tissue, model_group, name)
        group: DataFrame group containing individual expression data
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        matched_control_dict: Dictionary mapping models to matched controls

    Returns:
        Dictionary containing the complete output entry with individual_results
    """
    ensembl_gene_id, tissue, model_group, name = group_key

    # Extract common metadata (gene_symbol, tissue mapping)
    common_metadata = extract_common_metadata(
        ensembl_gene_id, tissue, gene_metadata_dict
    )

    matched_control = matched_control_dict.get(name, "")

    # Create individual_results structure
    individual_results = _create_individual_results_from_group(group)

    return {
        "ensembl_gene_id": common_metadata["ensembl_gene_id"],
        "hgnc_symbol": common_metadata["gene_symbol"],
        "tissue": common_metadata["tissue"],
        "name": name,
        "model_group": normalize_model_group_value(model_group),
        "matched_control": matched_control,
        "units": "Log2 Counts per Million",
        "individual_results": individual_results,
    }


def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    data_file_required_columns: List[str],
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    matched_control_dict: Dict[str, str],
    genotypes_by_model_group: Dict[str, List[str]],
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single individual expression data file.

    Args:
        file_name: Name of the data file being processed
        data_file: DataFrame containing the individual expression data
        data_file_required_columns: List of required column names
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels
        model_group_dict: Dictionary mapping model names to model_groups
        matched_control_dict: Dictionary mapping models to matched controls
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

    # Add model_group and display_label to data_file based on genotype label map
    # Map genotype to display_label
    data_file["genotype_display"] = data_file.apply(
        lambda row: label_map_dict.get(
            (row["model"], row["genotype"]), row["genotype"]
        ),
        axis=1,
    )

    # Map model to model_group
    data_file["model_group"] = data_file["model"].map(model_group_dict).fillna("")

    # Determine the "name" field based on grouping logic
    # If model_group exists and is different from model, use model_group
    # Otherwise, use the model name
    data_file["name"] = data_file.apply(
        lambda row: row["model_group"]
        if row["model_group"] and row["model_group"] != row["model"]
        else row["model"],
        axis=1,
    )

    # Filter data to only include genotypes that belong to the model_group
    def filter_by_model_group(row):
        model_group = row["model_group"] if row["model_group"] else row["model"]
        allowed_genotypes = genotypes_by_model_group.get(model_group, [])
        return row["genotype"] in allowed_genotypes

    data_file = data_file[data_file.apply(filter_by_model_group, axis=1)]

    # Group by gene, tissue, model_group, and name to create one entry per group
    grouped = data_file.groupby(["ensembl_gene_id", "tissue", "model_group", "name"])

    output_entries = []
    for group_key, group in grouped:
        output_entry = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            matched_control_dict,
        )
        output_entries.append(output_entry)

    # Clean up memory
    del data_file
    gc.collect()

    return output_entries


def transform_rna_de_individual(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for RNA individual expression data.

    This function transforms individual RNA expression data files into a structured
    format grouped by model_group to support display paradigms for models with
    single or multiple controls.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'rnaseq_genotype_label_map': Maps genotypes to display labels and model_groups
            - 'mouse_gene_metadata': Gene symbols for Ensembl IDs
            - One or more data files: CSV DataFrames containing individual expression
              results with columns: ensembl_gene_id, expression, model, genotype, age,
              sex, tissue, individualID
        required_input: Dictionary mapping required dataset names to required columns

    Returns:
        List of dictionaries, each representing a unique combination of gene, tissue,
        and model_group/name with individual_results containing all expression data
        grouped by age.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Pre-compute lookup dictionaries
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map"].fillna("")
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"].fillna("")

    # Create lookup dictionaries
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    label_map_dict = create_label_map_dict(rnaseq_genotype_label_map_df)

    # Validate that each model has consistent model_group values
    validate_model_group_consistency(rnaseq_genotype_label_map_df)

    model_group_dict = create_model_group_dict(rnaseq_genotype_label_map_df)

    # Create a dictionary of genotypes by model_group
    # This is used to filter data to only include genotypes that belong to the model_group
    genotypes_by_model_group = {}
    for _, row in rnaseq_genotype_label_map_df.iterrows():
        model_group = row["model_group"] if row["model_group"] else row["model"]
        if model_group not in genotypes_by_model_group:
            genotypes_by_model_group[model_group] = []
        if row["genotype"] not in genotypes_by_model_group[model_group]:
            genotypes_by_model_group[model_group].append(row["genotype"])

    # Create matched_control dictionary
    # For each model, find the genotype that maps to a control (contains "noncarrier" or is a control strain)
    matched_control_dict = {}
    for model in rnaseq_genotype_label_map_df["model"].unique():
        model_df = rnaseq_genotype_label_map_df[
            rnaseq_genotype_label_map_df["model"] == model
        ]
        # Look for control genotypes (typically contain "noncarrier")
        control_genotypes = model_df[
            model_df["genotype"].str.contains("noncarrier", case=False, na=False)
        ]
        if not control_genotypes.empty:
            matched_control = control_genotypes.iloc[0]["display_label"]
        else:
            # Fallback: use the first genotype's display label
            matched_control = (
                model_df.iloc[0]["display_label"] if not model_df.empty else ""
            )
        matched_control_dict[model] = matched_control

    output = []

    # Process files one at a time to reduce memory usage
    file_list = [k for k in datasets.keys() if k not in required_input]
    total_files = len(file_list)

    data_file_required_columns = [
        "ensembl_gene_id",
        "expression",
        "model",
        "genotype",
        "age",
        "sex",
        "tissue",
        "individualID",
    ]

    logger.info(f"Transform rna_de_individual total data files: {total_files}")
    logger.info(f"Data files list: {file_list}")

    for i, file_name in enumerate(file_list):
        data_file = datasets[file_name]
        file_output = _process_single_data_file(
            file_name,
            data_file,
            data_file_required_columns,
            gene_metadata_dict,
            label_map_dict,
            model_group_dict,
            matched_control_dict,
            genotypes_by_model_group,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform rna_de_individual total output entries: {len(output)}")
    return output
