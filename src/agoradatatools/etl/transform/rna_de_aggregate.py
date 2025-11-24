"""
RNA Differential Expression Aggregate Transform Module

This module transforms RNA differential expression (RNA-DE) aggregate data for Model AD.
It combines multiple datasets including gene metadata, model information, genotype labels,
and biodomain annotations to create a structured output format.

The transformation:
- Groups differential expression data by gene, model, tissue, and sex
- Creates age-based entries containing log2 fold change and adjusted p-values
- Enriches data with gene symbols, biodomains, and model metadata
- Maps genotypes to display labels for better readability
- Processes multiple data files efficiently to minimize memory usage

Key Functions:
    transform_rna_de_aggregate: Main transformation function that orchestrates the data processing
    validate_and_sort_age_entries: Validates and sorts age entries by numeric value

Required Inputs:
    - rnaseq_genotype_label_map: Maps models and genotypes to display labels
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - model_info: Model types and matched controls
    - biodom_genes_mm: Biodomain annotations for mouse genes
    - Data files: One or more CSV files containing differential expression results
"""

import pandas as pd
from typing import Dict, List, Any
import logging
import gc

from agoradatatools.etl.utils import check_required_datasets_and_columns, normalize_zero

logger = logging.getLogger(__name__)

REQUIRED_INPUT = {
    "rnaseq_genotype_label_map": ["model", "model_group", "display_label", "genotype"],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
    "model_info": ["model", "matched_controls", "model_type"],
    "biodom_genes_mm": [
        "biodomain",
        "abbr",
        "label",
        "color",
        "go_id",
        "goterm_name",
        "n_symbol",
        "symbol",
        "ensembl_id",
    ],
}


def validate_and_sort_age_entries(
    age_entries: Dict[str, Dict[str, float]],
    ensembl_gene_id: str,
    model: str,
    tissue: str,
    sex: str,
) -> Dict[str, Dict[str, float]]:
    """
    Validates and sorts age entries by their numeric value.

    Age entries are expected to be in the format 'N months' where N is an integer.
    This function validates that all age strings are properly formatted and not empty,
    then sorts them numerically.

    Args:
        age_entries: Dictionary mapping age strings to their log2_fc and adj_p_val values
        ensembl_gene_id: Gene identifier for error reporting
        model: Model name for error reporting
        tissue: Tissue type for error reporting
        sex: Sex category for error reporting

    Returns:
        Dictionary of age entries sorted by numeric age value

    Raises:
        ValueError: If any age string is empty, whitespace-only, or not in 'N months' format
    """
    # Validate that no age strings are empty or whitespace-only
    for age in age_entries.keys():
        age_stripped = age.strip()
        if not age_stripped:
            raise ValueError(
                f"Empty or whitespace-only age value found in data for gene '{ensembl_gene_id}', "
                f"model '{model}', tissue '{tissue}', sex '{sex}'. "
                f"Expected 'N months' format but found: '{age}'"
            )

    # Sort age entries by numeric value with error handling for format validation
    try:
        sorted_ages = dict(
            sorted(age_entries.items(), key=lambda x: int(x[0].split()[0]))
        )
    except (ValueError, IndexError) as e:
        raise ValueError(
            f"Invalid age format in data for gene '{ensembl_gene_id}', "
            f"model '{model}', tissue '{tissue}', sex '{sex}'. "
            f"Expected 'N months' format but found: {list(age_entries.keys())}. "
            f"Original error: {e}"
        ) from e

    return sorted_ages


def _create_age_entries_from_group(
    group: pd.DataFrame,
    ensembl_gene_id: str,
    model: str,
    tissue: str,
    sex: str,
) -> Dict[str, Dict[str, float]]:
    """
    Creates age-based entries from a grouped DataFrame.

    Args:
        group: DataFrame group containing age, log2foldchange, and padj columns
        ensembl_gene_id: Gene identifier for error reporting
        model: Model name for error reporting
        tissue: Tissue type for error reporting
        sex: Sex category for error reporting

    Returns:
        Dictionary mapping age strings to log2_fc and adj_p_val values
    """
    age_entries = {}
    for row in group.itertuples(index=False):
        age = str(row.age)
        # Check for negative p-values only if padj is not NA
        if not pd.isna(row.padj) and float(row.padj) < 0.0:
            raise ValueError(
                f"Negative adjusted p-value found in data for gene '{ensembl_gene_id}', "
                f"model '{model}', tissue '{tissue}', sex '{sex}'. "
                f"Expected positive adjusted p-value but found: '{row.padj}'"
            )
        age_entries[age] = {
            "log2_fc": normalize_zero(float(row.log2foldchange)),
            "adj_p_val": 0.0 if pd.isna(row.padj) else float(row.padj),
        }
    return age_entries


def _create_output_entry_from_group(
    group_key: tuple,
    group: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple, str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_info_dict: Dict[str, str],
) -> Dict[str, Any]:
    """
    Creates an output entry from a grouped DataFrame.

    Args:
        group_key: Tuple containing (ensembl_gene_id, model, tissue, sex, case, control)
        group: DataFrame group containing age-based differential expression data
        gene_metadata_dict: Dictionary mapping ensembl_gene_id to gene_symbol
        label_map_dict: Dictionary mapping (model, genotype) to display_label
        model_group_dict: Dictionary mapping model to model_group
        biodomain_dict: Dictionary mapping ensembl_id to list of biodomains
        model_info_dict: Dictionary mapping model to model_type

    Returns:
        Dictionary containing the output entry for this group
    """
    ensembl_gene_id, model, tissue, sex, case, control = group_key

    # Get gene metadata using dictionary lookup
    gene_symbol = gene_metadata_dict.get(ensembl_gene_id, "")

    # Use dictionary lookups instead of .loc[] operations
    name = label_map_dict.get((model, case), model)
    matched_control = label_map_dict.get((model, control), model)
    model_group = model_group_dict.get(model)

    # Get biodomains using dictionary lookup
    biodomains = biodomain_dict.get(ensembl_gene_id, [])

    # Get model type using dictionary lookup
    model_type = model_info_dict.get(model, "")

    # Create age-based entries
    age_entries = _create_age_entries_from_group(
        group, ensembl_gene_id, model, tissue, sex
    )

    # Validate and sort age entries
    sorted_ages = validate_and_sort_age_entries(
        age_entries, ensembl_gene_id, model, tissue, sex
    )

    # If tissue is "Right Cerebral Hemisphere", change tissue to "Hemibrain"
    # Only expected for JAX models
    tissue = "Hemibrain" if tissue == "Right Cerebral Hemisphere" else tissue

    # Create the output entry
    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": gene_symbol,
        "biodomains": biodomains,
        "name": name,
        "matched_control": matched_control,
        "model_group": model_group if model_group != "" else None,
        "model_type": model_type,
        "tissue": tissue,
        "sex": sex,
        **sorted_ages,
    }


def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    data_file_required_columns: List[str],
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple, str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_info_dict: Dict[str, str],
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single data file and returns output entries.

    Args:
        file_name: Name of the data file being processed
        data_file: DataFrame containing the data file contents
        data_file_required_columns: List of required column names
        gene_metadata_dict: Dictionary mapping ensembl_gene_id to gene_symbol
        label_map_dict: Dictionary mapping (model, genotype) to display_label
        model_group_dict: Dictionary mapping model to model_group
        biodomain_dict: Dictionary mapping ensembl_id to list of biodomains
        model_info_dict: Dictionary mapping model to model_type
        file_index: Current file index (0-based)
        total_files: Total number of files to process

    Returns:
        List of output entry dictionaries
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

    # Filter out rows with human gene ensembl IDs (ENSG*), keep only mouse (ENSMUSG*)
    data_file = data_file[data_file["ensembl_gene_id"].str.startswith("ENSMUSG")]

    # Round numeric columns to 5 decimal places for consistency
    data_file = data_file.round(decimals=5)

    # Group by gene, model, tissue, and sex to create one entry per group
    # Using groupby rather than pandas merge operations as a performance optimization
    grouped = data_file.groupby(
        ["ensembl_gene_id", "model", "tissue", "sex", "case", "control"]
    )

    output_entries = []
    for group_key, group in grouped:
        output_entry = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            label_map_dict,
            model_group_dict,
            biodomain_dict,
            model_info_dict,
        )
        output_entries.append(output_entry)

    # Clean up memory by deleting the processed file
    del data_file
    gc.collect()

    return output_entries


def transform_rna_de_aggregate(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the rna_de_aggregate source files into a structured format for Model AD.
    Groups by gene, model, tissue, and sex, with age-based entries containing log2_fc and adj_p_val.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Pre-compute lookup dictionaries for efficient lookups
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map"].fillna("")
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"].fillna("")
    model_info_df = datasets["model_info"].fillna("")
    biodom_genes_mm_df = (
        datasets["biodom_genes_mm"]
        .dropna(axis="index", subset=["ensembl_id"])
        .fillna("")
    )

    # Create lookup dictionaries
    gene_metadata_dict = mouse_gene_metadata_df.set_index("ensembl_gene_id")[
        "gene_symbol"
    ].to_dict()
    model_info_dict = model_info_df.set_index("model")["model_type"].to_dict()

    # Create label map dictionaries for efficient lookups
    label_map_dict = rnaseq_genotype_label_map_df.set_index(["model", "genotype"])[
        "display_label"
    ].to_dict()

    # Validate that each model has consistent model_group values
    inconsistent_models = (
        rnaseq_genotype_label_map_df.groupby("model")["model_group"]
        .nunique()
        .pipe(lambda x: x[x > 1].index.tolist())
    )
    if inconsistent_models:
        raise ValueError(
            f"Each model must have a consistent model_group value in rnaseq_genotype_label_map. "
            f"Models with inconsistent model_group values: {inconsistent_models}"
        )

    model_group_dict = (
        rnaseq_genotype_label_map_df.groupby("model")["model_group"].first().to_dict()
    )

    # Create biodomain lookup dictionary
    biodomain_dict = (
        biodom_genes_mm_df[["ensembl_id", "biodomain"]]
        .drop_duplicates()
        .groupby("ensembl_id")["biodomain"]
        .apply(list)
        .to_dict()
    )

    output = []

    # Process files one at a time to reduce memory usage
    file_list = [k for k in datasets.keys() if k not in required_input]
    total_files = len(file_list)

    data_file_required_columns = [
        "ensembl_gene_id",
        "log2foldchange",
        "padj",
        "model",
        "case",
        "control",
        "age",
        "sex",
        "tissue",
    ]

    logger.info(f"Transform rna_de_aggregate total data files: {total_files}")
    logger.info(f"Data files list: {file_list}")

    for i, file_name in enumerate(file_list):
        # Download and process one file at a time
        data_file = datasets[file_name]
        file_output = _process_single_data_file(
            file_name,
            data_file,
            data_file_required_columns,
            gene_metadata_dict,
            label_map_dict,
            model_group_dict,
            biodomain_dict,
            model_info_dict,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform rna_de_aggregate total output entries: {len(output)}")
    return output
