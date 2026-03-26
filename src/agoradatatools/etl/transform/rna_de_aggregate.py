"""
RNA Differential Expression Aggregate Transform Module

This module transforms RNA differential expression (RNA-DE) aggregate data for Model AD.
It combines multiple datasets including gene metadata, model information, genotype labels,
and biodomain annotations to create a structured output format.

The transformation:
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Groups differential expression data by gene, model, tissue, sex, case, and control
- Creates age-based entries containing log2 fold change and adjusted p-values
- Validates and sorts age entries by numeric value
- Normalizes zero values in log2 fold change for consistent representation
- Enriches data with gene symbols, biodomains, and model metadata
- Maps genotypes to display labels with strict validation (raises ValueError if mappings are missing)
- Applies special tissue name transformation for JAX models ("Right Cerebral Hemisphere" -> "Hemibrain")
- Rounds numeric columns to 5 decimal places for consistency
- Processes multiple data files sequentially to minimize memory usage

Key Functions:
    transform_rna_de_aggregate: Main transformation function that orchestrates the data processing
    _validate_and_sort_age_entries: Validates and sorts age entries by numeric value
    _create_age_entries_from_group: Creates age-based entries from a grouped DataFrame with normalization and validation
    _create_output_entry_from_group: Creates a complete output entry from a grouped DataFrame by enriching it with metadata
    _process_single_data_file: Processes a single differential expression data file and transforms it into output entries

Required Inputs:
    - rnaseq_genotype_label_map: Maps (model, genotype) tuples to display labels. All genotypes
      used in data files must have corresponding entries or a ValueError will be raised.
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - model_info: Model types and matched controls
    - biodom_genes_mm: Biodomain annotations for mouse genes
    - Data files: One or more CSV files containing differential expression results with columns:
      ensembl_gene_id, log2foldchange, padj, model, case, control, age, sex, tissue
"""

import pandas as pd
from typing import Dict, List, Any
import logging
import gc

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    normalize_null_values,
    normalize_zero,
)
from agoradatatools.etl.transform.model_ad_transform_utils import preprocess_model_info

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


def _validate_and_sort_age_entries(
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
    Creates age-based entries from a grouped DataFrame containing differential expression data.

    This function processes a DataFrame group that has been grouped by gene, model, tissue, and sex.
    It extracts age-specific differential expression measurements (log2 fold change and adjusted
    p-values) for each age timepoint in the group. The function performs data normalization and
    validation, including:
    - Normalizing zero values in log2 fold change to ensure consistent representation
    - Converting missing (NA) adjusted p-values to 0.0
    - Validating that adjusted p-values are non-negative when present

    The resulting dictionary structure allows for easy lookup of differential expression metrics
    by age timepoint, which is used downstream to create structured output entries.

    Args:
        group: DataFrame group containing age, log2foldchange, and padj columns. Each row
            represents a single age timepoint measurement for the grouped combination of
            gene, model, tissue, and sex.
        ensembl_gene_id: Gene identifier (e.g., 'ENSMUSG00000000001') used for error reporting
            when validation fails.
        model: Model name used for error reporting when validation fails.
        tissue: Tissue type used for error reporting when validation fails.
        sex: Sex category used for error reporting when validation fails.

    Returns:
        Dictionary mapping age strings (e.g., '3 months', '6 months') to nested dictionaries
        containing:
            - 'log2_fc': float, normalized log2 fold change value (zero values normalized)
            - 'adj_p_val': float, adjusted p-value (NA values converted to 0.0)

        Example:
            {
                '3 months': {'log2_fc': 1.234, 'adj_p_val': 0.001},
                '6 months': {'log2_fc': 2.456, 'adj_p_val': 0.0}
            }

    Raises:
        ValueError: If any adjusted p-value (padj) is negative when not NA. This indicates
            invalid data that should be caught during processing.
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
            "adj_p_val": 1.0 if pd.isna(row.padj) else float(row.padj),
        }
    return age_entries


def _create_output_entry_from_group(
    group_key: tuple[str, str, str, str, str, str],
    group: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_info_dict: Dict[str, str],
) -> Dict[str, Any]:
    """
    Creates a complete output entry from a grouped DataFrame by enriching it with metadata.

    This function orchestrates the creation of a structured output entry for a single group
    of differential expression data. It takes a DataFrame group that has been grouped by
    gene, model, tissue, and sex, and enriches it with:
    - Gene metadata (symbol, biodomains)
    - Model information (display labels, model group, model type)
    - Age-based differential expression measurements (log2 fold change and adjusted p-values)

    The function performs several key operations:
    1. Extracts metadata from lookup dictionaries for efficient data enrichment
    2. Creates age-based entries from the grouped DataFrame using helper functions
    3. Validates and sorts age entries by numeric age value
    4. Applies special tissue name transformation (JAX models: "Right Cerebral Hemisphere" -> "Hemibrain")
    5. Constructs a comprehensive output dictionary combining all metadata and age-based data

    The resulting entry represents a complete record for one gene-model-tissue-sex combination
    with all associated age timepoint measurements, ready for inclusion in the final output.

    Args:
        group_key: Tuple containing (ensembl_gene_id, model, tissue, sex, case, control)
            that uniquely identifies this group. The case and control values represent the
            genotype identifiers (e.g., '5XFAD_carrier', '5XFAD_noncarrier') from the input
            data files, which are mapped to display labels via the label_map_dict.
        group: DataFrame group containing age-based differential expression data. Each row
            represents a single age timepoint with columns: age, log2foldchange, padj.
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols. Used to
            enrich entries with human-readable gene names.
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels.
            Used to map genotype identifiers to human-readable display names for case and
            control conditions. For example, ('5xFAD (UCI)', '5XFAD_carrier') -> '5xFAD (UCI)',
            or ('5xFAD (UCI)', '5XFAD_noncarrier') -> 'C57BL/6J'.
        model_group_dict: Dictionary mapping model names to model groups. Used to categorize
            models into groups (e.g., "5XFAD", "APP/PS1").
        biodomain_dict: Dictionary mapping Ensembl gene IDs to lists of biodomain names.
            Used to annotate genes with their associated biological domains.
        model_info_dict: Dictionary mapping model names to model types. Used to classify
            models (e.g., "knockout", "transgenic").

    Returns:
        Dictionary containing a complete output entry with the following structure:
            - 'ensembl_gene_id': str, Ensembl gene identifier
            - 'gene_symbol': str, Human-readable gene symbol (empty string if not found)
            - 'biodomains': List[str], List of biodomain names associated with the gene
            - 'name': dict, Object with 'link_url' and 'link_text' keys containing the model link path and display text
            - 'matched_control': str, Display label for the control genotype
            - 'model_group': str or None, Model group name (None if empty)
            - 'model_type': str or None, Model type classification (None if not found)
            - 'tissue': str, Tissue name (transformed for JAX models if applicable)
            - 'sex': str, Sex category
            - Age-based entries: Dictionary keys are age strings (e.g., '3 months', '6 months')
              with values containing 'log2_fc' and 'adj_p_val' for each age timepoint

        Example:
            {
                'ensembl_gene_id': 'ENSMUSG00000000001',
                'gene_symbol': 'Gapdh',
                'biodomains': ['Synaptic', 'Metabolic'],
                'name': {'link_url': 'models/5XFAD', 'link_text': '5XFAD'},
                'matched_control': 'Wild-type',
                'model_group': '5XFAD',
                'model_type': 'transgenic',
                'tissue': 'Hemibrain',
                'sex': 'M',
                '3 months': {'log2_fc': 1.234, 'adj_p_val': 0.001},
                '6 months': {'log2_fc': 2.456, 'adj_p_val': 0.0001}
            }

    Raises:
        ValueError: If the case or control genotype is not found in label_map_dict.
            The error includes details about the model, genotype, gene, tissue, and sex
            to help identify which mapping is missing.

    Note:
        Age entries are validated and sorted numerically before being included in the output.
        Missing values in gene_metadata_dict, biodomain_dict, and model_info_dict result
        in empty strings or empty lists, not errors. However, missing entries in label_map_dict
        for the case or control genotypes will raise a ValueError.
    """
    ensembl_gene_id, model, tissue, sex, case, control = group_key

    gene_symbol = gene_metadata_dict.get(ensembl_gene_id, "")

    # Lookup name and matched_control - raise error if not found
    case_key = (model, case)
    control_key = (model, control)
    for k in [case_key, control_key]:
        if k not in label_map_dict:
            raise ValueError(
                f"Label mapping not found for genotype. "
                f"Model: '{model}', Genotype: '{k[1]}', "
                f"Gene: {ensembl_gene_id}, Tissue: {tissue}, Sex: {sex}. "
                f"Please ensure the rnaseq_genotype_label_map dataset contains "
                f"an entry for model '{model}' and genotype '{k[1]}'."
            )
    name = label_map_dict[case_key]
    matched_control = label_map_dict[control_key]
    model_group = model_group_dict.get(model)
    biodomains = biodomain_dict.get(ensembl_gene_id, [])
    model_type = model_info_dict.get(model)

    age_entries = _create_age_entries_from_group(
        group, ensembl_gene_id, model, tissue, sex
    )

    sorted_ages = _validate_and_sort_age_entries(
        age_entries, ensembl_gene_id, model, tissue, sex
    )

    # If tissue is "Right Cerebral Hemisphere", change tissue to "Hemibrain"
    # Only expected for JAX models
    tissue = "Hemibrain" if tissue == "Right Cerebral Hemisphere" else tissue

    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": gene_symbol,
        "biodomains": biodomains,
        "name": {"link_url": f"models/{name}", "link_text": name},
        "matched_control": matched_control,
        "model_group": model_group,
        "model_type": model_type,
        "tissue": tissue,
        "sex_cohort": sex,
        **sorted_ages,
    }


def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    data_file_required_columns: List[str],
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_info_dict: Dict[str, str],
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single differential expression data file and transforms it into output entries.

    This function handles the complete processing pipeline for a single RNA differential
    expression data file. It performs data validation, filtering, grouping, and enrichment
    to create structured output entries. The function is designed to process files one at
    a time to minimize memory usage when handling large datasets.

    The processing pipeline includes:
    1. Logging file processing information (row count, column count, memory usage)
    2. Validating that the data file is not empty
    3. Validating that all required columns are present
    4. Filtering to keep only mouse genes (ENSMUSG*), excluding human genes (ENSG*)
    5. Rounding numeric columns to 5 decimal places for consistency
    6. Grouping data by gene, model, tissue, sex, case, and control
    7. Creating enriched output entries for each group using metadata dictionaries
    8. Cleaning up memory by deleting the processed DataFrame and running garbage collection

    Each output entry represents a unique combination of gene, model, tissue, and sex,
    with age-based differential expression measurements and enriched metadata.

    Args:
        file_name: Name of the data file being processed. Used for logging and error
            reporting purposes.
        data_file: DataFrame containing the differential expression data. Expected columns
            include: ensembl_gene_id, log2foldchange, padj, model, case, control, age,
            sex, tissue.
        data_file_required_columns: List of required column names that must be present
            in the data_file. Used for validation before processing.
        gene_metadata_dict: Dictionary mapping Ensembl gene IDs to gene symbols. Used
            to enrich output entries with human-readable gene names.
        label_map_dict: Dictionary mapping (model, genotype) tuples to display labels.
            Used to map genotype identifiers to human-readable display names for case and
            control conditions. For example, ('5xFAD (UCI)', '5XFAD_carrier') -> '5xFAD (UCI)',
            or ('5xFAD (UCI)', '5XFAD_noncarrier') -> 'C57BL/6J'.
        model_group_dict: Dictionary mapping model names to model groups. Used to
            categorize models into groups (e.g., "5XFAD", "APP/PS1").
        biodomain_dict: Dictionary mapping Ensembl gene IDs to lists of biodomain names.
            Used to annotate genes with their associated biological domains.
        model_info_dict: Dictionary mapping model names to model types. Used to classify
            models (e.g., "knockout", "transgenic").
        file_index: Current file index (0-based) for progress tracking. Used in logging
            to indicate which file is being processed (e.g., "Processing file 3/10").
        total_files: Total number of files to process. Used in logging to show progress
            (e.g., "Processing file 3/10").

    Returns:
        List of output entry dictionaries. Each dictionary represents a unique combination
        of gene, model, tissue, and sex, enriched with metadata and containing age-based
        differential expression measurements. The structure matches the output format from
        `_create_output_entry_from_group`.

    Raises:
        ValueError: If the data file is empty, if required columns are missing, or if any
            case or control genotype is not found in label_map_dict during group processing.
            The error message includes the file name and specific details for debugging purposes.

    Note:
        This function performs memory cleanup by explicitly deleting the processed DataFrame
        and calling garbage collection. This is important when processing multiple large
        files sequentially to prevent memory exhaustion. The function filters out human
        genes to ensure only mouse (Mus musculus) data is processed, as indicated by
        Ensembl IDs starting with "ENSMUSG".
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
    Main transformation function that orchestrates the processing of RNA differential expression data.

    This function serves as the entry point for transforming RNA differential expression (RNA-DE)
    aggregate data files into a structured format suitable for Model AD. It coordinates the entire
    transformation pipeline, from data validation through enrichment to final output generation.

    The transformation workflow:
    1. Validates that all required input datasets and columns are present
    2. Pre-computes lookup dictionaries from metadata for efficient data enrichment:
       - Gene symbols from Ensembl IDs
       - Display labels for genotype identifiers (case/control) by model
       - Model groups and types
       - Biodomain annotations
    3. Validates data consistency (e.g., ensures each model has a consistent model_group)
    4. Processes one or more differential expression data files sequentially:
       - Filters to mouse genes only (ENSMUSG*)
       - Groups data by gene, model, tissue, sex, case, and control
       - Enriches each group with metadata
       - Creates age-based entries with log2 fold change and adjusted p-values
    5. Combines all processed entries into a single output list

    The output format groups differential expression measurements by unique combinations of
    gene, model, tissue, and sex, with each entry containing age-based measurements as
    nested dictionaries.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'rnaseq_genotype_label_map': Maps (model, genotype) combinations to display labels
              and organizes models into model_groups. Each row specifies how a genotype identifier
              should be displayed for a given model.
            - 'mouse_gene_metadata': Gene symbols and aliases for Ensembl IDs
            - 'model_info': Model types and metadata
            - 'biodom_genes_mm': Biodomain annotations for mouse genes
            - One or more data files: CSV DataFrames containing differential expression
              results with columns: ensembl_gene_id, log2foldchange, padj, model, case,
              control, age, sex, tissue
        required_input: Dictionary mapping required dataset names to lists of required
            column names. Defaults to REQUIRED_INPUT constant. Used to validate that all
            necessary metadata datasets are present with correct structure.

    Returns:
        List of dictionaries, where each dictionary represents a unique combination of
        gene, model, tissue, and sex. Each entry contains:
            - Gene identifiers: ensembl_gene_id, gene_symbol
            - Model information: name (object with link_url and link_text), matched_control, model_group, model_type
            - Sample information: tissue, sex
            - Biodomain annotations: biodomains (list)
            - Age-based measurements: Dictionary keys are age strings (e.g., '3 months')
              with values containing 'log2_fc' and 'adj_p_val' for each age timepoint

        Example entry structure:
            {
                'ensembl_gene_id': 'ENSMUSG00000000001',
                'gene_symbol': 'Gapdh',
                'biodomains': ['Synaptic', 'Metabolic'],
                'name': {'link_url': 'models/5XFAD', 'link_text': '5XFAD'},
                'matched_control': 'Wild-type',
                'model_group': '5XFAD',
                'model_type': 'transgenic',
                'tissue': 'Hemibrain',
                'sex': 'M',
                '3 months': {'log2_fc': 1.234, 'adj_p_val': 0.001},
                '6 months': {'log2_fc': 2.456, 'adj_p_val': 0.0001}
            }

    Raises:
        ValueError: If required datasets or columns are missing, if any model has
            inconsistent model_group values, if any data file is empty or invalid,
            or if any case or control genotype used in the data files is not found
            in the rnaseq_genotype_label_map dataset.
            Error messages include specific details about what validation failed.

    Note:
        This function processes data files sequentially (one at a time) rather than
        loading all files into memory simultaneously. This design minimizes memory
        usage when processing large numbers of files. Each file is processed, its
        entries are added to the output list, and then the file is deleted from
        memory before processing the next file. The function also filters out human
        genes (ENSG*) to ensure only mouse (Mus musculus) data is included in the output.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Pre-compute lookup dictionaries for efficient lookups
    rnaseq_genotype_label_map_df = normalize_null_values(
        datasets["rnaseq_genotype_label_map"]
    )
    mouse_gene_metadata_df = normalize_null_values(
        datasets["mouse_gene_metadata"], empty_string_columns=["gene_symbol"]
    )
    model_info_df = preprocess_model_info(datasets["model_info"])
    biodom_genes_mm_df = datasets["biodom_genes_mm"].dropna(
        axis="index", subset=["ensembl_id"]
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
        .nunique(dropna=False)
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
