"""
RNA Differential Expression Aggregate Transform Module

This module transforms RNA differential expression (RNA-DE) aggregate data for Model AD.
It combines multiple datasets including gene metadata, model information, genotype labels,
and biodomain annotations to create a structured output format.

The transformation:
- Filters to mouse genes only (ENSMUSG*), excluding human genes (ENSG*)
- Filters out combined-cohort rows where sex is "Females & Males", keeping single-sex rows only
- Maps plural sex values to singular display labels
- Groups differential expression data by gene, model, tissue, sex, case, and control
- Creates age-based entries containing log2 fold change and adjusted p-values
- Requires age to match the N months format and orders age entries numerically
- Normalizes zero values in log2 fold change for consistent representation
- Enriches data with gene symbols, biodomains, and model metadata
- Maps genotypes to display labels with strict validation (raises ValueError if mappings are missing)
- Applies the shared tissue alias mapping (Right Cerebral Hemisphere -> Hemibrain), case-insensitively
- Rounds numeric columns to 5 decimal places for consistency
- Processes multiple data files sequentially to minimize memory usage

Key Functions:
    transform_rna_de_aggregate: Main transformation function that orchestrates the data processing
    _create_output_entry_from_group: Creates a complete output entry from a grouped DataFrame by enriching it with metadata
    _process_single_data_file: Processes a single differential expression data file and transforms it into output entries

Required Inputs:
    - genotype_label_map: Maps (model, genotype) tuples to display labels, and provides
      model_type for each model. All genotypes used in data files must have corresponding entries
      or a ValueError will be raised.
    - mouse_gene_metadata: Gene symbols and aliases for Ensembl IDs
    - biodom_genes_mm: Biodomain annotations for mouse genes
    - Data files: One or more CSV files containing differential expression results with columns:
      ensembl_gene_id, log2foldchange, padj, model, case, control, age, sex, tissue
"""

import pandas as pd
from typing import Dict, List, Any
import logging

from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    build_biodomain_lookup,
    build_model_to_model_group_lookup,
    build_model_type_lookup,
    create_age_entries_from_group,
    create_gene_metadata_dict,
    filter_to_mouse_genes,
    log_file_processing_info,
    normalize_tissue,
    prepare_genotype_label_map,
)

logger = logging.getLogger(__name__)

STATIC_INPUTS = frozenset(
    {"genotype_label_map", "mouse_gene_metadata", "biodom_genes_mm"}
)


def _create_output_entry_from_group(
    group_key: tuple[str, str, str, str, str, str],
    group: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_type_dict: Dict[str, str],
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
    3. Orders age entries by numeric age value
    4. Constructs a comprehensive output dictionary combining all metadata and age-based data

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
        model_type_dict: Dictionary mapping model names to model types, derived from the
            genotype_label_map dataset. Used to classify models (e.g., "Familial AD",
            "Late Onset AD"). Unlike model_info, this dict includes entries for split variant
            models such as "Abca7*V1599M.5xFAD".

    Returns:
        Dictionary containing a complete output entry with the following structure:
            - 'ensembl_gene_id': str, Ensembl gene identifier
            - 'gene_symbol': str, Human-readable gene symbol (empty string if not found)
            - 'biodomains': List[str], List of biodomain names associated with the gene
            - 'name': dict, Object with 'link_url' and 'link_text' keys containing the model link path and display text
            - 'matched_control': str, Display label for the control genotype
            - 'model_group': str or None, Model group (None only when the model is absent
              from the label map; an empty value is rejected up front)
            - 'model_type': str, Model type classification (empty string if not found)
            - 'tissue': str, Tissue name (aliases already applied by the caller)
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
                'model_type': 'Familial AD',
                'tissue': 'Hemibrain',
                'sex': 'Male',
                '3 months': {'log2_fc': 1.234, 'adj_p_val': 0.001},
                '6 months': {'log2_fc': 2.456, 'adj_p_val': 0.0001}
            }

    Raises:
        ValueError: If the case or control genotype is not found in label_map_dict.
            The error includes details about the model, genotype, gene, tissue, and sex
            to help identify which mapping is missing.

    Note:
        Age entries are in numeric age order; the age format itself is validated by the
        data file column rules before grouping.
        Missing values in gene_metadata_dict and biodomain_dict result in empty strings or empty
        lists, not errors. However, missing entries in label_map_dict for the case or control
        genotypes will raise a ValueError.
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
                f"Please ensure the genotype_label_map dataset contains "
                f"an entry for model '{model}' and genotype '{k[1]}'."
            )
    name = label_map_dict[case_key]
    matched_control = label_map_dict[control_key]
    model_group = model_group_dict.get(model)
    biodomains = biodomain_dict.get(ensembl_gene_id, [])
    model_type = model_type_dict.get(model, "")

    age_entries = create_age_entries_from_group(
        group, ensembl_gene_id, model, tissue, sex
    )

    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": gene_symbol,
        "biodomains": biodomains,
        "name": {"link_url": f"models/{name}", "link_text": name},
        "matched_control": matched_control,
        "model_group": model_group,
        "model_type": model_type,
        "tissue": tissue,
        "sex": sex,
        **age_entries,
    }


def _process_single_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    gene_metadata_dict: Dict[str, str],
    label_map_dict: Dict[tuple[str, str], str],
    model_group_dict: Dict[str, str],
    biodomain_dict: Dict[str, List[str]],
    model_type_dict: Dict[str, str],
    file_index: int,
    total_files: int,
) -> List[Dict[str, Any]]:
    """
    Processes a single differential expression data file and transforms it into output entries.

    This function handles the complete processing pipeline for a single RNA differential
    expression data file. It performs filtering, grouping, and enrichment to create
    structured output entries. The function is designed to process files one at a time
    to minimize memory usage when handling large datasets.

    The processing pipeline includes:
    1. Logging file processing information (row count, column count, memory usage)
    2. Filtering to keep only mouse genes (ENSMUSG*), excluding human genes (ENSG*)
    3. Filtering out combined-cohort rows where sex is Females & Males
    4. Mapping plural sex values to singular display labels
    5. Applying the shared tissue alias mapping before grouping
    6. Rounding numeric columns to 5 decimal places for consistency
    7. Grouping data by gene, model, tissue, sex, case, and control
    8. Creating enriched output entries for each group using metadata dictionaries

    Each output entry represents a unique combination of gene, model, tissue, and sex,
    with age-based differential expression measurements and enriched metadata.

    Args:
        file_name: Name of the data file being processed. Used for logging and error
            reporting purposes.
        data_file: DataFrame containing the differential expression data. Expected columns
            include: ensembl_gene_id, log2foldchange, padj, model, case, control, age,
            sex, tissue.
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
        model_type_dict: Dictionary mapping model names to model types, derived from the
            genotype_label_map dataset. Used to classify models (e.g., "Familial AD",
            "Late Onset AD").
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
        The function filters out human genes to ensure only mouse (Mus musculus) data
        is processed, as indicated by Ensembl IDs starting with "ENSMUSG".
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)

    data_file = filter_to_mouse_genes(data_file)

    # Filter out combined-cohort rows; keep single-sex rows only
    data_file = data_file[data_file["sex"] != "Females & Males"]

    # Map plural source sex labels to their singular display form
    data_file["sex"] = remap_sex_labels(data_file["sex"])

    # Ensure tissue names are normalized before grouping to avoid duplicate groups due to casing or
    # alias differences
    data_file["tissue"] = normalize_tissue(data_file["tissue"])

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
            model_type_dict,
        )
        output_entries.append(output_entry)

    return output_entries


def transform_rna_de_aggregate(
    datasets: dict[str, pd.DataFrame],
) -> list[dict[str, Any]]:
    """
    Main transformation function that orchestrates the processing of RNA differential expression data.

    This function serves as the entry point for transforming RNA differential expression (RNA-DE)
    aggregate data files into a structured format suitable for Model AD. It coordinates the entire
    transformation pipeline, from data validation through enrichment to final output generation.

    The transformation workflow:
    1. Pre-computes lookup dictionaries from metadata for efficient data enrichment:
       - Gene symbols from Ensembl IDs
       - Display labels for genotype identifiers (case/control) by model
       - Model groups and types
       - Biodomain annotations
    2. Processes one or more differential expression data files sequentially:
       - Filters to mouse genes only (ENSMUSG*)
       - Filters out combined-cohort rows where sex is "Females & Males"
       - Maps plural sex values to singular display labels
       - Groups data by gene, model, tissue, sex, case, and control
       - Enriches each group with metadata
       - Creates age-based entries with log2 fold change and adjusted p-values
    3. Combines all processed entries into a single output list

    The output format groups differential expression measurements by unique combinations of
    gene, model, tissue, and sex, with each entry containing age-based measurements as
    nested dictionaries.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include:
            - 'genotype_label_map': Maps (model, genotype) combinations to display labels
              and organizes models into model_groups. Also provides model_type for each model,
              including split variant models (e.g., "Abca7*V1599M.5xFAD") that are not present
              in model_info.
            - 'mouse_gene_metadata': Gene symbols and aliases for Ensembl IDs
            - 'biodom_genes_mm': Biodomain annotations for mouse genes
            - One or more data files: CSV DataFrames containing differential expression
              results with columns: ensembl_gene_id, log2foldchange, padj, model, case,
              control, age, sex, tissue

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
                'model_type': 'Familial AD',
                'tissue': 'Hemibrain',
                'sex': 'Male',
                '3 months': {'log2_fc': 1.234, 'adj_p_val': 0.001},
                '6 months': {'log2_fc': 2.456, 'adj_p_val': 0.0001}
            }

    Raises:
        ValueError: If any data file is empty or invalid, or if any case or control
            genotype used in the data files is not found in the genotype_label_map
            dataset. Error messages include specific details about what validation failed.

    Note:
        This function processes data files sequentially (one at a time) rather than
        concatenating them first. The function also filters out human genes (ENSG*)
        to ensure only mouse (Mus musculus) data is included in the output.
    """
    # Pre-compute lookup dictionaries for efficient lookups
    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"]

    # Missing gene symbols are dropped inside create_gene_metadata_dict; lookups default
    # to an empty string when the Ensembl ID is absent.
    gene_metadata_dict = create_gene_metadata_dict(mouse_gene_metadata_df)

    # Create label map dictionaries for efficient lookups
    label_map_dict = genotype_label_map_df.set_index(["model", "genotype"])[
        "display_label"
    ].to_dict()

    model_group_dict = build_model_to_model_group_lookup(genotype_label_map_df)
    # model_type comes from the label map so split variant models are covered
    # without entries in model_info.
    model_type_dict = build_model_type_lookup(genotype_label_map_df)
    biodomain_dict = build_biodomain_lookup(datasets["biodom_genes_mm"])

    output = []

    # Process files one at a time to reduce memory usage
    file_list = [k for k in datasets.keys() if k not in STATIC_INPUTS]
    total_files = len(file_list)

    logger.info(f"Transform rna_de_aggregate total data files: {total_files}")
    logger.info(f"Data files list: {file_list}")

    for i, file_name in enumerate(file_list):
        # Download and process one file at a time
        data_file = datasets[file_name]
        file_output = _process_single_data_file(
            file_name,
            data_file,
            gene_metadata_dict,
            label_map_dict,
            model_group_dict,
            biodomain_dict,
            model_type_dict,
            i,
            total_files,
        )
        output.extend(file_output)

    logger.info(f"Transform rna_de_aggregate total output entries: {len(output)}")
    return output
