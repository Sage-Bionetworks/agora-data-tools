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

from agoradatatools.etl.utils import check_required_datasets_and_columns

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
        datasets["biodom_genes_mm"].dropna(axis="index", subset="ensembl_id").fillna("")
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
        logger.info(
            f"Processing {file_name} ({i+1}/{total_files}): {len(data_file)} rows, "
            f"{len(data_file.columns)} columns, "
            f"{data_file.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )

        check_required_datasets_and_columns(
            {file_name: data_file}, {file_name: data_file_required_columns}
        )

        # Check if data file is empty
        if len(data_file) == 0:
            raise ValueError(f"Data file {file_name} is empty")

        # Filter out rows with human gene ensembl IDs (ENSG*), keep only mouse (ENSMUSG*)
        data_file = data_file[data_file["ensembl_gene_id"].str.startswith("ENSMUSG")]

        # Round numeric columns to 5 decimal places for consistency
        data_file = data_file.round(decimals=5)

        # Group by gene, model, tissue, and sex to create one entry per group
        # Using groupby rather than pandas merge operations as a performance optimization
        grouped = data_file.groupby(
            ["ensembl_gene_id", "model", "tissue", "sex", "case", "control"]
        )

        for (ensembl_gene_id, model, tissue, sex, case, control), group in grouped:
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
            age_entries = {}
            for _, row in group.iterrows():
                age = str(row["age"])
                age_entries[age] = {
                    "log2_fc": float(row["log2foldchange"]),
                    "adj_p_val": float(row["padj"]),
                }

            # Sort age entries with error handling for format validation
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

            # If tissue is "Right Cerebral Hemisphere", change tissue to "Hemibrain"
            # Only expected for JAX models
            tissue = "Hemibrain" if tissue == "Right Cerebral Hemisphere" else tissue

            # Create the output entry
            output.append(
                {
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
            )

        # Clean up memory by deleting the processed file
        del data_file
        gc.collect()

    logger.info(f"Transform rna_de_aggregate total output entries: {len(output)}")
    return output
