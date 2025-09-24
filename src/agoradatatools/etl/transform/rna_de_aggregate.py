import pandas as pd
from typing import Dict, List, Any
import logging

from agoradatatools.etl.extract import get_entity_as_df

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    _login_to_synapse,
)

logger = logging.getLogger(__name__)


REQUIRED_INPUT = {
    "rna_de_aggregate_data_files": ["file_name", "syn_id"],
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


def _quick_validate_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    required_columns: List[str] = [
        "ensembl_gene_id",
        "log2FoldChange",
        "padj",
        "model",
        "case",
        "control",
        "age",
        "sex",
        "tissue",
    ],
) -> None:
    """
    Quick validate the data file. Only validates the required columns.

    Args:
        file_name (str): The name of the file.
        data_file (pd.DataFrame): The data file to validate.
        required_columns (List[str]): The required columns to validate.

    Raises:
        ValueError: If the data file is empty or if the required columns are missing.

    Returns:
        None
    """
    if data_file.empty:
        raise ValueError("Data file is empty")

    # Validate required columns
    missing_columns = [col for col in required_columns if col not in data_file.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {file_name} dataset: {', '.join(missing_columns)}. "
            f"Please ensure the {file_name} dataset contains all required columns: {', '.join(required_columns)}."
        )


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
    biodom_genes_mm_df = datasets["biodom_genes_mm"].fillna("")

    # Create lookup dictionaries
    gene_metadata_dict = mouse_gene_metadata_df.set_index("ensembl_gene_id")[
        "gene_symbol"
    ].to_dict()
    model_info_dict = model_info_df.set_index("model")["model_type"].to_dict()

    # Create label map dictionaries for efficient lookups
    label_map_dict = {}
    model_group_dict = {}
    for _, row in rnaseq_genotype_label_map_df.iterrows():
        key = (row["model"], row["genotype"])
        label_map_dict[key] = row["display_label"]
        model_group_dict[row["model"]] = row["model_group"]

    # Create biodomain lookup dictionary
    biodomain_dict = (
        biodom_genes_mm_df.groupby("ensembl_id")["biodomain"].apply(list).to_dict()
    )

    # Validate model info
    # Temporarily commenting this out because I thought model info had "name" column instead of "model"
    # input_validation_model_info(model_info_df)

    output = []

    # Process files one at a time to reduce memory usage
    file_list = datasets["rna_de_aggregate_data_files"]
    total_files = len(file_list)
    syn = _login_to_synapse()

    for i, (file_name, syn_id) in enumerate(file_list.itertuples(index=False)):
        # Download and process one file at a time
        data_file = get_entity_as_df(syn_id=syn_id, source="csv", syn=syn)
        logger.info(
            f"Processing {file_name} ({i+1}/{total_files}): {len(data_file)} rows, {len(data_file.columns)} columns, {data_file.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )

        _quick_validate_data_file(file_name, data_file)

        # Filter out rows with human gene ensembl IDs (ENSG*), keep only mouse (ENSMUSG*)
        data_file = data_file[data_file["ensembl_gene_id"].str.startswith("ENSMUSG")]

        # Group by gene, model, tissue, and sex to create one entry per group
        grouped = data_file.groupby(["ensembl_gene_id", "model", "tissue", "sex"])

        for i, ((ensembl_gene_id, model, tissue, sex), group) in enumerate(grouped):
            # Get gene metadata using dictionary lookup
            gene_symbol = gene_metadata_dict.get(ensembl_gene_id, "")

            # Get case and control from first row of group
            case = group.iloc[0]["case"]
            control = group.iloc[0]["control"]

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
                    "log2_fc": float(f"{float(row['log2FoldChange']):.5g}"),
                    "adj_p_val": float(f"{float(row['padj']):.5g}"),
                }

            # If "name" includes "Jax" (case-insensitive) and tissue is "Right Cerebral Hemisphere", change tissue to "Hemibrain"
            if "jax" in str(name).lower() and tissue == "Right Cerebral Hemisphere":
                tissue = "Hemibrain"

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
                    **age_entries,  # Add all age entries as separate keys
                }
            )

        # Clean up memory by deleting the processed file
        del data_file
        import gc

        gc.collect()

    logger.info(f"Transform rna_de_aggregate total output entries: {len(output)}")
    return output
