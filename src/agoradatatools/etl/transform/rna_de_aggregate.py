import pandas as pd
from typing import Dict, List, Any
import re

from agoradatatools.etl.extract import get_entity_as_df

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    input_validation_model_info,
    _login_to_synapse
)


REQUIRED_INPUT = {
    "rna_de_aggregate_data_files": [
        "file_name",
        "syn_id"
    ],
    "rnaseq_genotype_label_map": [
        "model",
        "model_group",
        "display_label",
        "genotype"
    ],
    "mouse_gene_metadata": [
        "ensembl_gene_id",
        "gene_symbol",
        "alias"
    ],
    "model_info": [
        "name",
        "matched_controls",
        "model_type"
    ],
    "biodom_genes_mm": [
        "Biodomain",
        "abbr",
        "label",
        "color",
        "GO_ID",
        "GOterm_Name",
        "n_symbol",
        "symbol",
        "ensembl_id"
    ],
}


def get_data_files(
    df: pd.DataFrame,
    required_columns: List[str] = ["ensembl_gene_id", "log2FoldChange", "padj", "model", "case", "control", "age", "sex","tissue"]
    ) -> Dict[str, pd.DataFrame]:
    """
    Download the data files from Synapse and return a dictionary of dataframes.

    Args:
        df (pd.DataFrame): The dataframe containing the data files.
        required_columns (List[str]): The required columns for the data files.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary of dataframes.
    """
    syn = _login_to_synapse()
    data_files = {}
    for file_name, syn_id in df.itertuples(index=False):
        data_file = get_entity_as_df(syn_id=syn_id, source="csv", syn=syn)

        # Validate required columns
        missing_columns = [
            col for col in required_columns if col not in data_file.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in {file_name} dataset: {', '.join(missing_columns)}. "
                f"Please ensure the {file_name} dataset contains all required columns: {', '.join(required_columns)}."
            )

        # Add file to output dictionary
        data_files[file_name] = data_file
    return data_files


def transform_rna_de_aggregate(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the rna_de_aggregate source files into a structured format for Model AD.
    """
    check_required_datasets_and_columns(datasets, required_input)

    data_files = get_data_files(datasets["rna_de_aggregate_data_files"])
    rnaseq_genotype_label_map_df = datasets["rnaseq_genotype_label_map"].fillna("")
    mouse_gene_metadata_df = datasets["mouse_gene_metadata"].fillna("")
    model_info_df = datasets["model_info"].fillna("")
    biodom_genes_mm_df = datasets["biodom_genes_mm"].fillna("")

    # Validate model info
    input_validation_model_info(model_info_df)






