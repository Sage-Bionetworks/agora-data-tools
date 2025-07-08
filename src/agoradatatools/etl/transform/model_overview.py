"""
This module contains the transformation logic for the model_overview datasets.
This is for the Model AD project.
"""
import pandas as pd
from typing import Any, Dict, List

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    remove_duplicates_keep_order,
)
from agoradatatools.etl.transform.model_details import process_genetic_info

REQUIRED_INPUT = {
    "model_info": [
        "model",
        "matched_controls",
        "model_type",
        "contributing_group",
        "study_synid",
        "rrid",
        "jax_id",
        "alzforum_id",
        "genotype",
        "aliases",
    ],
    "model_results_info": [
        "model",
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ],
    "allele_info": [
        "model",
        "modified_gene",
        "gene_ensembl_id",
        "allele",
        "allele_type",
        "mgi_allele_id",
    ],
    "human_transgene_allele_map": [
        "mgi_allele_id",
        "gene_symbol",
        "human_ensembl_id",
    ],
}


def transform_model_overview(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the model_overview source files into a structured format for Model AD.

    This function merges and processes the following input datasets:
        - model_info: Contains metadata about each model.
        - model_results_info: Contains information about available results for each model (e.g., gene expression, pathology).
        - allele_info: Contains allele and genetic modification details for each model.
        - human_transgene_allele_map: Maps mouse alleles to human Ensembl gene IDs.

    The transformation includes:
        1. Merging model_info and model_results_info on the "model" column.
        2. For each model, extracting genetic information using process_genetic_info, which maps alleles to human Ensembl IDs where possible.
        3. Building a structured dictionary for each model, including:
            - model metadata (model, model_type, matched_controls, etc.)
            - links to available results (gene_expression, disease_correlation, pathology, biomarkers)

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary mapping dataset names to their DataFrames.
        required_input (Dict[str, List[str]], optional): Dictionary specifying required datasets and columns. Defaults to REQUIRED_INPUT.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a transformed model overview record.

    Raises:
        ValueError: If required datasets or columns are missing.
    """

    check_required_datasets_and_columns(datasets, required_input)

    model_info = datasets["model_info"]
    model_results_info = datasets["model_results_info"]
    allele_info = datasets["allele_info"]
    human_transgene_allele_map = datasets["human_transgene_allele_map"]

    # Merge the two datasets on the "model" column
    merged_df = pd.merge(
        model_info, model_results_info, on="model", how="left", validate="1:1"
    )

    # Transform the merged dataframe into the target structure
    transformed_records = []

    for _, row in merged_df.iterrows():
        # Get genetic info for this model
        genetic_info = process_genetic_info(
            human_transgene_allele_map,
            model_alleles=allele_info[allele_info["model"] == row["model"]],
        )

        modified_genes = (
            remove_duplicates_keep_order(
                [gene["modified_gene"] for gene in genetic_info]
            )
            if genetic_info
            else []
        )
        modified_genes = [
            gene for gene in modified_genes if gene is not None and str(gene) != "nan"
        ]

        record = {
            "model": row["model"],
            "model_type": row["model_type"] if pd.notna(row["model_type"]) else None,
            "matched_controls": row["matched_controls"]
            if pd.notna(row["matched_controls"])
            else None,
            "gene_expression": {
                "link_url": f"comparison/expression?model={row['model']}"
            }
            if row["gene_expression"] is True
            else None,
            "disease_correlation": {
                "link_url": f"comparison/correlation?model={row['model']}"
            }
            if row["disease_correlation"] is True
            else None,
            "pathology": {"link_url": f"models/{row['model']}/pathology"}
            if row["pathology"] is True
            else None,
            "biomarkers": {"link_url": f"models/{row['model']}/biomarkers"}
            if row["biomarkers"] is True
            else None,
            "study_data": {
                "link_url": f"https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study={row['study_synid']}"
            }
            if pd.notna(row["study_synid"])
            else None,
            "jax_strain": {"link_url": f"https://jax.org/strain/{row['jax_id']}"}
            if pd.notna(row["jax_id"])
            else None,
            "center": {"link_name": row["contributing_group"]}
            if pd.notna(row["contributing_group"])
            else None,
            "modified_genes": modified_genes,
        }

        transformed_records.append(record)

    return transformed_records
