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
from agoradatatools.etl.transform.model_ad_transform_utils import (
    build_gene_expression_url,
    process_genetic_info,
    preprocess_model_info,
)

REQUIRED_INPUT = {
    "model_info": [
        "name",
        "matched_controls",
        "model_type",
        "contributing_group",
        "study_synid",
        "rrid",
        "jax_id",
        "alzforum_id",
        "genotype",
        "aliases",
        "url_categories_value",
        "url_models_value",
    ],
    "model_results_info": [
        "name",
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ],
    "allele_info": [
        "name",
        "gene",
        "gene_ensembl_id",
        "allele",
        "allele_type",
        "mgi_allele_id",
    ],
    "human_transgene_allele_map": [
        "mgi_allele_id",
        "gene_symbol",
        "ensembl_id",
    ],
}


def get_list_of_available_data(row: Dict[str, Any]) -> List[str]:
    """
    Get a list of available data for a given model.
    This is used to populate the "Available Data" section of the model overview page.
    If the value is not None, it is considered available.

    Args:
        row (Dict[str, Any]): A dictionary containing the model information.

    Returns:
        List[str]: A list of available data for the model.
    """
    fields = {
        "gene_expression": "Gene Expression",
        "disease_correlation": "Disease Correlation",
        "pathology": "Pathology",
        "biomarkers": "Biomarkers",
    }

    available_data = [
        label for key, label in fields.items() if row.get(key) is not None
    ]

    return available_data


def get_center_link_url(contributing_group: str) -> str:
    """
    Get the link URL for the center.
    """
    if isinstance(contributing_group, str):
        if contributing_group.upper() == "UCI":
            return (
                "http://model-ad.org/uci-disease-model-development-and-phenotyping-dmp/"
            )
        elif contributing_group.upper() == "IU/JAX/PITT":
            return "https://www.model-ad.org/iu-jax-pitt-disease-modeling-project/"
    raise ValueError(f"Invalid contributing group: {contributing_group}")


def transform_model_overview(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the model_overview source files into a structured format for Model AD.

    This function merges and processes the following input datasets:
        - model_info: Contains metadata about each model.
        - model_results_info: Contains information about available results for each model (e.g., gene expression,
          pathology).
        - allele_info: Contains allele and genetic modification details for each model.
        - human_transgene_allele_map: Maps mouse alleles to human Ensembl gene IDs.

    The transformation includes:
        1. Merging model_info and model_results_info
        2. For each model, extracting genetic information using process_genetic_info, which maps alleles to human
           Ensembl IDs where possible.
        3. Building a structured dictionary for each model, including:
            - model metadata (name, model_type, matched_controls, etc.)
            - links to available results (gene_expression, disease_correlation, pathology, biomarkers)

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary mapping dataset names to their DataFrames.
        required_input (Dict[str, List[str]], optional): Dictionary specifying required datasets and columns. Defaults
        to REQUIRED_INPUT.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a transformed model overview record.

    Raises:
        ValueError: If required datasets or columns are missing.
    """

    check_required_datasets_and_columns(datasets, required_input)

    merged_df = preprocess_model_info(
        datasets["model_info"], datasets["model_results_info"], model_name_col="name"
    )

    allele_info_df = process_genetic_info(
        datasets["human_transgene_allele_map"], datasets["allele_info"]
    )

    # Transform the merged dataframe into the target structure
    transformed_records = []

    for _, row in merged_df.iterrows():
        # Get genetic info for this model
        genetic_info = allele_info_df[allele_info_df["name"] == row["name"]]
        modified_genes = remove_duplicates_keep_order(genetic_info["gene"].tolist())

        row["modified_genes"] = [gene for gene in modified_genes if gene != ""]

        # Build the links first
        row["gene_expression"] = (
            {"link_url": build_gene_expression_url(row)}
            if row["gene_expression"]
            else None
        )
        row["disease_correlation"] = (
            {"link_url": f"comparison/correlation?models={row['name']}"}
            if row["disease_correlation"]
            else None
        )
        row["pathology"] = (
            {"link_url": f"models/{row['name']}/pathology"}
            if row["pathology"]
            else None
        )
        row["biomarkers"] = (
            {"link_url": f"models/{row['name']}/biomarkers"}
            if row["biomarkers"]
            else None
        )
        row["study_data"] = (
            {
                "link_url": f"https://adknowledgeportal.synapse.org/Explore/Studies/DetailsPage/StudyDetails?Study={row['study_synid']}"
            }
            if row["study_synid"]
            else None
        )
        row["jax_strain"] = (
            {"link_url": f"https://jax.org/strain/{row['jax_id']}"}
            if len(row["jax_id"]) > 0
            else None
        )
        row["center"] = (
            {
                "link_text": row["contributing_group"],
                "link_url": get_center_link_url(row["contributing_group"]),
            }
            if row["contributing_group"]
            else None
        )

        # Calculate available_data based on which links are actually present
        row["available_data"] = get_list_of_available_data(row)

        # Keep only the columns that will be in transformed_records in row
        keep_columns = [
            "name",
            "model_type",
            "matched_controls",
            "gene_expression",
            "disease_correlation",
            "pathology",
            "biomarkers",
            "study_data",
            "jax_strain",
            "center",
            "modified_genes",
            "available_data",
        ]

        transformed_records.append(row[keep_columns].to_dict())

    return transformed_records
