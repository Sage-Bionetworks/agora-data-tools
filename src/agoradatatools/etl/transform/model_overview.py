"""
This module contains the transformation logic for the model_overview datasets.
This is for the Model AD project.
"""

import pandas as pd
from typing import Any

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    normalize_null_values,
    delim_string_to_list,
)
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    build_transcriptomics_url,
    process_genetic_modifications,
    zero_pad_jax_ids,
)

REQUIRED_INPUT = {
    "model_metadata": [
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
        "transcriptomics",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ],
    "model_genetic_modifications": [
        "name",
        "modified_gene",
        "mouse_ensembl_id",
        "allele",
        "allele_type",
        "mgi_allele_id",
        "human_gene_symbol",
        "human_ensembl_id",
    ],
}


def get_list_of_available_data(row: pd.Series) -> list[str]:
    """
    Get a list of available data for a given model.
    This is used to populate the "Available Data" section of the model overview page.
    If the value is not None, it is considered available.

    Args:
        row (pd.Series): A pandas Series containing the model information.

    Returns:
        list[str]: A list of available data for the model.
    """
    fields = {
        "transcriptomics": "Transcriptomics",
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
    datasets: dict[str, pd.DataFrame],
    required_input: dict[str, list[str]] = REQUIRED_INPUT,
) -> list[dict[str, Any]]:
    """
    Transforms the model_overview source files into a structured format for Model AD.

    This function merges and processes the following input datasets:
        - model_metadata: Contains metadata about each model, and the available results (e.g., gene
          expression, pathology).
        - model_genetic_modifications: Contains allele and genetic modification details for each model,
            including human Ensembl gene IDs for human transgene alleles

    The transformation includes:
        1. For each model, extracting genetic information using process_genetic_modifications, which maps alleles to human
           Ensembl IDs where possible.
        2. Building a structured dictionary for each model, including:
            - model metadata (name, model_type, matched_controls, etc.)
            - links to available results (gene_expression, disease_correlation, pathology, biomarkers)

    Args:
        datasets (dict[str, pd.DataFrame]): Dictionary mapping dataset names to their DataFrames.
        required_input (dict[str, list[str]], optional): Dictionary specifying required datasets and columns. Defaults
        to REQUIRED_INPUT.

    Returns:
        list[dict[str, Any]]: A list of dictionaries, each representing a transformed model overview record.

    Raises:
        ValueError: If required datasets or columns are missing.
    """

    check_required_datasets_and_columns(datasets, required_input)

    model_metadata = datasets["model_metadata"]
    model_genetic_modifications_df = process_genetic_modifications(
        datasets["model_genetic_modifications"]
    )

    modified_genes = model_genetic_modifications_df.groupby("name")[
        "modified_gene"
    ].apply(lambda x: x.drop_duplicates().dropna().to_list())

    # Merge the modified_genes into model_metadata
    model_metadata = model_metadata.merge(
        modified_genes.rename("modified_genes"), how="left", on="name", validate="1:1"
    )

    # Fix some fields in model_metadata
    boolean_columns = [
        "transcriptomics",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ]
    model_metadata = normalize_null_values(
        model_metadata,
        boolean_columns=boolean_columns,
        empty_list_columns=["modified_genes"],
    )
    model_metadata["jax_id"] = zero_pad_jax_ids(model_metadata["jax_id"])

    # Transform the merged dataframe into the target structure
    transformed_records = []

    for _, row in model_metadata.iterrows():
        # Build the links
        row["transcriptomics"] = (
            {"link_url": build_transcriptomics_url(row)}
            if row["transcriptomics"]
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
            if row["jax_id"]
            else None
        )
        row["center"] = row["contributing_group"]

        # Calculate available_data based on which links are actually present
        row["available_data"] = get_list_of_available_data(row)

        # Convert matched_controls from comma-delimited strings to lists
        row["matched_controls"] = delim_string_to_list(
            row["matched_controls"], delim=","
        )

        # Keep only the columns that will be in transformed_records in row
        keep_columns = [
            "name",
            "model_type",
            "matched_controls",
            "transcriptomics",
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
