"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from agoradatatools.etl.transform.immunohisto_transform import immunohisto_transform
from agoradatatools.etl.utils import check_required_datasets_and_columns


REQUIRED_INPUT = {
    "allele_info": [
        "name",
        "modified_gene",
        "gene_ensembl_id",
        "allele",
        "allele_type",
        "mgi_allele_id",
    ],
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
    ],
    "model_results_info": [
        "name",
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ],
    "human_transgene_allele_map": [
        "mgi_allele_id",
        "gene_symbol",
        "human_ensembl_id",
    ],
    "immunohisto_measure_order": [
        "dataset_name",
        "evidence_type",
    ],
    "biomarkers": [
        "name",
        "evidence_type",
        "value",
        "units",
        "age",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
    "pathology": [
        "name",
        "evidence_type",
        "value",
        "units",
        "age",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
}


def process_genetic_info(
    human_transgene_allele_map_df: pd.DataFrame,
    model_alleles: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Processes the gene information DataFrame. If the allele is a human transgene,
    replace the ensembl_id with the human one. Each model's alleles are processed independently.
    Multiple entries are preserved for different alleles of the same gene.

    Args:
        human_transgene_allele_map_df (pd.DataFrame): The DataFrame containing the human transgene allele information.
        model_alleles (pd.DataFrame): The DataFrame containing the model allele information.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the processed gene information.
    """
    # Copy dataframes to avoid modifying originals
    # Using copy() to avoid warning: A value is trying to be set on a copy of a slice from a DataFrame.
    # Warning appears even if using .loc to set the value.
    model_alleles = model_alleles.copy()
    human_transgene_allele_map_df = human_transgene_allele_map_df.copy()

    # Normalize gene columns to uppercase for consistent merging
    model_alleles["gene_upper"] = model_alleles["modified_gene"].str.upper()
    human_transgene_allele_map_df["gene_upper"] = human_transgene_allele_map_df[
        "gene_symbol"
    ].str.upper()

    # Merge on mgi_allele_id and gene_upper to ensure we preserve different alleles
    merged_df = model_alleles.merge(
        human_transgene_allele_map_df[
            ["mgi_allele_id", "gene_upper", "human_ensembl_id", "gene_symbol"]
        ],
        on=["mgi_allele_id", "gene_upper"],
        how="left",
    )

    # Only override ensembl_id if we have a valid human_ensembl_id
    merged_df["ensembl_gene_id"] = merged_df.apply(
        lambda row: row["human_ensembl_id"]
        if pd.notna(row["human_ensembl_id"])
        else row["gene_ensembl_id"],
        axis=1,
    )

    # Only override gene_symbol if we have a valid human_ensembl_id
    merged_df["modified_gene"] = merged_df.apply(
        lambda row: row["gene_symbol"]
        if pd.notna(row["human_ensembl_id"])
        else row["modified_gene"],
        axis=1,
    )

    # Drop duplicates to ensure we don't have exact duplicates of the same allele
    merged_df = merged_df.drop_duplicates(
        subset=["modified_gene", "allele", "mgi_allele_id"]
    )

    return merged_df[
        ["modified_gene", "ensembl_gene_id", "allele", "allele_type", "mgi_allele_id"]
    ].to_dict(orient="records")


def transform_model_details(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the model_details souce files into a structured format for Model AD.

    Source Files: model_info (syn61378590), allele_info (syn64618791),
    pathology (syn61357279), biomarkers (syn61250724), human_transgene_allele_map (syn64846805)

    Expected Transformations:
        1. Column renames are applied to Pathology and Biomarkers:
            - measure -> evidence_type
            - ageDeath -> age
            - measurement -> value
        2. Sex and tissue values are converted to use Initial Caps (e.g. Female, Cerebral Cortex)
        3. Biomarker measure (pre-transform in source file) aka evidence_type (post-transform
        in output file)values use &beta; entity codes, instead of beta string literals
        4. For the human_transgene_allele_map source file use the human_ensembl_id and
        gene values for rows with a matching mgi_allele_id

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        list[dict[str, Any]]: A list containing dicionaries with the transformed data.

    Raises:
        ValueError: If required datasets are missing or if required columns are missing from any dataset.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Load and prepare datasets
    allele_info_df = datasets["allele_info"].fillna("")
    model_info_df = datasets["model_info"].fillna("")
    human_transgene_allele_map_df = datasets["human_transgene_allele_map"].fillna("")
    model_results_info_df = datasets["model_results_info"].fillna({np.nan: None})

    # Ensure jax_id preserves leading zeros by converting to string with proper formatting
    if "jax_id" in model_info_df.columns:
        model_info_df["jax_id"] = model_info_df["jax_id"].apply(
            lambda x: str(x).zfill(6) if pd.notna(x) and str(x).strip() != "" else x
        )

    # Prepare biomarker and pathology dataframes
    grouped_biomarkers = immunohisto_transform(datasets, dataset_name="biomarkers")
    grouped_pathology = immunohisto_transform(datasets, dataset_name="pathology")

    # Convert matching controls and aliases from comma-delimited strings to lists
    for col_name in ["matched_controls", "aliases"]:
        model_info_df[col_name] = model_info_df[col_name].apply(
            lambda x: [item.strip() for item in str(x).split(",")]
            if pd.notna(x) and x != ""
            else []
        )

    # Process each model
    result = []
    for _, model_row in model_info_df.iterrows():
        model_name = model_row["name"]

        # Get genetic info for this model
        genetic_info = process_genetic_info(
            human_transgene_allele_map_df,
            model_alleles=allele_info_df[allele_info_df["name"] == model_name],
        )

        # Process the biomarkers and pathology datasets for this model
        model_biomarkers = [x for x in grouped_biomarkers if x["name"] == model_name]
        model_pathology = [x for x in grouped_pathology if x["name"] == model_name]

        # Build the complete model entry
        model_entry = {
            "name": model_name,
            "matched_controls": model_row["matched_controls"],
            "model_type": model_row["model_type"],
            "contributing_group": model_row["contributing_group"],
            "study_synid": model_row["study_synid"],
            "rrid": model_row["rrid"],
            "jax_id": model_row["jax_id"],
            "alzforum_id": model_row["alzforum_id"],
            "genotype": model_row["genotype"],
            "aliases": model_row["aliases"],
            "gene_expression": None,
            "disease_correlation": None,
            "spatial_transcriptomics": None,
            "genetic_info": genetic_info,
            "biomarkers": model_biomarkers,
            "pathology": model_pathology,
        }

        # Add gene expression and disease correlation links if they exist
        if model_name in model_results_info_df["name"].values:
            model_results_info_row_dict = model_results_info_df[
                model_results_info_df["name"] == model_name
            ].to_dict("records")[0]

            model_entry["gene_expression"] = (
                f"comparison/expression?model={model_name}"
                if pd.notna(model_results_info_row_dict["gene_expression"])
                and bool(model_results_info_row_dict["gene_expression"])
                else None
            )
            model_entry["disease_correlation"] = (
                f"comparison/correlation?model={model_name}"
                if pd.notna(model_results_info_row_dict["disease_correlation"])
                and bool(model_results_info_row_dict["disease_correlation"])
                else None
            )

        result.append(model_entry)

    return result
