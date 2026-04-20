"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.transform.immunohisto_transform import immunohisto_transform
from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    normalize_null_values,
)
from agoradatatools.etl.transform.model_ad_transform_utils import (
    build_transcriptomics_url,
    process_genetic_info,
    zero_pad_jax_ids,
)


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
        "url_categories_value",
        "url_models_value",
    ],
    "model_results_info": [
        "name",
        "transcriptomics",
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
    allele_info_df = normalize_null_values(
        datasets["allele_info"],
        empty_string_columns=["gene_ensembl_id", "modified_gene", "allele"],
    )
    human_transgene_allele_map_df = datasets["human_transgene_allele_map"]

    # Merge model_results_df into model_info to get which types of data are available for each model
    model_info_df = pd.merge(
        datasets["model_info"],
        datasets["model_results_info"],
        how="left",
        on="name",
        validate="one_to_one",
    )

    model_info_df = normalize_null_values(
        model_info_df,
        boolean_columns=["transcriptomics", "disease_correlation"],
        empty_string_columns=["rrid", "alzforum_id"],
    )

    # Ensure jax_id preserves leading zeros by converting to string with proper formatting
    model_info_df["jax_id"] = zero_pad_jax_ids(model_info_df["jax_id"])

    # Prepare biomarker and pathology dataframes
    grouped_biomarkers = immunohisto_transform(datasets, dataset_name="biomarkers")
    grouped_pathology = immunohisto_transform(datasets, dataset_name="pathology")

    # Convert matching controls and aliases from comma-delimited strings to lists
    for col_name in ["matched_controls", "aliases"]:
        model_info_df[col_name] = model_info_df[col_name].apply(
            lambda x: (
                [item.strip() for item in str(x).split(",")]
                if pd.notna(x) and x != ""
                else []
            )
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
            "transcriptomics": None,
            "disease_correlation": None,
            "spatial_transcriptomics": None,
            "genetic_info": genetic_info,
            "biomarkers": model_biomarkers,
            "pathology": model_pathology,
        }

        # Add transcriptomics and disease correlation links if they exist
        model_entry["transcriptomics"] = build_transcriptomics_url(model_row)
        model_entry["disease_correlation"] = (
            f"comparison/correlation?models={model_name}"
            if bool(model_row["disease_correlation"])
            else None
        )

        result.append(model_entry)

    return result
