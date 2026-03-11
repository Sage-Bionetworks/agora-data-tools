"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.transform.immunohisto_transform import immunohisto_transform
from agoradatatools.etl.utils import check_required_datasets_and_columns

from agoradatatools.etl.transform.model_ad_transform_utils import (
    build_gene_expression_url,
    process_genetic_info,
    preprocess_model_info,
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
    model_info_df = preprocess_model_info(
        datasets["model_info"], datasets["model_results_info"], model_name_col="name"
    )

    allele_info_df = process_genetic_info(
        datasets["human_transgene_allele_map"], datasets["allele_info"]
    )

    # Prepare biomarker and pathology dataframes
    grouped_biomarkers = immunohisto_transform(datasets, dataset_name="biomarkers")
    grouped_pathology = immunohisto_transform(datasets, dataset_name="pathology")

    # Process each model
    result = []
    for _, model_row in model_info_df.iterrows():
        model_name = model_row["name"]

        # Get genetic info for this model
        model_alleles = allele_info_df[allele_info_df["name"] == model_name]
        genetic_info = model_alleles[
            [
                "modified_gene",
                "ensembl_gene_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
            ]
        ].to_dict(orient="records")

        # Process the biomarkers and pathology datasets for this model
        model_biomarkers = [x for x in grouped_biomarkers if x["name"] == model_name]
        model_pathology = [x for x in grouped_pathology if x["name"] == model_name]

        # Build the complete model entry. This starts with all data in model_row except the url_<x>_value columns and
        # unprocessed biomarkers, and pathology values, and then the gene_expression, and disease_correlation values are
        # changed to URLs. Processed biomarkers and pathology data is added at the end.
        base_dict = model_row.drop(
            ["url_categories_value", "url_models_value", "biomarkers", "pathology"]
        ).to_dict()

        model_entry = {
            **base_dict,
            "gene_expression": build_gene_expression_url(model_row),
            "disease_correlation": (
                f"comparison/correlation?models={model_name}"
                if model_row["disease_correlation"]
                else None
            ),
            "spatial_transcriptomics": None,
            "genetic_info": genetic_info,
            "biomarkers": model_biomarkers,
            "pathology": model_pathology,
        }

        result.append(model_entry)

    return result
