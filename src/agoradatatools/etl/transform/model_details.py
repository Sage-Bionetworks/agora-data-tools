"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""
import pandas as pd
from typing import Any, Dict


def transform_model_details(
    datasets: Dict[str, pd.DataFrame], dataset_name: str = "model_details"
) -> list[Dict[str, Any]]:
    """
    Transforms the model_details datasets into a structured format.
    See MG-42 for more details on expected structure: https://sagebionetworks.jira.com/browse/MG-42

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.

    Returns:
        list[dict[str, Any]]: A list containing dicionaries with the transformed data.
    """
    # Load all datasets
    biomarkers_df = datasets["biomarkers"]
    human_transgene_allele_map_df = datasets["human_transgene_allele_map"]
    allele_info_df = datasets["allele_info"]
    model_info_df = datasets["model_info"]
    pathology_df = datasets["pathology"]

    # Fill all NaN values with empty strings
    biomarkers_df.fillna("", inplace=True)
    human_transgene_allele_map_df.fillna("", inplace=True)
    allele_info_df.fillna("", inplace=True)
    model_info_df.fillna("", inplace=True)
    pathology_df.fillna("", inplace=True)

    # Transform text fields to Initial Caps
    biomarkers_df["sex"] = biomarkers_df["sex"].str.capitalize()
    biomarkers_df["tissue"] = biomarkers_df["tissue"].apply(
        lambda x: " ".join(word.capitalize() for word in x.split())
        if isinstance(x, str)
        else x
    )

    pathology_df["sex"] = pathology_df["sex"].str.capitalize()
    pathology_df["tissue"] = pathology_df["tissue"].apply(
        lambda x: " ".join(word.capitalize() for word in x.split())
        if isinstance(x, str)
        else x
    )

    # Replace 'beta' with '&beta;' in biomarker types
    biomarkers_df["type"] = biomarkers_df["type"].str.replace("beta", "&beta;")
    pathology_df["type"] = pathology_df["type"].str.replace("beta", "&beta;")

    # Rename 'type' column to 'evidence_type' and 'measurement' to 'value'
    biomarkers_df = biomarkers_df.rename(
        columns={"type": "evidence_type", "measurement": "value"}
    )
    pathology_df = pathology_df.rename(
        columns={"type": "evidence_type", "measurement": "value"}
    )

    # Convert matching controls and aliases from comma-delimited strings to lists
    model_info_df["matched_controls"] = model_info_df["matched_controls"].apply(
        lambda x: [item.strip() for item in str(x).split(",")]
        if (pd.notna(x) and x != "")
        else []
    )

    model_info_df["aliases"] = model_info_df["aliases"].apply(
        lambda x: [item.strip() for item in str(x).split(",")]
        if (pd.notna(x) and x != "")
        else []
    )

    # Build the final data structure
    result = []

    # Process each model
    for _, model_row in model_info_df.iterrows():
        model_name = model_row["model"]

        # Get genetic info for this model
        genetic_info = []
        model_alleles = allele_info_df[allele_info_df["model"] == model_name]

        for _, allele_row in model_alleles.iterrows():
            gene_info = {
                "modified_gene": allele_row["gene"],
                "ensembl_id": allele_row["gene_ensembl_id"],
                "allele": allele_row["allele"],
                "allele_type": allele_row["allele_type"],
                "mgi_allele_id": allele_row["mgi_allele_id"],
            }

            # If it's a human transgene, replace the ensembl_id with the human one
            if (
                allele_row["mgi_allele_id"]
                in human_transgene_allele_map_df["mgi_allele_id"].values
            ):
                matching_row = human_transgene_allele_map_df[
                    human_transgene_allele_map_df["mgi_allele_id"]
                    == allele_row["mgi_allele_id"]
                ]
                if (
                    len(matching_row) > 0
                    and matching_row.iloc[0]["gene_symbol"] == allele_row["gene"]
                ):
                    gene_info["ensembl_id"] = matching_row.iloc[0]["ensembl_id"]

            genetic_info.append(gene_info)

        # Process biomarkers data
        model_biomarkers = []
        model_biomarkers_df = biomarkers_df[biomarkers_df["model"] == model_name]

        # Group biomarkers by evidence_type, tissue, and age_death
        for (evidence_type, tissue, age), group in model_biomarkers_df.groupby(
            ["evidence_type", "tissue", "age_death"]
        ):
            biomarker_entry = {
                "model": model_name,
                "evidence_type": evidence_type,
                "tissue": tissue,
                "age": f"{age} months",
                "units": group["units"].iloc[0],
                "data": [],
            }

            # Add individual data points
            for _, row in group.iterrows():
                data_point = {
                    "genotype": row["genotype"],
                    "sex": row["sex"],
                    "individual_id": row["individual_id"],
                    "value": row["value"],
                }
                biomarker_entry["data"].append(data_point)

            model_biomarkers.append(biomarker_entry)

        # Process pathology data
        model_pathology = []
        model_pathology_df = pathology_df[pathology_df["model"] == model_name]

        # Group pathology by evidence_type, tissue, and age_death
        for (evidence_type, tissue, age), group in model_pathology_df.groupby(
            ["evidence_type", "tissue", "age_death"]
        ):
            pathology_entry = {
                "model": model_name,
                "evidence_type": evidence_type,
                "tissue": tissue,
                "age": f"{age} months",
                "units": group["units"].iloc[0],
                "data": [],
            }

            # Add individual data points
            for _, row in group.iterrows():
                data_point = {
                    "genotype": row["genotype"],
                    "sex": row["sex"],
                    "individual_id": row["individual_id"],
                    "value": row["value"],
                }
                pathology_entry["data"].append(data_point)

            model_pathology.append(pathology_entry)

        # Build the complete model entry
        model_entry = {
            "model": model_name,
            "matched_controls": model_row["matched_controls"],
            "model_type": model_row["model_type"],
            "contributing_group": model_row["contributing_group"],
            "study_synid": model_row["study_synid"],
            "rrid": model_row["rrid"],
            "jax_id": model_row["jax_id"],
            "alzforum_id": model_row["alzforum_id"],
            "genotype": model_row["genotype"],
            "aliases": model_row["aliases"],
            "genetic_info": genetic_info,
            "biomarkers": model_biomarkers,
            "pathology": model_pathology,
        }

        result.append(model_entry)

    return result
