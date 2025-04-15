"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""
import pandas as pd
from typing import Any, Dict, List


def prepare_biomarker_pathology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Capitalize 'sex' and 'tissue' columns in the DataFrame.
    Replace 'beta' with '&beta;' in the 'type' column.
    Rename 'type' column to 'evidence_type' and 'measurement' to 'value'.
    """
    # Transform text fields to Initial Caps
    df["sex"] = df["sex"].str.capitalize()
    df["tissue"] = df["tissue"].apply(
        lambda x: " ".join(word.capitalize() for word in x.split())
        if isinstance(x, str)
        else x
    )
    # Replace 'beta' with '&beta;' in biomarker types
    df["type"] = df["type"].str.replace("beta", "&beta;")

    # Rename 'type' column to 'evidence_type' and 'measurement' to 'value'
    df = df.rename(columns={"type": "evidence_type", "measurement": "value"})

    return df


def proccess_biomarker_pathology(
    df: pd.DataFrame, model_name: str
) -> List[Dict[str, Any]]:
    """
    Processes the biomarkers and pathology data for a specific model.
    Group by evidence_type, tissue, and age_death.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        model_name (str): The name of the model to process.
    Returns:
        list[dict]: A list of dictionaries containing the processed data.
    """

    output = []
    model_df = df[df["model"] == model_name]

    # Group biomarkers by evidence_type, tissue, and age_death
    for (evidence_type, tissue, age), group in model_df.groupby(
        ["evidence_type", "tissue", "age_death"]
    ):
        df_entry = {
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
            df_entry["data"].append(data_point)

        output.append(df_entry)
    return output


def process_genetic_info(
    human_transgene_allele_map_df: pd.DataFrame,
    model_alleles: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Processes the gene information DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the gene information.

    Returns:
        list[dict]: A list of dictionaries containing the processed gene information.
    """
    genetic_info = []
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
    return genetic_info


def transform_model_details(datasets: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """
    Transforms the model_details souce files into a structured format for Model AD.

    Source Files: model_info (syn61378590), allele_info (syn64618791),
    pathology (syn61357279), biomarkers (syn61250724), human_transgene_allele_map (syn64846805)

    Expected Changes:
        1. Column reanames are applied to Pathology and Biomarkers:
            - measure -> evidence_type
            - ageDeath -> age
            - measurement -> value
        2. Sex and tissue values are converted to use Initial Caps (e.g. Female, Cerebral Cortex)
        3. Biomarker measure (pre-transform in source file) aka evidence_type (post-transform
        in output file)values use &beta; entity codes, instead of beta string literals
        4. For the human_transgene_allele_map source file use the ensembl_id and
        gene_symbol values for rows with a matching mgi_allele_id

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.

    Returns:
        list[dict[str, Any]]: A list containing dicionaries with the transformed data.
    """
    # Load datasets
    allele_info_df = datasets["allele_info"].fillna("")
    model_info_df = datasets["model_info"].fillna("")
    human_transgene_allele_map_df = datasets["human_transgene_allele_map"].fillna("")

    # Load and prepare the biomarker and pathology dataframes
    biomarkers_df = prepare_biomarker_pathology(datasets["biomarkers"].fillna(""))
    pathology_df = prepare_biomarker_pathology(datasets["pathology"].fillna(""))

    # Convert matching controls and aliases from comma-delimited strings to lists
    for col_name in ["matched_controls", "aliases"]:
        model_info_df[col_name] = model_info_df[col_name].apply(
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
        genetic_info = process_genetic_info(
            human_transgene_allele_map_df,
            model_alleles=allele_info_df[allele_info_df["model"] == model_name],
        )

        # Process the biomarkers and pathology datasets for this model
        model_biomarkers = proccess_biomarker_pathology(biomarkers_df, model_name)
        model_pathology = proccess_biomarker_pathology(pathology_df, model_name)

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
