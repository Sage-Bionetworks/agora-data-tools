"""
This module contains the transformation logic for the model_details datasets.
This is for the Model AD project.
"""
import pandas as pd
from typing import Any, Dict, List

from agoradatatools.etl.utils import check_required_datasets_and_columns


REQUIRED_INPUT = required_input = {
    "allele_info": [
        "model",
        "modified_gene",
        "gene_ensembl_id",
        "allele",
        "allele_type",
        "mgi_allele_id",
    ],
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
    "human_transgene_allele_map": [
        "mgi_allele_id",
        "gene_symbol",
        "human_ensembl_id",
    ],
    "biomarkers": [
        "model",
        "type",
        "measurement",
        "units",
        "age_death",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
    "pathology": [
        "model",
        "type",
        "measurement",
        "units",
        "age_death",
        "tissue",
        "sex",
        "genotype",
        "individual_id",
    ],
}


def prepare_biomarker_pathology(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function prepares the biomarker and pathology dataframes for the Model AD project.
    It performs the following transformations:
    1. Fill missing values with an empty string.
    2. Capitalize 'sex' and 'tissue' columns in the DataFrame.
    3. Replace 'beta' with '&beta;' in the 'type' column.
    4. Rename 'type' column to 'evidence_type' and 'measurement' to 'value'.
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Fill missing values and transform text fields
    df = df.fillna("")
    df["sex"] = df["sex"].str.title()
    df["tissue"] = df["tissue"].str.title()

    # Replace 'beta' with '&beta;' in biomarker types
    df["type"] = df["type"].str.replace("beta", "&beta;")

    # Rename columns
    return df.rename(columns={"type": "evidence_type", "measurement": "value"})


def process_biomarker_pathology(
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
    # Filter for the specific model
    model_df = df[df["model"] == model_name]

    # Group by the required columns and aggregate the data
    grouped = model_df.groupby(["evidence_type", "tissue", "age_death", "units"])

    # Process each group into the required format
    output = []
    for (evidence_type, tissue, age, units), group in grouped:
        df_entry = {
            "model": model_name,
            "evidence_type": evidence_type,
            "tissue": tissue,
            "age": f"{age} months",
            "units": units,
            "data": group[["genotype", "sex", "individual_id", "value"]].to_dict(
                orient="records"
            ),
        }
        output.append(df_entry)

    return output


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

    # Ensure jax_id preserves leading zeros by converting to string with proper formatting
    if "jax_id" in model_info_df.columns:
        model_info_df["jax_id"] = model_info_df["jax_id"].apply(
            lambda x: str(x).zfill(6) if pd.notna(x) and str(x).strip() != "" else x
        )

    # Prepare biomarker and pathology dataframes
    biomarkers_df = prepare_biomarker_pathology(datasets["biomarkers"])
    pathology_df = prepare_biomarker_pathology(datasets["pathology"])

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
        model_name = model_row["model"]

        # Get genetic info for this model
        genetic_info = process_genetic_info(
            human_transgene_allele_map_df,
            model_alleles=allele_info_df[allele_info_df["model"] == model_name],
        )

        # Process the biomarkers and pathology datasets for this model
        model_biomarkers = process_biomarker_pathology(biomarkers_df, model_name)
        model_pathology = process_biomarker_pathology(pathology_df, model_name)

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
