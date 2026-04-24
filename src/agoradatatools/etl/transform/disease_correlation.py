"""
This module contains the transformation logic for the disease correlation dataset.
This is for the Model AD project.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
import re

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    delim_string_to_list,
    flatten_list,
    remove_duplicates_keep_order,
    extract_age_numeric,
)


REQUIRED_INPUT = {
    "disease_correlation_results": [
        "cluster",
        "module",
        "mouse_model",
        "sex",
        "age",
        "correlation",
        "adjusted_p_value",
    ],
    "model_info": [
        "model",
        "matched_controls",
        "model_type",
    ],
    "allele_info": [
        "model",
        "gene",
        "mgi_allele_id",
    ],
    "human_transgene_allele_map": [
        "mgi_allele_id",
        "gene_symbol",
        "human_ensembl_id",
    ],
}


def create_lookup(df: pd.DataFrame, group_by_col: str) -> Dict[str, Dict[str, Any]]:
    """
    Creates a nested dictionary lookup from a pandas DataFrame, grouping by a specified column.

    For each unique value in the specified group-by column, constructs a dictionary of the
    remaining columns as keys and their corresponding values. If multiple rows share the same
    group-by value but have differing values for the same column, the conflicting values are
    merged into a list of unique values.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data.
        group_by_col (str): The column name to group the data by.

    Returns:
        Dict[str, Dict[str, Any]]: A dictionary where each key is a unique value from the
        group-by column, and the value is another dictionary of column-value pairs.
    """

    lookup = {}
    for _, row in df.iterrows():
        index = row[group_by_col]
        if index not in lookup:
            lookup[index] = {col: row[col] for col in df.columns if col != group_by_col}
        else:
            for k, v in lookup[index].items():
                if row[k] != v:
                    lookup[index][k] = remove_duplicates_keep_order(
                        flatten_list([lookup[index][k], row[k]])
                    )
    return lookup


def extract_module_name(module: str) -> str:
    """
    Extracts the base module name by removing color suffixes.

    Args:
        module (str): The module name that may contain color suffixes (e.g. 'IFGyellow')

    Returns:
        str: The base module name (e.g. 'IFG')
    """
    match = re.match(r"^[A-Z]+", module)
    return match.group(0) if match else module


def map_genes_to_human_symbols(
    allele_info_df: pd.DataFrame,
    human_transgene_allele_map_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Maps mouse gene names to human gene symbols using the human transgene allele map.

    This function normalizes gene names to uppercase for matching, then merges with the
    human transgene map to replace mouse gene names with their human equivalents where
    a mapping exists. For genes without a mapping, the original name is preserved.

    Args:
        allele_info_df (pd.DataFrame): DataFrame containing allele information with columns:
            model, gene, and mgi_allele_id
        human_transgene_allele_map_df (pd.DataFrame): DataFrame containing the mapping with columns:
            mgi_allele_id, gene_symbol (human), human_ensembl_id

    Returns:
        pd.DataFrame: A copy of allele_info_df with gene names mapped to human symbols where applicable
    """
    # Copy dataframes to avoid modifying originals
    allele_info_df = allele_info_df.copy()
    human_transgene_allele_map_df = human_transgene_allele_map_df.copy()

    # Normalize gene columns to uppercase for consistent merging
    allele_info_df["gene_upper"] = allele_info_df["gene"].str.upper()
    human_transgene_allele_map_df["gene_upper"] = human_transgene_allele_map_df[
        "gene_symbol"
    ].str.upper()

    # Merge on both mgi_allele_id and gene_upper for precise matching
    merged_df = allele_info_df.merge(
        human_transgene_allele_map_df[["mgi_allele_id", "gene_upper", "gene_symbol"]],
        on=["mgi_allele_id", "gene_upper"],
        how="left",
    )

    # Replace gene name with human symbol where mapping exists
    merged_df["gene"] = merged_df["gene_symbol"].fillna(merged_df["gene"])

    # Drop the temporary columns and gene_symbol (already merged into gene)
    merged_df = merged_df.drop(columns=["gene_upper", "gene_symbol"])

    return merged_df


def process_group(
    group: pd.DataFrame,
    model_info: Dict[str, Any],
    allele_info: Dict[str, Any],
    name: str,
    cluster: str,
    age: str,
    sex: str,
) -> Dict[str, Any]:
    """
    Processes a group of disease correlation results for a specific model, cluster, age, and sex combination.

    Args:
        group (pd.DataFrame): The group of rows to process
        model_info (Dict[str, Any]): Information about the model
        allele_info (Dict[str, Any]): Information about the alleles
        name (str): The mouse model name
        cluster (str): The cluster name
        age (str): The age group
        sex (str): The sex

    Returns:
        Dict[str, Any]: A dictionary containing the processed group data
    """
    # Get the first list element of matched_controls, default to empty string if not present
    mc = model_info.get("matched_controls", [])
    matched_control = next(iter(mc), "")

    # Ensure modified_genes is always a list
    raw_modified_genes = allele_info.get("gene", [])
    if not isinstance(raw_modified_genes, list):
        modified_genes = [raw_modified_genes]
    else:
        modified_genes = raw_modified_genes

    output = {
        "name": name,
        "matched_control": matched_control,
        "model_type": model_info.get("model_type", ""),
        "modified_genes": modified_genes,
        "cluster": cluster,
        "age": age,
        "age_numeric": extract_age_numeric(age),
        "sex": sex,
    }

    for _, row in group.iterrows():
        module_name = extract_module_name(row["module"])
        if module_name in output:
            raise ValueError(
                f"Module {module_name} already exists for {output['name']}"
            )

        # Only add the module if it has valid data (not all None values). Using "is not None" instead of "if x" so that
        # 0 values are preserved and pass this check
        if row["correlation"] is not None or row["adjusted_p_value"] is not None:
            output[module_name] = {
                "correlation": row["correlation"],
                "adj_p_val": row["adjusted_p_value"],
            }

    return output


def transform_disease_correlation(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the disease correlation source files into a structured format for Model AD.

    Source Files: disease_correlation_results (syn65467849), model_info (syn61378590),
    allele_info (syn64618791), human_transgene_allele_map (syn64846805)

    Expected Transformations:
        1. Groups data by mouse_model, Cluster, Age and Sex
        2. For each group:
            - Gets model info from model_info lookup (matched controls, model type)
            - Maps mouse gene names to human gene symbols using human_transgene_allele_map
            - Strips color suffixes from Module names (e.g. IFGyellow -> IFG)
            - Nests correlation results by module
        3. Converts correlation and p-value strings to floats where possible

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]], optional): Dictionary specifying required columns
            for each input dataset. Defaults to REQUIRED_INPUT.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the transformed data with the
            following structure:
            {
                "name": str,
                "matched_control": str or list,
                "model_type": str,
                "cluster": str,
                "age": str,
                "age_numeric": int,
                "sex": str,
                "<module name>": Dict[str, float] correlation and adj_p_val
            }

    Raises:
        ValueError: If required datasets are missing or if required columns are missing from any dataset.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Load datasets and prepare lookups if necessary
    disease_correlation_df = datasets["disease_correlation_results"].replace(
        np.nan, None
    )
    model_info_df = datasets["model_info"]
    allele_info_df = datasets["allele_info"]
    human_transgene_allele_map_df = datasets["human_transgene_allele_map"]

    # Map mouse gene names to human gene symbols
    allele_info_mapped = map_genes_to_human_symbols(
        allele_info_df, human_transgene_allele_map_df
    )

    # Need to convert 'matched_controls' from comma-separated strings to lists
    model_info_df["matched_controls"] = model_info_df["matched_controls"].apply(
        lambda x: delim_string_to_list(x, delim=",")
    )
    model_info_lookup = create_lookup(model_info_df, group_by_col="model")

    model_allele_lookup = create_lookup(df=allele_info_mapped, group_by_col="model")

    # Group by all static fields
    output = []
    group_cols = ["mouse_model", "cluster", "age", "sex"]

    # Drop any rows with missing values in the grouping columns or the module column
    disease_correlation_df = disease_correlation_df.dropna(
        subset=group_cols + ["module"]
    )

    for (name, cluster, age, sex), group in disease_correlation_df.groupby(
        group_cols, sort=False
    ):
        model_info = model_info_lookup.get(name, {})
        allele_info = model_allele_lookup.get(name, {})

        processed_group = process_group(
            group=group,
            model_info=model_info,
            allele_info=allele_info,
            name=name,
            cluster=cluster,
            age=age,
            sex=sex,
        )
        output.append(processed_group)

    return output
