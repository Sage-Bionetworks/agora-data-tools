"""
This module contains the transformation logic for the disease correlation dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List, Any
import re

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    flatten_list,
    remove_duplicates_keep_order,
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


def input_validation_model_info(df: pd.DataFrame) -> None:
    """
    Validates that each model has consistent matched_controls and model_type values.

    Args:
        df (pd.DataFrame): DataFrame containing model information with columns 'model',
                          'matched_controls', and 'model_type'

    Raises:
        ValueError: If any model has inconsistent matched_controls or model_type values
    """
    # Group by model and check for consistency
    for model, group in df.groupby("model"):
        # Check matched_controls consistency
        unique_matched_controls = group["matched_controls"].unique()
        if len(unique_matched_controls) > 1:
            raise ValueError(
                f"Model {model} has inconsistent matched_controls values: {unique_matched_controls}"
            )

        # Check model_type consistency
        unique_model_types = group["model_type"].unique()
        if len(unique_model_types) > 1:
            raise ValueError(
                f"Model {model} has inconsistent model_type values: {unique_model_types}"
            )


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


def create_result_dict(row: pd.Series) -> Dict[str, Any]:
    """
    Creates a result dictionary for a single module's correlation data.
    
    Args:
        row (pd.Series): A row from the disease correlation DataFrame
        
    Returns:
        Dict[str, Any]: A dictionary containing the module name, correlation, and adjusted p-value
    """
    return {
        "module": extract_module_name(row["module"]),
        "correlation": float(row["correlation"]) if row["correlation"] != "" else None,
        "adj_p_val": float(row["adjusted_p_value"]) if row["adjusted_p_value"] != "" else None,
    }


def process_group(
    group: pd.DataFrame,
    model_info: Dict[str, Any],
    allele_info: Dict[str, Any],
    model: str,
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
        model (str): The mouse model name
        cluster (str): The cluster name
        age (str): The age group
        sex (str): The sex
        
    Returns:
        Dict[str, Any]: A dictionary containing the processed group data
    """
    # If matched_controls is a list, get the first element
    mc = model_info.get("matched_controls", "")
    matched_control = next(iter(mc), "") if isinstance(mc, list) else mc
    
    # Process results for all modules in this group
    results = [create_result_dict(row) for _, row in group.iterrows()]
    
    return {
        "model": model,
        "matched_control": matched_control,
        "model_type": model_info.get("model_type", ""),
        "modified_genes": allele_info.get("gene", ""),
        "cluster": cluster,
        "age": age,
        "sex": sex,
        "results": results,
    }


def transform_disease_correlation(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the disease correlation source files into a structured format for Model AD.

    Source Files: disease_correlation_results (syn61378590), model_info (syn61357279),
    allele_info (syn61250724)

    Expected Transformations:
        1. Groups data by mouse_model, Cluster, Age and Sex
        2. For each group:
            - Gets model info from model_info lookup (matched controls, model type)
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
                "model": str,
                "matched_control": str or list,
                "model_type": str,
                "cluster": str,
                "age": str,
                "sex": str,
                "results": List[Dict] containing module, correlation and adj_p_val
            }

    Raises:
        ValueError: If required datasets are missing or if required columns are missing from any dataset.
    """
    check_required_datasets_and_columns(datasets, required_input)

    # Load datasets and prepare lookups if necessary
    disease_correlation_df = datasets["disease_correlation_results"].fillna("")
    model_info_df = datasets["model_info"].fillna("")

    # Validate model info
    input_validation_model_info(model_info_df)

    # Need to split using ', ' because the 'matched_controls' column contains comma-separated lists stored as strings
    model_info_lookup = create_lookup(
        df=model_info_df.applymap(
            lambda x: x.split(", ") if isinstance(x, str) and ", " in x else x
        ),
        group_by_col="model",
    )

    model_allele_lookup = create_lookup(
        df=datasets["allele_info"].fillna(""), group_by_col="model"
    )

    # Group by all static fields and nest results by module
    output = []
    group_cols = ["mouse_model", "cluster", "age", "sex"]
    for (model, cluster, age, sex), group in disease_correlation_df.groupby(group_cols):
        model_info = model_info_lookup.get(model, {})
        allele_info = model_allele_lookup.get(model, {})
        
        processed_group = process_group(
            group=group,
            model_info=model_info,
            allele_info=allele_info,
            model=model,
            cluster=cluster,
            age=age,
            sex=sex,
        )
        output.append(processed_group)
        
    return output
