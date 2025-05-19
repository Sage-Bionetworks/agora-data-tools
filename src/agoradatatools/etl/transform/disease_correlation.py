"""
This module contains the transformation logic for the disease correlation dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List, Any
import re

from agoradatatools.etl.utils import check_required_datasets_and_columns, create_lookup


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
        # If matched_controls is a list, get the first element
        mc = model_info.get("matched_controls", "")
        matched_control = next(iter(mc), "") if isinstance(mc, list) else mc
        # Prepare results for all modules in this group
        results = []
        for _, row in group.iterrows():
            # Strip the 'color' suffixes from Module (e.g. IFGyellow -> IFG)
            module = (
                re.match(r"^[A-Z]+", row["module"]).group(0)
                if re.match(r"^[A-Z]+", row["module"])
                else row["module"]
            )
            results.append(
                {
                    "module": module,
                    "correlation": float(row["correlation"])
                    if row["correlation"] != ""
                    else None,
                    "adj_p_val": float(row["adjusted_p_value"])
                    if row["adjusted_p_value"] != ""
                    else None,
                }
            )
        output.append(
            {
                "model": model,
                "matched_control": matched_control,
                "model_type": model_info.get("model_type", ""),
                "modified_genes": allele_info.get("gene", ""),
                "cluster": cluster,
                "age": age,
                "sex": sex,
                "results": results,
            }
        )
    return output
