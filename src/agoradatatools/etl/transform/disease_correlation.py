"""
This module contains the transformation logic for the disease correlation dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List, Any
import re

from agoradatatools.etl.utils import check_required_datasets_and_columns

REQUIRED_INPUT = {
    "disease_correlation_results": [
        "Cluster",
        "Module",
        "Mouse Model",
        "Sex",
        "Age",
        "Correlation",
        "Adjusted P-Value",
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


def transform_disease_correlation(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the disease correlation source files into a structured format for Model AD.

    Source Files: disease_correlation_results (syn61378590), model_info (syn61357279),
    allele_info (syn61250724)

    Expected Transformations:
        1. Groups data by Mouse Model, Cluster, Age and Sex
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
                "matched_control": str,
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
    model_info_lookup = (
        datasets["model_info"].fillna("").set_index("model").to_dict(orient="index")
    )
    model_allele_lookup = (
        datasets["model_allele_info"]
        .fillna("")
        .groupby("model")["gene"]
        .apply(list)
        .to_dict()
    )

    # Group by all static fields and nest results by module
    output = []
    group_cols = ["Mouse Model", "Cluster", "Age", "Sex"]
    for (model, cluster, age, sex), group in disease_correlation_df.groupby(group_cols):
        # Get static model info
        model_info = model_info_lookup.get(model, {})
        # If matched_controls is a list, get the first element
        mc = model_info.get("matched_controls", "")
        matched_control = next(iter(mc), "") if isinstance(mc, list) else mc
        # Prepare results for all modules in this group
        results = []
        for _, row in group.iterrows():
            # Strip the ‘color’ suffixes from Module (e.g. IFGyellow -> IFG)
            module = (
                re.match(r"^[A-Z]+", row["Module"]).group(0)
                if re.match(r"^[A-Z]+", row["Module"])
                else row["Module"]
            )
            results.append(
                {
                    "module": module,
                    "correlation": float(row["Correlation"])
                    if row["Correlation"] != ""
                    else None,
                    "adj_p_val": float(row["Adjusted P-Value"])
                    if row["Adjusted P-Value"] != ""
                    else None,
                }
            )
        output.append(
            {
                "model": model,
                "matched_control": matched_control,
                "model_type": model_info.get("model_type", ""),
                "modified_genes": model_allele_lookup.get(model, []),
                "cluster": cluster,
                "age": age,
                "sex": sex,
                "results": results,
            }
        )
    return output
