"""
This module contains the transformation logic for the model_overview datasets.
This is for the Model AD project.
"""
import pandas as pd
from typing import Any, Dict, List

from agoradatatools.etl.utils import check_required_datasets_and_columns


REQUIRED_INPUT = {
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
    "model_results_info": [
        "model",
        "gene_expression",
        "disease_correlation",
        "pathology",
        "biomarkers",
    ],
}


def transform_model_overview(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the model_overview source files into a structured format for Model AD.
    """
    check_required_datasets_and_columns(datasets, required_input)

    model_info = datasets["model_info"]
    model_results_info = datasets["model_results_info"]

    # Merge the two datasets on the "model" column
    merged_df = pd.merge(model_info, model_results_info, on="model", how="left")

    # Transform the merged dataframe into the target structure
    transformed_records = []

    for _, row in merged_df.iterrows():
        record = {
            "model": row["model"],
            "model_type": row["model_type"] if pd.notna(row["model_type"]) else None,
            "matched_controls": row["matched_controls"]
            if pd.notna(row["matched_controls"])
            else None,
            "gene_expression": {
                "link_url": f"comparison/expression?model={row['model']}"
            }
            if row["gene_expression"] is True
            else None,
            "disease_correlation": {
                "link_url": f"comparison/correlation?model={row['model']}"
            }
            if row["disease_correlation"] is True
            else None,
            "pathology": {"link_url": f"models/{row['model']}/pathology"}
            if row["pathology"] is True
            else None,
            "biomarkers": {"link_url": f"models/{row['model']}/biomarkers"}
            if row["biomarkers"] is True
            else None,
            "study_data": {
                "link_url": f"https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study={row['study_synid']}"
            }
            if pd.notna(row["study_synid"])
            else None,
            "jax_strain": {"link_url": f"https://jax.org/strain/{row['jax_id']}"}
            if pd.notna(row["jax_id"])
            else None,
            "center": {"link_name": row["contributing_group"]}
            if pd.notna(row["contributing_group"])
            else None,
            "modified_genes": row["genetic_info"]["modified_gene"]
            if "genetic_info" in row
            else [],
        }

        transformed_records.append(record)

    return transformed_records
