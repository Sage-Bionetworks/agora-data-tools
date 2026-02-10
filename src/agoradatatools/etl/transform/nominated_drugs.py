import numpy as np
import pandas as pd

from agoradatatools.etl.utils import nest_fields


def transform_nominated_drugs(datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called nominated_drugs.
    """
    drug_list = datasets["drug_list"]

    # Group data by common_name
    nominated_drugs = nest_fields(
        df=drug_list,
        grouping="common_name",
        new_column="drug_object",
        drop_columns=["common_name"],
    )

    # create 'total_nominations' field by counting grouped nominations
    nominated_drugs["total_nominations"] = nominated_drugs.apply(
        lambda row: (
            len(row["drug_object"])
            if isinstance(row["drug_object"], list)
            else np.NaN
        ),
        axis=1,
    )

    # create 'year_first_nominated' field by finding the smallest initial_nomination value
    nominated_drugs["year_first_nominated"] = nominated_drugs.apply(
        lambda row: (
            min((item["initial_nomination"] for item in row["drug_object"] if item.get("initial_nomination")), default=np.NaN)
            if isinstance(row["drug_object"], list) and row["drug_object"]
            else np.NaN
        ),
        axis=1,
    )

    # Create 'principal_investigators' field by collecting unique values
    nominated_drugs["principal_investigators"] = nominated_drugs.apply(
        lambda row: (
            list({item["contact_pi"] for item in row["drug_object"] if item.get("contact_pi")})
            if isinstance(row["drug_object"], list)
            else []
        ),
        axis=1,
    )

    # Create 'programs' field by collecting unique source values
    nominated_drugs["programs"] = nominated_drugs.apply(
        lambda row: (
            list({item["source"] for item in row["drug_object"] if item.get("source")})
            if isinstance(row["drug_object"], list)
            else []
        ),
        axis=1,
    )


    # Keep only the columns we need
    nominated_drugs = nominated_drugs[
        [
            "common_name",
            "total_nominations",
            "year_first_nominated",
            "principal_investigators",
            "programs"
        ]
    ]

    # Make sure there are no N/A common_name values
    nominated_drugs = nominated_drugs.dropna(subset=["common_name"])

    return nominated_drugs
