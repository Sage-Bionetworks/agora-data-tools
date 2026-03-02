import numpy as np
import pandas as pd

from agoradatatools.etl.utils import nest_fields


def transform_nominated_drugs(datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called nominated_drugs.
    """
    drug_list = datasets["drug_list"]
    drug_metadata = datasets["drug_metadata"]

    # Clean & prepare drug_list data
    nominated_drugs = drug_list.groupby(["common_name", "chembl_id"]).agg(
        total_nominations=("common_name", "size"),
        initial_nomination=("initial_nomination", "min"),
        principal_investigators=("contact_pi", lambda x: list(set(x.dropna()))),
        programs=("source", lambda x: list(set(x.dropna())))
    ).reset_index()


    # Merge in other datasets by chembl_id
    for dataset in [
        drug_metadata
    ]:
        nominated_drugs = pd.merge(
            left=nominated_drugs,
            right=dataset,
            on="chembl_id",
            how="outer", # this may not be the best choice, but I don't want to silently lose any rows
            validate="one_to_one"
        )

    # Convert specific columns to nullable integers
    cols_to_fix = ["year_of_first_approval"]
    for col in cols_to_fix:
        nominated_drugs[col] = nominated_drugs[col].astype("Int64")

     # Keep only the columns we need
    nominated_drugs = nominated_drugs[
        [
            "common_name",
            "chembl_id",
            "total_nominations",
            "initial_nomination",
            "principal_investigators",
            "programs",
            "modality",
            "year_of_first_approval",
            "maximum_clinical_trial_phase"
        ]
    ]

    # Make sure there are no N/A common_name values
    nominated_drugs = nominated_drugs.dropna(subset=["common_name"])

    return nominated_drugs
