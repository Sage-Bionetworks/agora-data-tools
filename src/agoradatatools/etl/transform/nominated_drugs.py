import pandas as pd

# Validation function that fails if a pair of values do not have a 1:1 relationship
def validate_linkages(df, name_col, id_col):
    # Filter out rows where both name and ID are missing
    valid_rows = df.dropna(subset=[name_col, id_col])

    # Count unique IDs per name
    counts = valid_rows.groupby(name_col)[id_col].nunique()

    offending_names = counts[counts > 1].index.tolist()

    if offending_names:
        raise ValueError(
            f"Data Integrity Error: The following {name_col}(s) are associated with "
            f"multiple {id_col} values: {offending_names}. "
            "Please fix the source data before re-running."
        )

def transform_nominated_drugs(datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called nominated_drugs.
    """
    drug_list = datasets["drug_list"]
    drug_metadata = datasets["drug_metadata"]

    # Clean & prepare drug_list data
    # Trim leading and trailing whitespace that could prevent proper grouping
    for col in ["common_name", "combined_with_common_name", "chembl_id", "combined_with_chembl_id"]:
        drug_list[col] = drug_list[col].str.strip()

    # Validate that common_name : chembl_id mappings are 1:1
    validate_linkages(drug_list, "common_name", "chembl_id")
    validate_linkages(drug_list, "combined_with_common_name", "combined_with_chembl_id")

    nominated_drugs = drug_list.groupby(
        ["common_name", "chembl_id", "combined_with_common_name", "combined_with_chembl_id"], dropna=False).agg(
            total_nominations=("common_name", "size"),
            initial_nomination=("initial_nomination", "min"),
            principal_investigators=("contact_pi", lambda x: list(set(x.dropna()))),
            programs=("source", lambda x: list(set(x.dropna())))
    ).reset_index()

    # Give this field a unique name to distinguish it from drug_info's version
    nominated_drugs = nominated_drugs.rename(
        columns={"combined_with_common_name": "combined_with"}
    )

    # Merge in other datasets by chembl_id
    for dataset in [
        drug_metadata
    ]:
        nominated_drugs = pd.merge(
            left=nominated_drugs,
            right=dataset,
            on="chembl_id",
            how="left"
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
            "combined_with",
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