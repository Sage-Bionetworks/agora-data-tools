"""Shared helpers for drug_list and OpenTargets drug metadata transforms."""

import pandas as pd

from agoradatatools.etl.utils import (
    column_value_present,
    strip_whitespace_columns,
    validate_one_to_one_mapping,
)

DISPLAY_CLINICAL_PHASES = {
    "Phase I",
    "Phase II",
    "Phase III",
    "Phase IV",
    "Preclinical",
    "Unknown",
}

MODALITY_VALUES = {"Small molecule", "Protein"}

CHEMBL_ID_REGEX = r"^CHEMBL\d+$"

DRUG_LIST_STRIP_COLUMNS = [
    "common_name",
    "combined_with_common_name",
    "chembl_id",
    "combined_with_chembl_id",
]


def validate_combined_with_column_pairs(drug_list: pd.DataFrame) -> None:
    """Ensure the combined_with name and ChEMBL ID columns are populated together.

    Each row must either name a combination partner with both
    ``combined_with_common_name`` and ``combined_with_chembl_id`` set, or leave
    both empty. A row with exactly one of the two set is a data integrity error.

    Args:
        drug_list: The drug_list DataFrame to validate.

    Raises:
        ValueError: If any row has a value in only one of the two combined_with
            columns.
    """
    present_name = column_value_present(drug_list["combined_with_common_name"])
    present_id = column_value_present(drug_list["combined_with_chembl_id"])
    mismatched = present_name != present_id
    if mismatched.any():
        row_indices = drug_list.index[mismatched].tolist()
        raise ValueError(
            "Data Integrity Error: "
            f"{int(mismatched.sum())} row(s) have a value in only one of "
            "combined_with_common_name and combined_with_chembl_id. "
            f"Affected row index(es): {row_indices}. "
            "Please fix the source data before re-running."
        )


def validate_drug_list_integrity(drug_list: pd.DataFrame) -> pd.DataFrame:
    """Strip and validate a drug_list before aggregation.

    Validation steps:
        1. Strip surrounding whitespace from the drug name and ChEMBL ID columns.
        2. Require the combined_with name/ID columns to be set or empty together.
        3. Enforce a 1:1 mapping between common_name and chembl_id across both the
           primary columns and the combined_with partner columns. The two column
           pairs are stacked into a single frame so a name (or ID) used as a
           primary drug and as a combination partner must agree.

    Args:
        drug_list: The raw drug_list DataFrame.

    Returns:
        A stripped copy of *drug_list*.

    Raises:
        ValueError: If the combined_with columns are unevenly populated, or if a
            common_name maps to multiple chembl_ids (or vice versa).
    """
    drug_list = strip_whitespace_columns(drug_list, DRUG_LIST_STRIP_COLUMNS)
    validate_combined_with_column_pairs(drug_list)

    name_id_pairs = pd.concat(
        [
            drug_list[["chembl_id", "common_name"]],
            drug_list[["combined_with_chembl_id", "combined_with_common_name"]].rename(
                columns={
                    "combined_with_chembl_id": "chembl_id",
                    "combined_with_common_name": "common_name",
                }
            ),
        ]
    )
    validate_one_to_one_mapping(
        name_id_pairs, "chembl_id", "common_name", bidirectional=True
    )
    return drug_list
