"""Shared helpers for drug_list and OpenTargets drug metadata transforms."""

import pandas as pd

from agoradatatools.etl.utils import validate_one_to_one_mapping

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


def validate_drug_list_integrity(drug_list: pd.DataFrame) -> None:
    """Validate a drug_list before aggregation.

    Validation steps:
        1. Require the combined_with name/ID columns to be set or empty together:
           each row must have both combined_with_common_name and
           combined_with_chembl_id set, or both empty.
        2. Enforce a 1:1 mapping between common_name and chembl_id across both the
           primary columns and the combined_with partner columns. The two column
           pairs are stacked into a single frame so a name (or ID) used as a
           primary drug and as a combination partner must agree.

    Args:
        drug_list: The drug_list DataFrame to validate.

    Raises:
        ValueError: If the combined_with columns are unevenly populated, or if a
            common_name maps to multiple chembl_ids (or vice versa).
    """
    present_name = drug_list["combined_with_common_name"].notna()
    present_id = drug_list["combined_with_chembl_id"].notna()
    mismatched = present_name != present_id
    if mismatched.any():
        row_indices = drug_list.index[mismatched].tolist()
        raise ValueError(
            "Data Integrity Error: "
            f"{int(mismatched.sum())} row(s) have a value in only one of "
            "combined_with_common_name and combined_with_chembl_id. "
            "Values must be either both present or both missing. "
            f"Affected row index(es): {row_indices}. "
            "Please fix the source data before re-running."
        )

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
