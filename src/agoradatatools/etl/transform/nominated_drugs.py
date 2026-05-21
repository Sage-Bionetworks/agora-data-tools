from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    MatchesRegexRule,
    NotEmptyRule,
    OneOfRule,
    check_column_rules,
    check_required_datasets_and_columns,
    validate_linkages,
    validate_paired_columns,
)

REQUIRED_INPUT = {
    "drug_list": [
        "common_name",
        "chembl_id",
        "combined_with_common_name",
        "combined_with_chembl_id",
        "initial_nomination",
        "contact_pi",
        "source",
    ],
    "drug_metadata": [
        "chembl_id",
        "modality",
        "maximum_clinical_trial_phase",
        "year_of_first_approval",
    ],
}

COLUMN_RULES = {
    "drug_list": {
        "common_name": [NotEmptyRule()],
        "chembl_id": [NotEmptyRule(), MatchesRegexRule(r"^CHEMBL\d+$")],
        "initial_nomination": [NotEmptyRule()],
    },
    # Allowed values match syn73724873 OpenTargets export; modality and phase are
    # non-null in that file. Null modality/phase in output come from left-merge when
    # a nominated chembl_id has no metadata row.
    "drug_metadata": {
        "chembl_id": [NotEmptyRule()],
        "modality": [OneOfRule({"Small molecule", "Protein"})],
        "maximum_clinical_trial_phase": [
            OneOfRule({"Phase II", "Phase III", "Phase IV", "Preclinical", "Unknown"})
        ],
    },
}

_STRIP_COLUMNS = [
    "common_name",
    "combined_with_common_name",
    "chembl_id",
    "combined_with_chembl_id",
]

_OUTPUT_COLUMNS = [
    "common_name",
    "chembl_id",
    "total_nominations",
    "combined_with",
    "initial_nomination",
    "principal_investigators",
    "programs",
    "modality",
    "year_of_first_approval",
    "maximum_clinical_trial_phase",
]


def transform_nominated_drugs(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
    """Build the nominated_drugs dataset for the Agora Nominated Drugs interface.

    Each row in the output represents one nominated drug (or drug combination)
    defined by its ChEMBL ID, optional combined-with partner, and aggregated
    nomination details. The transform collapses multiple nomination records from
    the source list into a single summary row per unique grouping key.

    Inputs:
        drug_list: One row per nomination event. Used to compute nomination counts,
            earliest nomination year, nominating PIs, programs, and combination
            partners (combined_with_common_name / combined_with_chembl_id).
        drug_metadata: OpenTargets-derived metadata keyed by chembl_id (modality,
            clinical trial phase, year of first approval).

    Processing steps:
        1. Validate required datasets and columns.
        2. Strip whitespace from drug name and ChEMBL ID columns in drug_list.
        3. Validate per-column content rules on stripped drug_list and metadata.
        4. Require combined_with_common_name and combined_with_chembl_id to be
           both present or both empty on each row; then enforce bijective mappings
           (unique in both directions) for common_name and chembl_id, and for
           combined_with_common_name and combined_with_chembl_id, among non-null
           pairs.
        5. Group drug_list by (common_name, chembl_id, combined_with_*) and
           aggregate: row count, min(initial_nomination), sorted unique PIs and
           programs.
        6. Rename combined_with_common_name to combined_with.
        7. Left-merge drug_metadata on chembl_id (drugs without metadata retain
           null modality/phase/approval fields).

    Args:
        datasets: Dictionary containing "drug_list" and "drug_metadata" DataFrames.
        required_input: Required datasets and columns. Defaults to REQUIRED_INPUT.

    Returns:
        DataFrame with columns: common_name, chembl_id, total_nominations,
        combined_with, initial_nomination, principal_investigators, programs,
        modality, year_of_first_approval, maximum_clinical_trial_phase.

    Raises:
        ValueError: If required datasets or columns are missing, column content
            rules are violated, paired combined_with columns are mismatched, or
            name/ID linkages are not bijective.
    """
    check_required_datasets_and_columns(datasets, required_input)

    drug_list = datasets["drug_list"].copy()
    drug_metadata = datasets["drug_metadata"]

    for col in _STRIP_COLUMNS:
        present = drug_list[col].notna()
        if present.any():
            drug_list.loc[present, col] = (
                drug_list.loc[present, col].astype(str).str.strip()
            )

    datasets_for_rules = {**datasets, "drug_list": drug_list}
    check_column_rules(datasets_for_rules, COLUMN_RULES)

    validate_paired_columns(
        drug_list, "combined_with_common_name", "combined_with_chembl_id"
    )
    validate_linkages(drug_list, "common_name", "chembl_id")
    validate_linkages(drug_list, "chembl_id", "common_name")
    validate_linkages(drug_list, "combined_with_common_name", "combined_with_chembl_id")
    validate_linkages(drug_list, "combined_with_chembl_id", "combined_with_common_name")

    nominated_drugs = (
        drug_list.groupby(
            [
                "common_name",
                "chembl_id",
                "combined_with_common_name",
                "combined_with_chembl_id",
            ],
            dropna=False,
        )
        .agg(
            total_nominations=("common_name", "size"),
            initial_nomination=("initial_nomination", "min"),
            principal_investigators=(
                "contact_pi",
                lambda x: sorted(set(x.dropna())),
            ),
            programs=("source", lambda x: sorted(set(x.dropna()))),
        )
        .reset_index()
    )

    nominated_drugs = nominated_drugs.rename(
        columns={"combined_with_common_name": "combined_with"}
    )

    nominated_drugs = pd.merge(
        left=nominated_drugs,
        right=drug_metadata,
        on="chembl_id",
        how="left",
        validate="m:1",
    )

    nominated_drugs["year_of_first_approval"] = nominated_drugs[
        "year_of_first_approval"
    ].astype("Int64")

    nominated_drugs = nominated_drugs[_OUTPUT_COLUMNS]

    return nominated_drugs
