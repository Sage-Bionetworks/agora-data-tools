from typing import Dict, List

import pandas as pd

from agoradatatools.etl.transform.transform_utils.drug_transform_utils import (
    CHEMBL_ID_REGEX,
    DISPLAY_CLINICAL_PHASES,
    validate_drug_list_integrity,
)
from agoradatatools.etl.utils import (
    MatchesRegexRule,
    NotEmptyRule,
    OneOfRule,
    check_column_rules,
    check_required_datasets_and_columns,
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
        "chembl_id": [NotEmptyRule(), MatchesRegexRule(CHEMBL_ID_REGEX)],
        "initial_nomination": [NotEmptyRule()],
        "contact_pi": [NotEmptyRule()],
    },
    # Allowed values match syn73724873 OpenTargets export; modality and phase are
    # non-null in that file. Null modality/phase in output come from left-merge when
    # a nominated chembl_id has no metadata row.
    "drug_metadata": {
        "chembl_id": [NotEmptyRule()],
        "modality": [OneOfRule({"Small molecule", "Protein"})],
        "maximum_clinical_trial_phase": [OneOfRule(DISPLAY_CLINICAL_PHASES)],
    },
}

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


def _unique_sorted_pis(series: pd.Series) -> list:
    """Return sorted unique non-empty principal investigator names."""
    return sorted({v.strip() for v in series.dropna().astype(str) if v.strip()})


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
        2. Strip whitespace and validate drug_list integrity (paired columns,
           per-column linkages, and cross-field name/ChEMBL bijection).
        3. Validate per-column content rules on drug_list and metadata.
        4. Map numeric OpenTargets phases to display strings on metadata.
        5. Group drug_list by (common_name, chembl_id, combined_with_*) and
           aggregate: row count, min(initial_nomination), sorted unique PIs and
           programs.
        6. Rename combined_with_common_name to combined_with.
        7. Left-merge drug_metadata on chembl_id (drugs without metadata retain
           null modality/phase/approval fields).
        8. Sort rows for deterministic output.

    Args:
        datasets: Dictionary containing "drug_list" and "drug_metadata" DataFrames.
        required_input: Required datasets and columns. Defaults to REQUIRED_INPUT.

    Returns:
        DataFrame with columns: common_name, chembl_id, total_nominations,
        combined_with, initial_nomination, principal_investigators, programs,
        modality, year_of_first_approval, maximum_clinical_trial_phase.

    Raises:
        ValueError: If required datasets or columns are missing, column content
            rules are violated, paired combined_with columns are mismatched,
            per-column or cross-field name/ID linkages are not bijective.
    """
    check_required_datasets_and_columns(datasets, required_input)

    drug_list = validate_drug_list_integrity(datasets["drug_list"])

    datasets_for_rules = {
        **datasets,
        "drug_list": drug_list,
    }
    check_column_rules(datasets_for_rules, COLUMN_RULES)

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
            principal_investigators=("contact_pi", _unique_sorted_pis),
            programs=("source", lambda x: sorted(set(x.dropna()))),
        )
        .reset_index()
    )

    nominated_drugs = nominated_drugs.rename(
        columns={"combined_with_common_name": "combined_with"}
    )

    nominated_drugs = pd.merge(
        left=nominated_drugs,
        right=datasets["drug_metadata"],
        on="chembl_id",
        how="left",
        validate="m:1",
    )

    nominated_drugs["year_of_first_approval"] = nominated_drugs[
        "year_of_first_approval"
    ].astype("Int64")

    nominated_drugs = nominated_drugs.sort_values(
        ["common_name", "chembl_id", "combined_with"],
        na_position="last",
    ).reset_index(drop=True)

    nominated_drugs = nominated_drugs[_OUTPUT_COLUMNS]

    return nominated_drugs
