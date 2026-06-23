"""Transform drug metadata and nominations into the drug_info dataset."""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.transform.transform_utils.drug_transform_utils import (
    CHEMBL_ID_REGEX,
    DISPLAY_CLINICAL_PHASES,
    MODALITY_VALUES,
    validate_drug_list_integrity,
)
from agoradatatools.etl.utils import (
    MatchesRegexRule,
    NotEmptyRule,
    OneOfRule,
    capitalize_first_character,
    check_column_rules,
    check_required_datasets_and_columns,
    nest_fields,
)

REQUIRED_INPUT = {
    "ot_drug_metadata": [
        "chembl_id",
        "description",
        "modality",
        "maximum_clinical_trial_phase",
        "mechanisms_of_action",
        "linked_targets",
        "aliases",
        "drug_bank_id",
        "year_of_first_approval",
    ],
    "drug_list": [
        "chembl_id",
        "common_name",
        "combined_with_common_name",
        "combined_with_chembl_id",
        "iupac_id",
        "contact_pi",
        "grant_number",
        "evidence",
        "data_used",
        "ad_moa",
        "reference",
        "computational_validation_status",
        "computational_validation_results",
        "experimental_validation_status",
        "experimental_validation_results",
        "additional_evidence",
        "contributors",
        "initial_nomination",
        "program",
        "priority_score",
        "priority_score_criteria",
        "published",
    ],
    "gene_metadata": ["ensembl_gene_id", "symbol"],
}

COLUMN_RULES = {
    # Allowed values match syn73724873 OpenTargets export (see nominated_drugs).
    "ot_drug_metadata": {
        "chembl_id": [
            NotEmptyRule(),
            MatchesRegexRule(CHEMBL_ID_REGEX),
        ],
        "modality": [OneOfRule(MODALITY_VALUES)],
        "maximum_clinical_trial_phase": [OneOfRule(DISPLAY_CLINICAL_PHASES)],
    },
    "drug_list": {
        "common_name": [NotEmptyRule()],
        "chembl_id": [
            NotEmptyRule(),
            MatchesRegexRule(CHEMBL_ID_REGEX),
        ],
        # Nomination-critical fields used in drug_nominations output and PI sorting;
        # aligned with transform_nominated_drugs.
        "initial_nomination": [NotEmptyRule()],
        "contact_pi": [NotEmptyRule()],
        "program": [NotEmptyRule()],
    },
}

DRUG_LIST_NEST_DROP_COLUMNS = [
    "priority_score",
    "priority_score_criteria",
    "published",
    "combined_with_common_name",
    "combined_with_chembl_id",
]

OUTPUT_COLUMN_ORDER = [
    "common_name",
    "description",
    "iupac_id",
    "chembl_id",
    "drug_bank_id",
    "aliases",
    "modality",
    "year_of_first_approval",
    "maximum_clinical_trial_phase",
    "linked_targets",
    "mechanisms_of_action",
    "drug_nominations",
]

# Flat nomination text columns capitalized before nesting into drug_nominations.
# common_name and description are already properly capitalized in the source.
CAPITALIZE_FIRST_CHARACTER_FIELDS = [
    "evidence",
    "data_used",
    "ad_moa",
    "additional_evidence",
    "computational_validation_status",
    "computational_validation_results",
    "experimental_validation_status",
    "experimental_validation_results",
]

# Keys that uniquely identify a drug (one row per group). These stay top-level and
# are dropped from each nested nomination dict to avoid duplicating them.
_NOMINATION_GROUPING = ["chembl_id", "common_name", "iupac_id"]


def _pi_lastname_sort_key(nomination: dict[str, Any]) -> str:
    """Return lowercase PI last name for sorting nomination rows."""
    name_part = nomination["contact_pi"].split(",")[0].strip()
    parts = name_part.split()
    return parts[-1].lower() if parts else ""


def _map_ensembl_id_list_to_dicts(
    target_list: list[str] | None, gene_map: dict[str, str]
) -> List[Dict[str, str]]:
    """Map Ensembl IDs in target_list to {ensembl_gene_id, hgnc_symbol} dicts.

    When an Ensembl ID is missing from gene_map, hgnc_symbol falls back to the
    Ensembl ID.
    """
    if not isinstance(target_list, list):
        return []
    return [
        {
            "ensembl_gene_id": g_id,
            "hgnc_symbol": gene_map.get(g_id, g_id),
        }
        for g_id in target_list
        if pd.notnull(g_id)
    ]


def _get_best_iupac_id(group: pd.Series) -> str:
    """Pick the first non-null, non-Unknown iupac_id for a chembl_id group.

    Falls back to the "Unknown" sentinel when the group has none. The sentinel
    survives the iupac_id grouping key in nest_fields (groupby drops null keys)
    and is converted back to None after nesting.
    """
    valid_ids = group.replace({"Unknown": None}).dropna()
    return next(iter(valid_ids), "Unknown")


def _resolve_linked_target_symbols(
    drug_metadata: pd.DataFrame, gene_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Replace Ensembl ID lists with {ensembl_gene_id, hgnc_symbol} dicts."""
    gene_map = gene_metadata.set_index("ensembl_gene_id")["symbol"].to_dict()

    drug_metadata = drug_metadata.copy()
    drug_metadata["linked_targets"] = drug_metadata["linked_targets"].apply(
        lambda target_list: _map_ensembl_id_list_to_dicts(target_list, gene_map)
    )
    return drug_metadata


def _build_single_combined_with_entry(
    common_name: str | None, chembl_id: str | None
) -> List[Dict[str, str]]:
    """Combine a name/ChEMBL ID pair into a dict wrapped as a single-item list.

    validate_drug_list_integrity guarantees the name and ID columns are either both
    populated or both missing; returns an empty list when both are missing.

    The result is returned as a single-item list (rather than a dict or None)
    because the drug_info output contract consumed by the Agora UI expects
    combined_with to be a list of partner dicts.
    """
    if pd.isnull(common_name) or pd.isnull(chembl_id):
        return []
    return [{"common_name": common_name, "chembl_id": chembl_id}]


def _collapse_drug_nominations(drug_list: pd.DataFrame) -> pd.DataFrame:
    """Nest nomination rows into drug_nominations and collapse to one row per chembl_id.

    Expects drug_list already passed through validate_drug_list_integrity. For each
    chembl_id this picks a single best iupac_id, builds a per-row combined_with list,
    capitalizes the flat nomination text columns, nests the remaining nomination
    columns into a drug_nominations list, and sorts those nominations by PI last name.

    Returns one row per chembl_id with a nested drug_nominations column.
    """
    drug_list = drug_list.copy()
    drug_list["iupac_id"] = drug_list.groupby("chembl_id")["iupac_id"].transform(
        _get_best_iupac_id
    )

    drug_list["combined_with"] = drug_list.apply(
        lambda row: _build_single_combined_with_entry(
            row["combined_with_common_name"], row["combined_with_chembl_id"]
        ),
        axis=1,
    )

    # Capitalize the flat nomination text columns before nesting so the values
    # land capitalized inside drug_nominations.
    drug_list = capitalize_first_character(drug_list, CAPITALIZE_FIRST_CHARACTER_FIELDS)

    # common_name is 1:1 with chembl_id (validate_drug_list_integrity) and iupac_id
    # is uniform per chembl_id (_get_best_iupac_id), so this grouping already yields
    # one row per chembl_id. The grouping keys are dropped from the nested dicts to
    # avoid duplicating them inside each nomination.
    drug_list = nest_fields(
        df=drug_list,
        grouping=_NOMINATION_GROUPING,
        new_column="drug_nominations",
        drop_columns=DRUG_LIST_NEST_DROP_COLUMNS + _NOMINATION_GROUPING,
    )

    drug_list["drug_nominations"] = drug_list["drug_nominations"].apply(
        lambda nominations: sorted(nominations, key=_pi_lastname_sort_key)
    )

    # Missing iupac_ids were set to the "Unknown" sentinel so nest_fields' groupby
    # would not drop those rows on a null grouping key; restore them to None for output.
    drug_list["iupac_id"] = drug_list["iupac_id"].replace("Unknown", None)

    return drug_list


def transform_drug_info(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
    """Build drug_info for nominated drugs only, enriched with OpenTargets metadata.

    Output is driven by drug_list: only nominated chembl_id values appear.
    OpenTargets-only drugs are excluded. Nominated drugs without a metadata row keep
    null OT fields. Unmapped linked_targets Ensembl IDs use the Ensembl ID as
    hgnc_symbol.

    Args:
        datasets: ot_drug_metadata, drug_list (with program column), and
            gene_metadata DataFrames.
        required_input: Required datasets and columns (overridable in tests).

    Returns:
        One row per nominated drug with nested drug_nominations and resolved
        linked_targets.

    Raises:
        ValueError: If required datasets or columns are missing, column content rules
            are violated, or drug_list integrity checks fail.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, COLUMN_RULES)
    validate_drug_list_integrity(datasets["drug_list"])

    drug_metadata = _resolve_linked_target_symbols(
        datasets["ot_drug_metadata"], datasets["gene_metadata"]
    )

    collapsed_drug_list = _collapse_drug_nominations(datasets["drug_list"])

    drug_info = pd.merge(
        left=collapsed_drug_list,
        right=drug_metadata,
        on="chembl_id",
        how="left",
        validate="m:1",
    )

    drug_info["year_of_first_approval"] = drug_info["year_of_first_approval"].astype(
        "Int64"
    )

    drug_info = drug_info.reindex(columns=OUTPUT_COLUMN_ORDER)

    return drug_info
