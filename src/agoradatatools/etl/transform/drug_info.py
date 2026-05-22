"""Transform drug metadata and nominations into the drug_info dataset."""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.transform.transform_utils.drug_transform_utils import (
    CHEMBL_ID_REGEX,
    DISPLAY_CLINICAL_PHASES,
    build_combined_with_list,
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
        "modality": [OneOfRule({"Small molecule", "Protein"})],
        "maximum_clinical_trial_phase": [OneOfRule(DISPLAY_CLINICAL_PHASES)],
    },
    "drug_list": {
        "common_name": [NotEmptyRule()],
        "chembl_id": [
            NotEmptyRule(),
            MatchesRegexRule(CHEMBL_ID_REGEX),
        ],
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

CAPITALIZE_FIRST_CHARACTER_FIELDS = [
    "common_name",
    "description",
    "drug_nominations.evidence",
    "drug_nominations.data_used",
    "drug_nominations.ad_moa",
    "drug_nominations.additional_evidence",
    "drug_nominations.computational_validation_status",
    "drug_nominations.computational_validation_results",
    "drug_nominations.experimental_validation_status",
    "drug_nominations.experimental_validation_results",
]

_NOMINATION_STRIP_KEYS = frozenset({"chembl_id", "common_name", "iupac_id"})


def _pi_lastname_sort_key(nomination: dict[str, Any]) -> str:
    """Return lowercase PI last name for sorting nomination rows."""
    name = nomination.get("contact_pi")
    if not name or not isinstance(name, str):
        return ""
    name_part = name.split(",")[0].strip()
    parts = name_part.split()
    return parts[-1].lower() if parts else ""


def _sort_by_pi_lastname(
    nominations: list[dict[str, Any]] | object,
) -> list[dict[str, Any]] | object:
    """Sort nomination dicts alphabetically by PI last name."""
    if not isinstance(nominations, list):
        return nominations
    return sorted(nominations, key=_pi_lastname_sort_key)


def _strip_redundant_nomination_keys(
    nominations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return nominations without chembl_id, common_name, and iupac_id keys."""
    return [
        {k: v for k, v in d.items() if k not in _NOMINATION_STRIP_KEYS}
        if isinstance(d, dict)
        else d
        for d in nominations
    ]


def _resolve_target_list(
    target_list: list[str] | None, gene_map: dict[str, str]
) -> List[Dict[str, str]]:
    """Map Ensembl IDs in *target_list* to {ensembl_gene_id, hgnc_symbol} dicts.

    When *g_id* is missing from *gene_map*, ``hgnc_symbol`` is set to the Ensembl ID.
    Pending data-team decision (AG-1795): may switch to null, omit, or fail instead.
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
    """Pick the first non-null, non-Unknown iupac_id for a chembl_id group."""
    valid_ids = group.dropna()
    valid_ids = valid_ids[valid_ids != "Unknown"]
    if not valid_ids.empty:
        return valid_ids.iloc[0]
    return "Unknown"


def _resolve_linked_targets(
    drug_metadata: pd.DataFrame, gene_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Replace Ensembl ID lists with {ensembl_gene_id, hgnc_symbol} dicts."""
    ensembl_ids = drug_metadata["linked_targets"].explode().dropna().unique()
    gene_map = (
        gene_metadata[gene_metadata["ensembl_gene_id"].isin(ensembl_ids)]
        .set_index("ensembl_gene_id")["symbol"]
        .to_dict()
    )

    drug_metadata = drug_metadata.copy()
    drug_metadata["linked_targets"] = drug_metadata["linked_targets"].apply(
        lambda target_list: _resolve_target_list(target_list, gene_map)
    )
    return drug_metadata


def _collapse_drug_nominations(drug_list: pd.DataFrame) -> pd.DataFrame:
    """Nest nomination rows into drug_nominations and collapse to one row per chembl_id.

    Expects *drug_list* already passed through ``validate_drug_list_integrity``.
    """
    drug_list = drug_list.copy()
    drug_list["iupac_id"] = drug_list.groupby("chembl_id")["iupac_id"].transform(
        _get_best_iupac_id
    )
    drug_list["iupac_id"] = drug_list["iupac_id"].fillna("Unknown")

    drug_list["combined_with"] = drug_list.apply(
        lambda row: build_combined_with_list(
            row["combined_with_common_name"], row["combined_with_chembl_id"]
        ),
        axis=1,
    )

    drug_list = nest_fields(
        df=drug_list,
        grouping=["chembl_id", "common_name", "iupac_id"],
        new_column="drug_nominations",
        drop_columns=DRUG_LIST_NEST_DROP_COLUMNS,
    )

    drug_list = drug_list.groupby("chembl_id", as_index=False).agg(
        {
            "common_name": "first",
            "iupac_id": "first",
            "drug_nominations": "sum",
        }
    )

    drug_list["drug_nominations"] = drug_list["drug_nominations"].apply(
        _sort_by_pi_lastname
    )

    drug_list["drug_nominations"] = drug_list["drug_nominations"].apply(
        lambda noms: _strip_redundant_nomination_keys(noms)
        if isinstance(noms, list)
        else noms
    )
    drug_list["iupac_id"] = drug_list["iupac_id"].replace("Unknown", None)

    return drug_list


def transform_drug_info(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
    """Build drug_info by joining OpenTargets metadata with harmonized drug nominations.

    Args:
        datasets: ``ot_drug_metadata``, ``drug_list`` (with ``program`` column), and
            ``gene_metadata`` DataFrames.
        required_input: Required datasets and columns (overridable in tests).

    Returns:
        One row per nominated drug (``chembl_id`` in drug_list) with nested
        ``drug_nominations`` and OpenTargets fields when metadata exists. Nominated
        drugs without an OT metadata row retain null metadata fields.

    Raises:
        ValueError: If required datasets or columns are missing, column content rules
            are violated, or drug_list integrity checks fail.
    """
    check_required_datasets_and_columns(datasets, required_input)

    drug_list = validate_drug_list_integrity(datasets["drug_list"])

    drug_metadata = datasets["ot_drug_metadata"].copy()
    gene_metadata = datasets["gene_metadata"]

    drug_metadata = _resolve_linked_targets(drug_metadata, gene_metadata)

    datasets_for_rules = {
        **datasets,
        "ot_drug_metadata": drug_metadata,
        "drug_list": drug_list,
    }
    # COLUMN_RULES apply to metadata before linked_targets is resolved to dicts.
    check_column_rules(datasets_for_rules, COLUMN_RULES)

    collapsed_drug_list = _collapse_drug_nominations(drug_list)

    drug_info = pd.merge(
        left=collapsed_drug_list,
        right=drug_metadata,
        on="chembl_id",
        how="left",
        validate="m:1",
    )

    if "year_of_first_approval" in drug_info.columns:
        drug_info["year_of_first_approval"] = drug_info[
            "year_of_first_approval"
        ].astype("Int64")

    drug_info = drug_info.reindex(columns=OUTPUT_COLUMN_ORDER)
    drug_info = capitalize_first_character(drug_info, CAPITALIZE_FIRST_CHARACTER_FIELDS)

    return drug_info
