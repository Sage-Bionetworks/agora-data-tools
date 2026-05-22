"""Shared helpers for drug_list and OpenTargets drug metadata transforms."""

from typing import Dict, List, Set, Tuple

import pandas as pd

from agoradatatools.etl.utils import validate_linkages, validate_paired_columns

DrugScalar = str | int | float | None

CLINICAL_PHASE_MAP = {
    1: "Phase I",
    2: "Phase II",
    3: "Phase III",
    4: "Phase IV",
    -1: "Unknown",
}

DISPLAY_CLINICAL_PHASES = {
    "Phase I",
    "Phase II",
    "Phase III",
    "Phase IV",
    "Preclinical",
    "Unknown",
}

CHEMBL_ID_REGEX = r"^CHEMBL\d+$"

DRUG_LIST_STRIP_COLUMNS = [
    "common_name",
    "combined_with_common_name",
    "chembl_id",
    "combined_with_chembl_id",
]


def strip_drug_list_columns(
    df: pd.DataFrame, columns: List[str] = DRUG_LIST_STRIP_COLUMNS
) -> pd.DataFrame:
    """Strip whitespace from drug name and ChEMBL ID columns in a copy of *df*."""
    result = df.copy()
    for col in columns:
        if col not in result.columns:
            continue
        present = result[col].notna()
        if present.any():
            result.loc[present, col] = result.loc[present, col].astype(str).str.strip()
    return result


def prepare_drug_list(drug_list: pd.DataFrame) -> pd.DataFrame:
    """Strip drug_list and validate paired columns and name/ID linkages.

    Used by ``nominated_drugs`` and as the first step of ``validate_drug_list_integrity``.

    Returns:
        Stripped copy of *drug_list*.
    """
    drug_list = strip_drug_list_columns(drug_list)
    validate_paired_columns(
        drug_list, "combined_with_common_name", "combined_with_chembl_id"
    )
    validate_linkages(drug_list, "common_name", "chembl_id")
    validate_linkages(drug_list, "chembl_id", "common_name")
    validate_linkages(drug_list, "combined_with_common_name", "combined_with_chembl_id")
    validate_linkages(drug_list, "combined_with_chembl_id", "combined_with_common_name")
    return drug_list


def build_combined_with_list(
    name_val: DrugScalar, id_val: DrugScalar
) -> List[Dict[str, str]]:
    """Convert comma-delimited combined_with fields into partner drug dicts."""
    name_is_null = pd.isnull(name_val) or str(name_val).strip() == ""
    id_is_null = pd.isnull(id_val) or str(id_val).strip() == ""

    if name_is_null and id_is_null:
        return []

    names = [n.strip() for n in str(name_val).split(",")]
    ids = [i.strip() for i in str(id_val).split(",")]

    if len(names) != len(ids):
        raise ValueError(
            f"Mismatched combined_with lists: {len(names)} name(s) but {len(ids)} ID(s) "
            f"(names: {names}, ids: {ids})"
        )

    return [{"common_name": n, "chembl_id": i} for n, i in zip(names, ids)]


def validate_drug_name_chembl_mappings(drug_list: pd.DataFrame) -> None:
    """Ensure chembl_id and common_name are 1:1 across primary and combined_with partners."""
    all_pairs: Set[Tuple[str, str]] = set(
        zip(drug_list["chembl_id"].astype(str), drug_list["common_name"].astype(str))
    )
    for _, row in drug_list.iterrows():
        for entry in build_combined_with_list(
            row["combined_with_common_name"], row["combined_with_chembl_id"]
        ):
            all_pairs.add((entry["chembl_id"], entry["common_name"]))

    id_to_names: Dict[str, Set[str]] = {}
    name_to_ids: Dict[str, Set[str]] = {}
    for chembl_id, common_name in all_pairs:
        id_to_names.setdefault(chembl_id, set()).add(common_name)
        name_to_ids.setdefault(common_name, set()).add(chembl_id)

    offending_ids = [k for k, v in id_to_names.items() if len(v) > 1]
    if offending_ids:
        raise ValueError(
            "Data Integrity Error: The following chembl_id(s) map to multiple "
            f"common_names across primary and combined_with fields: {offending_ids}"
        )

    offending_names = [k for k, v in name_to_ids.items() if len(v) > 1]
    if offending_names:
        raise ValueError(
            "Data Integrity Error: The following common_name(s) map to multiple "
            f"chembl_ids across primary and combined_with fields: {offending_names}"
        )


def map_clinical_trial_phase(value: DrugScalar) -> str:
    """Map OpenTargets numeric phase codes to display strings; pass through existing labels."""
    if pd.isna(value):
        result = "Preclinical"
    elif value in CLINICAL_PHASE_MAP:
        result = CLINICAL_PHASE_MAP[value]
    elif isinstance(value, str):
        result = value
    else:
        result = "Unknown"
    return result


def validate_drug_list_integrity(drug_list: pd.DataFrame) -> pd.DataFrame:
    """Full drug_list validation including cross-field ChEMBL/name checks (``drug_info``).

    Returns:
        Stripped copy of *drug_list*.
    """
    drug_list = prepare_drug_list(drug_list)
    validate_drug_name_chembl_mappings(drug_list)
    return drug_list
