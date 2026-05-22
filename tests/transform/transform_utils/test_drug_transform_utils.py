"""Tests for shared drug transform utilities."""

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils import drug_transform_utils as dtu


class TestMapClinicalTrialPhase:
    """Tests for map_clinical_trial_phase numeric and string phase mapping."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            (1, "Phase I"),
            (4, "Phase IV"),
            (-1, "Unknown"),
            ("Phase III", "Phase III"),
            (pd.NA, "Preclinical"),
            (0, "Unknown"),
        ],
    )
    def test_maps_numeric_and_string_phases(self, value: object, expected: str) -> None:
        assert dtu.map_clinical_trial_phase(value) == expected


class TestStripDrugListColumns:
    """Tests for strip_drug_list_columns."""

    def test_strips_whitespace_from_drug_columns(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["  DrugA  "],
                "chembl_id": [" CHEMBL1 "],
                "combined_with_common_name": [None],
                "combined_with_chembl_id": [None],
            }
        )
        result = dtu.strip_drug_list_columns(df)
        assert result["common_name"].iloc[0] == "DrugA"
        assert result["chembl_id"].iloc[0] == "CHEMBL1"


class TestBuildCombinedWithList:
    """Tests for build_combined_with_list."""

    def test_returns_empty_when_both_missing(self) -> None:
        assert dtu.build_combined_with_list(None, None) == []
        assert dtu.build_combined_with_list("", "") == []

    def test_parses_comma_delimited_partners(self) -> None:
        result = dtu.build_combined_with_list("DrugA, DrugB", "CHEMBL1, CHEMBL2")
        assert result == [
            {"common_name": "DrugA", "chembl_id": "CHEMBL1"},
            {"common_name": "DrugB", "chembl_id": "CHEMBL2"},
        ]

    def test_raises_when_name_and_id_counts_differ(self) -> None:
        with pytest.raises(ValueError, match="Mismatched combined_with lists"):
            dtu.build_combined_with_list("DrugA, DrugB", "CHEMBL1")


class TestValidateDrugNameChemblMappings:
    """Tests for validate_drug_name_chembl_mappings cross-field checks."""

    def test_passes_for_consistent_mappings(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, "DrugA"],
                "combined_with_chembl_id": [None, "CHEMBL1"],
            }
        )
        dtu.validate_drug_name_chembl_mappings(df)

    def test_raises_when_combined_with_conflicts_with_primary(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, "DrugA"],
                "combined_with_chembl_id": [None, "CHEMBL999"],
            }
        )
        with pytest.raises(ValueError, match="common_name"):
            dtu.validate_drug_name_chembl_mappings(df)


class TestPrepareDrugList:
    """Tests for prepare_drug_list stripping and validation."""

    def test_passes_for_consistent_drug_list(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA"],
                "chembl_id": ["CHEMBL1"],
                "combined_with_common_name": [None],
                "combined_with_chembl_id": [None],
            }
        )
        result = dtu.prepare_drug_list(df)
        assert result["chembl_id"].iloc[0] == "CHEMBL1"

    def test_raises_for_unpaired_combined_with(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA"],
                "chembl_id": ["CHEMBL1"],
                "combined_with_common_name": ["Partner"],
                "combined_with_chembl_id": [None],
            }
        )
        with pytest.raises(ValueError, match="Data Integrity Error"):
            dtu.prepare_drug_list(df)


class TestValidateDrugListIntegrity:
    """Tests for validate_drug_list_integrity full validation pipeline."""

    def test_returns_stripped_copy_on_pass(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["  DrugA  "],
                "chembl_id": [" CHEMBL1 "],
                "combined_with_common_name": [None],
                "combined_with_chembl_id": [None],
            }
        )
        result = dtu.validate_drug_list_integrity(df)
        assert result["common_name"].iloc[0] == "DrugA"
        assert result["chembl_id"].iloc[0] == "CHEMBL1"

    def test_raises_on_cross_field_mapping_conflict(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, "DrugA"],
                "combined_with_chembl_id": [None, "CHEMBL999"],
            }
        )
        with pytest.raises(ValueError, match="common_name"):
            dtu.validate_drug_list_integrity(df)
