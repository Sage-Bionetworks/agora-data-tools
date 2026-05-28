"""Tests for shared drug transform utilities."""

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils import drug_transform_utils as dtu


class TestValidateCombinedWithColumnPairs:
    """Tests for validate_combined_with_column_pairs."""

    def test_passes_when_both_present(self) -> None:
        df = pd.DataFrame(
            {
                "combined_with_common_name": ["DrugA"],
                "combined_with_chembl_id": ["CHEMBL1"],
            }
        )
        dtu.validate_combined_with_column_pairs(df)

    def test_passes_when_both_missing(self) -> None:
        df = pd.DataFrame(
            {
                "combined_with_common_name": [None, ""],
                "combined_with_chembl_id": [None, "  "],
            }
        )
        dtu.validate_combined_with_column_pairs(df)

    def test_raises_when_only_name_present(self) -> None:
        df = pd.DataFrame(
            {
                "combined_with_common_name": ["DrugA"],
                "combined_with_chembl_id": [None],
            }
        )
        with pytest.raises(ValueError, match="combined_with_common_name"):
            dtu.validate_combined_with_column_pairs(df)

    def test_raises_when_only_id_present(self) -> None:
        df = pd.DataFrame(
            {
                "combined_with_common_name": [None],
                "combined_with_chembl_id": ["CHEMBL1"],
            }
        )
        with pytest.raises(ValueError, match="combined_with_chembl_id"):
            dtu.validate_combined_with_column_pairs(df)


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

    def test_passes_for_consistent_combined_with(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, "DrugA"],
                "combined_with_chembl_id": [None, "CHEMBL1"],
            }
        )
        result = dtu.validate_drug_list_integrity(df)
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
        with pytest.raises(ValueError, match="combined_with_common_name"):
            dtu.validate_drug_list_integrity(df)

    def test_raises_when_name_maps_to_multiple_ids(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugA"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, None],
                "combined_with_chembl_id": [None, None],
            }
        )
        with pytest.raises(ValueError, match="common_name"):
            dtu.validate_drug_list_integrity(df)

    def test_raises_when_id_maps_to_multiple_names(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL1"],
                "combined_with_common_name": [None, None],
                "combined_with_chembl_id": [None, None],
            }
        )
        with pytest.raises(ValueError, match="chembl_id"):
            dtu.validate_drug_list_integrity(df)

    def test_raises_on_cross_field_mapping_conflict(self) -> None:
        # combined_with uses a different chembl_id for a name used as a primary drug.
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
