"""Tests for shared drug transform utilities."""

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils import drug_transform_utils as dtu


class TestValidateDrugListIntegrity:
    """Tests for validate_drug_list_integrity full validation pipeline."""

    def test_passes_on_valid_drug_list(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA"],
                "chembl_id": ["CHEMBL1"],
                "combined_with_common_name": [None],
                "combined_with_chembl_id": [None],
            }
        )
        dtu.validate_drug_list_integrity(df)

    def test_passes_for_consistent_combined_with(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
                "combined_with_common_name": [None, "DrugA"],
                "combined_with_chembl_id": [None, "CHEMBL1"],
            }
        )
        dtu.validate_drug_list_integrity(df)

    def test_raises_for_unpaired_combined_with_name_only(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA"],
                "chembl_id": ["CHEMBL1"],
                "combined_with_common_name": ["Partner"],
                "combined_with_chembl_id": [None],
            }
        )
        with pytest.raises(
            ValueError, match="have a value in only one of.*combined_with_common_name"
        ):
            dtu.validate_drug_list_integrity(df)

    def test_raises_for_unpaired_combined_with_id_only(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA"],
                "chembl_id": ["CHEMBL1"],
                "combined_with_common_name": [None],
                "combined_with_chembl_id": ["CHEMBL2"],
            }
        )
        with pytest.raises(
            ValueError, match="have a value in only one of.*combined_with_chembl_id"
        ):
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
        with pytest.raises(ValueError, match="common_name.*multiple chembl_id values"):
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
        with pytest.raises(ValueError, match="chembl_id.*multiple common_name values"):
            dtu.validate_drug_list_integrity(df)


class TestCapitalizeFirstCharacter:
    """Tests for capitalize_first_character on flat string columns."""

    def test_capitalizes_string_column(self) -> None:
        df = pd.DataFrame({"name": ["lowercase text"]})
        result = dtu.capitalize_first_character(df, ["name"])
        assert result["name"].iloc[0] == "Lowercase text"

    def test_preserves_mixed_case_after_first_character(self) -> None:
        df = pd.DataFrame({"name": ["aPOE variant"]})
        result = dtu.capitalize_first_character(df, ["name"])
        assert result["name"].iloc[0] == "APOE variant"

    def test_leaves_non_string_and_empty_values_unchanged(self) -> None:
        df = pd.DataFrame({"name": [None, "", "text"]})
        result = dtu.capitalize_first_character(df, ["name"])
        assert result["name"].iloc[0] is None
        assert result["name"].iloc[1] == ""
        assert result["name"].iloc[2] == "Text"

    def test_skips_missing_columns(self) -> None:
        df = pd.DataFrame({"name": ["text"]})
        result = dtu.capitalize_first_character(df, ["absent"])
        assert result["name"].iloc[0] == "text"
