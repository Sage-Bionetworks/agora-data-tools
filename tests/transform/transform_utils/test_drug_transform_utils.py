"""Tests for shared drug transform utilities."""

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils import drug_transform_utils as dtu


class TestMapClinicalTrialPhase:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (1, "Phase I"),
            (4, "Phase IV"),
            (-1, "Unknown"),
            ("Phase III", "Phase III"),
            (pd.NA, "Preclinical"),
        ],
    )
    def test_maps_numeric_and_string_phases(self, value, expected):
        assert dtu.map_clinical_trial_phase(value) == expected


class TestPrepareDrugList:
    def test_passes_for_consistent_drug_list(self):
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

    def test_raises_for_unpaired_combined_with(self):
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
