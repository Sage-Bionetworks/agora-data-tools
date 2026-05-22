"""Tests for the drug_info transform."""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import drug_info


class TestTransformDrugInfo:
    """Tests for transform_drug_info pass/fail paths using drug_info fixtures."""

    data_files_path = "tests/test_assets/drug_info"

    def _load_datasets(
        self,
        ot_file: str,
        dl_file: str,
        gm_file: str = "gene_metadata_good.feather",
    ) -> dict[str, pd.DataFrame]:
        drug_list = pd.read_csv(os.path.join(self.data_files_path, "input", dl_file))
        if "source" in drug_list.columns and "program" not in drug_list.columns:
            drug_list = drug_list.rename(columns={"source": "program"})
        return {
            "ot_drug_metadata": pd.read_json(
                os.path.join(self.data_files_path, "input", ot_file),
                orient="records",
            ),
            "drug_list": drug_list,
            "gene_metadata": pd.read_feather(
                os.path.join(self.data_files_path, "input", gm_file)
            ),
        }

    def test_transform_drug_info_should_pass(self) -> None:
        datasets = self._load_datasets(
            "ot_drug_metadata_good.json", "drug_list_good.csv"
        )
        output_df = drug_info.transform_drug_info(datasets=datasets)
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", "drug_info_good_output.json"),
        )
        pd.testing.assert_frame_equal(
            output_df.sort_values("chembl_id").reset_index(drop=True),
            expected_df.sort_values("chembl_id").reset_index(drop=True),
            check_dtype=False,
        )

    @pytest.mark.parametrize(
        "input_datasets,error_match",
        [
            (
                {"ot_drug_metadata": "ot_drug_metadata_good.json"},
                "Missing required datasets",
            ),
            (
                {
                    "ot_drug_metadata": "ot_drug_metadata_good.json",
                    "drug_list": "drug_list_missing_program.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                },
                "Missing required columns",
            ),
            (
                {
                    "ot_drug_metadata": "ot_drug_metadata_minimal.json",
                    "drug_list": "drug_list_mismatched_combined.csv",
                    "gene_metadata": "gene_metadata_minimal.feather",
                },
                "Mismatched combined_with",
            ),
            (
                {
                    "ot_drug_metadata": "ot_drug_metadata_good.json",
                    "drug_list": "drug_list_mismatched_combined_with_input.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                },
                "combined_with_common_name",
            ),
            (
                {
                    "ot_drug_metadata": "ot_drug_metadata_good.json",
                    "drug_list": "drug_list_integrity_conflict.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                },
                "Data Integrity Error",
            ),
            (
                {
                    "ot_drug_metadata": "ot_drug_metadata_invalid_phase.json",
                    "drug_list": "drug_list_good.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                },
                "maximum_clinical_trial_phase",
            ),
        ],
        ids=[
            "missing gene_metadata dataset",
            "missing program column",
            "mismatched combined_with",
            "unpaired combined_with columns",
            "chembl_id common_name conflict",
            "invalid maximum_clinical_trial_phase",
        ],
    )
    def test_transform_drug_info_should_fail(
        self, input_datasets: dict[str, str], error_match: str
    ) -> None:
        with pytest.raises(ValueError, match=error_match):
            if "drug_list" in input_datasets and input_datasets["drug_list"].endswith(
                ".csv"
            ):
                datasets = self._load_datasets(
                    input_datasets.get(
                        "ot_drug_metadata", "ot_drug_metadata_good.json"
                    ),
                    input_datasets["drug_list"],
                    input_datasets.get("gene_metadata", "gene_metadata_good.feather"),
                )
            elif "ot_drug_metadata" in input_datasets:
                drug_list = pd.read_csv(
                    os.path.join(self.data_files_path, "input", "drug_list_good.csv")
                )
                if "source" in drug_list.columns and "program" not in drug_list.columns:
                    drug_list = drug_list.rename(columns={"source": "program"})
                datasets = {
                    "ot_drug_metadata": pd.read_json(
                        os.path.join(
                            self.data_files_path,
                            "input",
                            input_datasets["ot_drug_metadata"],
                        ),
                        orient="records",
                    ),
                    "drug_list": drug_list,
                }
            drug_info.transform_drug_info(datasets=datasets)


class TestDrugInfoSynapseGolden:
    """Compare transform output to prototype golden when Synapse discovery files exist."""

    discovery_path = "staging/synapse_discovery"

    @pytest.fixture
    def synapse_inputs_available(self) -> bool:
        required = [
            "harmonized_drug_nominations_4_23_26.csv",
            "opentargets_drug_metadata.json",
            "gene_table_merged_GRCh38.p14.feather",
            "drug_info.json",
        ]
        return all(
            os.path.exists(os.path.join(self.discovery_path, f)) for f in required
        )

    def test_output_matches_golden_schema_and_row_count(
        self, synapse_inputs_available: bool
    ) -> None:
        if not synapse_inputs_available:
            pytest.skip("Synapse discovery files not present")

        from agoradatatools.etl import utils as etl_utils

        drug_list = pd.read_csv(
            os.path.join(self.discovery_path, "harmonized_drug_nominations_4_23_26.csv")
        )
        drug_list = etl_utils.rename_columns(drug_list, {"source": "program"})
        ot = pd.read_json(
            os.path.join(self.discovery_path, "opentargets_drug_metadata.json"),
            orient="records",
        )
        gm = pd.read_feather(
            os.path.join(self.discovery_path, "gene_table_merged_GRCh38.p14.feather")
        )
        golden = pd.read_json(
            os.path.join(self.discovery_path, "drug_info.json"), orient="records"
        )

        output = drug_info.transform_drug_info(
            {
                "ot_drug_metadata": ot,
                "drug_list": drug_list,
                "gene_metadata": gm,
            }
        )

        assert list(output.columns) == list(golden.columns)
        assert len(output) == len(golden)

        # Golden syn73880976 predates drug_list v14: Cefaclor chembl_id changed.
        known_chembl_id_updates = {"CHEMBL680": "CHEMBL1201018"}
        normalized_golden_ids = {
            known_chembl_id_updates.get(cid, cid) for cid in golden["chembl_id"]
        }
        assert set(output["chembl_id"]) == normalized_golden_ids
