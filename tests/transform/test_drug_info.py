"""Tests for the drug_info transform."""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import drug_info


def _minimal_drug_list_row(**overrides: object) -> dict[str, object]:
    """Build one drug_list row with defaults suitable for _collapse_drug_nominations."""
    row: dict[str, object] = {
        "grant_number": "G1",
        "contact_pi": "Alice Smith",
        "priority_score": 1,
        "priority_score_criteria": "x",
        "common_name": "DrugA",
        "chembl_id": "CHEMBL1",
        "combined_with_common_name": None,
        "combined_with_chembl_id": None,
        "iupac_id": "IUPAC-1",
        "evidence": "ev",
        "data_used": "d",
        "ad_moa": "moa",
        "published": True,
        "reference": "ref",
        "computational_validation_status": None,
        "computational_validation_results": None,
        "experimental_validation_status": None,
        "experimental_validation_results": None,
        "additional_evidence": None,
        "contributors": "team",
        "initial_nomination": 2024,
        "program": "Prog",
    }
    row.update(overrides)
    return row


class TestPiLastnameSortKey:
    """Tests for _pi_lastname_sort_key."""

    def test_uses_last_token_before_comma_suffix(self) -> None:
        assert (
            drug_info._pi_lastname_sort_key({"contact_pi": "Bob Beta, PhD"}) == "beta"
        )

    def test_uses_last_token_when_no_comma(self) -> None:
        assert drug_info._pi_lastname_sort_key({"contact_pi": "Zara Alpha"}) == "alpha"

    def test_returns_empty_for_missing_or_invalid_contact_pi(self) -> None:
        assert drug_info._pi_lastname_sort_key({}) == ""
        assert drug_info._pi_lastname_sort_key({"contact_pi": None}) == ""
        assert drug_info._pi_lastname_sort_key({"contact_pi": 42}) == ""


class TestSortByPiLastname:
    """Tests for _sort_by_pi_lastname."""

    def test_sorts_nominations_by_pi_last_name(self) -> None:
        nominations = [
            {"contact_pi": "Zara Alpha"},
            {"contact_pi": "Bob Beta, PhD"},
        ]
        result = drug_info._sort_by_pi_lastname(nominations)
        assert [n["contact_pi"] for n in result] == ["Zara Alpha", "Bob Beta, PhD"]

    def test_returns_non_list_unchanged(self) -> None:
        assert drug_info._sort_by_pi_lastname("not a list") == "not a list"


class TestStripRedundantNominationKeys:
    """Tests for _strip_redundant_nomination_keys."""

    def test_removes_chembl_common_name_and_iupac_id(self) -> None:
        nominations = [
            {
                "chembl_id": "CHEMBL1",
                "common_name": "DrugA",
                "iupac_id": "IUPAC-1",
                "grant_number": "G1",
            }
        ]
        result = drug_info._strip_redundant_nomination_keys(nominations)
        assert result == [{"grant_number": "G1"}]

    def test_preserves_non_dict_entries(self) -> None:
        nominations: list = ["keep", {"chembl_id": "CHEMBL1", "program": "P"}]
        result = drug_info._strip_redundant_nomination_keys(nominations)
        assert result[0] == "keep"
        assert result[1] == {"program": "P"}


class TestResolveTargetList:
    """Tests for _resolve_target_list."""

    def test_maps_ensembl_ids_to_symbols(self) -> None:
        gene_map = {"ENSG000001": "GENE1"}
        result = drug_info._resolve_target_list(["ENSG000001", "ENSG000099"], gene_map)
        assert result == [
            {"ensembl_gene_id": "ENSG000001", "hgnc_symbol": "GENE1"},
            {"ensembl_gene_id": "ENSG000099", "hgnc_symbol": "ENSG000099"},
        ]

    def test_returns_empty_for_non_list_input(self) -> None:
        assert drug_info._resolve_target_list(None, {}) == []

    def test_skips_null_ensembl_ids(self) -> None:
        result = drug_info._resolve_target_list([None, "ENSG1"], {"ENSG1": "G1"})
        assert result == [{"ensembl_gene_id": "ENSG1", "hgnc_symbol": "G1"}]


class TestGetBestIupacId:
    """Tests for _get_best_iupac_id."""

    def test_returns_first_non_unknown_value(self) -> None:
        group = pd.Series(["Unknown", "IUPAC-A", "IUPAC-B"])
        assert drug_info._get_best_iupac_id(group) == "IUPAC-A"

    def test_returns_unknown_when_only_unknown_or_null(self) -> None:
        assert drug_info._get_best_iupac_id(pd.Series(["Unknown", None])) == "Unknown"
        assert drug_info._get_best_iupac_id(pd.Series([None, None])) == "Unknown"


class TestResolveLinkedTargets:
    """Tests for _resolve_linked_targets."""

    def test_replaces_linked_targets_with_gene_dicts(self) -> None:
        drug_metadata = pd.DataFrame(
            {
                "chembl_id": ["CHEMBL1"],
                "linked_targets": [["ENSG000001", "ENSG000099"]],
            }
        )
        gene_metadata = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSG000001"],
                "symbol": ["GENE1"],
            }
        )
        result = drug_info._resolve_linked_targets(drug_metadata, gene_metadata)
        assert result["linked_targets"].iloc[0] == [
            {"ensembl_gene_id": "ENSG000001", "hgnc_symbol": "GENE1"},
            {"ensembl_gene_id": "ENSG000099", "hgnc_symbol": "ENSG000099"},
        ]


class TestCollapseDrugNominations:
    """Tests for _collapse_drug_nominations."""

    def test_collapses_to_one_row_per_chembl_id_with_nested_nominations(self) -> None:
        drug_list = pd.DataFrame(
            [
                _minimal_drug_list_row(
                    grant_number="G1",
                    contact_pi="Zara Alpha",
                    chembl_id="CHEMBL111",
                    common_name="DrugA",
                    iupac_id="IUPAC-A",
                ),
                _minimal_drug_list_row(
                    grant_number="G2",
                    contact_pi="Bob Beta, PhD",
                    chembl_id="CHEMBL111",
                    common_name="DrugA",
                    iupac_id="Unknown",
                ),
                _minimal_drug_list_row(
                    grant_number="G3",
                    contact_pi="Marina Sirota",
                    chembl_id="CHEMBL222",
                    common_name="DrugB",
                    iupac_id=None,
                    combined_with_common_name="Partner1",
                    combined_with_chembl_id="CHEMBL333",
                ),
            ]
        )

        result = drug_info._collapse_drug_nominations(drug_list.copy())

        assert len(result) == 2
        assert set(result["chembl_id"]) == {"CHEMBL111", "CHEMBL222"}

        drug_a = result.loc[result["chembl_id"] == "CHEMBL111"].iloc[0]
        assert drug_a["common_name"] == "DrugA"
        assert drug_a["iupac_id"] == "IUPAC-A"
        assert len(drug_a["drug_nominations"]) == 2
        assert [n["contact_pi"] for n in drug_a["drug_nominations"]] == [
            "Zara Alpha",
            "Bob Beta, PhD",
        ]
        for nom in drug_a["drug_nominations"]:
            assert "chembl_id" not in nom
            assert "common_name" not in nom
            assert "iupac_id" not in nom

        drug_b = result.loc[result["chembl_id"] == "CHEMBL222"].iloc[0]
        assert drug_b["iupac_id"] is None
        assert drug_b["drug_nominations"][0]["combined_with"] == [
            {"common_name": "Partner1", "chembl_id": "CHEMBL333"}
        ]


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
