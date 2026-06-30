"""Tests for the drug_info transform."""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import drug_info


def _minimal_drug_list_row(**overrides: object) -> dict[str, object]:
    """Build one drug_list row with defaults suitable for _collapse_drug_nominations.

    Pass overrides as column_name=value keyword pairs (e.g. chembl_id="CHEMBL1",
    contact_pi="Bob Beta") to replace the matching default values for a test row.
    """
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

    def test_uses_last_token_with_middle_initial(self) -> None:
        assert drug_info._pi_lastname_sort_key({"contact_pi": "Bob B. Beta"}) == "beta"

    def test_uses_last_token_with_middle_name(self) -> None:
        assert (
            drug_info._pi_lastname_sort_key({"contact_pi": "Bob Bill Beta"}) == "beta"
        )

    def test_uses_hyphenated_last_name(self) -> None:
        assert (
            drug_info._pi_lastname_sort_key({"contact_pi": "Zara Alpha-Smith"})
            == "alpha-smith"
        )


class TestMapEnsemblIdListToDicts:
    """Tests for _map_ensembl_id_list_to_dicts."""

    def test_maps_ensembl_ids_to_symbols(self) -> None:
        gene_map = {"ENSG000001": "GENE1"}
        result = drug_info._map_ensembl_id_list_to_dicts(
            ["ENSG000001", "ENSG000099"], gene_map, {"ENSG000001"}
        )
        assert result == [
            {
                "ensembl_gene_id": "ENSG000001",
                "hgnc_symbol": "GENE1",
                "is_nominated_target": True,
            },
            {
                "ensembl_gene_id": "ENSG000099",
                "hgnc_symbol": "ENSG000099",
                "is_nominated_target": False,
            },
        ]

    def test_falls_back_to_ensembl_id_when_symbol_unresolved(self) -> None:
        """D2: hgnc_symbol uses Ensembl ID when gene is absent from gene_metadata."""
        result = drug_info._map_ensembl_id_list_to_dicts(["ENSG000099"], {}, set())
        assert result == [
            {
                "ensembl_gene_id": "ENSG000099",
                "hgnc_symbol": "ENSG000099",
                "is_nominated_target": False,
            },
        ]

    def test_returns_empty_for_non_list_input(self) -> None:
        assert drug_info._map_ensembl_id_list_to_dicts(None, {}, set()) == []

    def test_skips_null_ensembl_ids(self) -> None:
        result = drug_info._map_ensembl_id_list_to_dicts(
            [None, "ENSG1"], {"ENSG1": "G1"}, {"ENSG1"}
        )
        assert result == [
            {
                "ensembl_gene_id": "ENSG1",
                "hgnc_symbol": "G1",
                "is_nominated_target": True,
            }
        ]


class TestGetBestIupacId:
    """Tests for _get_best_iupac_id."""

    def test_returns_first_non_unknown_value(self) -> None:
        group = pd.Series([None, "Unknown", "IUPAC-A", "IUPAC-B"])
        assert drug_info._get_best_iupac_id(group) == "IUPAC-A"

    def test_returns_unknown_when_only_unknown_or_null(self) -> None:
        assert drug_info._get_best_iupac_id(pd.Series(["Unknown", None])) == "Unknown"
        assert drug_info._get_best_iupac_id(pd.Series([None, None])) == "Unknown"


class TestResolveLinkedTargetSymbols:
    """Tests for _resolve_linked_target_symbols."""

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
        result = drug_info._resolve_linked_target_symbols(
            drug_metadata, gene_metadata, {"ENSG000001"}
        )
        assert result["linked_targets"].iloc[0] == [
            {
                "ensembl_gene_id": "ENSG000001",
                "hgnc_symbol": "GENE1",
                "is_nominated_target": True,
            },
            {
                "ensembl_gene_id": "ENSG000099",
                "hgnc_symbol": "ENSG000099",
                "is_nominated_target": False,
            },
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

    def _build_datasets(self, file_map: dict[str, str]) -> dict[str, pd.DataFrame]:
        """Load each dataset named in *file_map* by file extension.

        Only the datasets present in *file_map* are loaded, so callers can omit a
        dataset to exercise the missing-dataset path.
        """
        datasets: dict[str, pd.DataFrame] = {}
        for name, filename in file_map.items():
            path = os.path.join(self.data_files_path, "input", filename)
            if filename.endswith(".csv"):
                df = pd.read_csv(path)
                if "source" in df.columns and "program" not in df.columns:
                    df = df.rename(columns={"source": "program"})
                datasets[name] = df
            elif filename.endswith(".json"):
                datasets[name] = pd.read_json(path, orient="records")
            elif filename.endswith(".feather"):
                datasets[name] = pd.read_feather(path)
        return datasets

    def _load_datasets(
        self,
        ot_file: str,
        dl_file: str,
        gm_file: str = "gene_metadata_good.feather",
        ht_file: str = "target_list_good.csv",
    ) -> dict[str, pd.DataFrame]:
        return self._build_datasets(
            {
                "drug_metadata": ot_file,
                "drug_list": dl_file,
                "gene_metadata": gm_file,
                "target_list": ht_file,
            }
        )

    def test_transform_drug_info_should_pass(self) -> None:
        datasets = self._load_datasets("drug_metadata_good.json", "drug_list_good.csv")
        output_df = drug_info.transform_drug_info(datasets=datasets)
        assert output_df["year_of_first_approval"].dtype == pd.Int64Dtype()
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", "drug_info_good_output.json"),
        )
        pd.testing.assert_frame_equal(
            output_df.sort_values("chembl_id").reset_index(drop=True),
            expected_df.sort_values("chembl_id").reset_index(drop=True),
            check_dtype=False,
        )

    def test_keeps_nominated_drugs_without_ot_metadata(self) -> None:
        """Left merge: nominated chembl_ids without OT rows keep null metadata fields."""
        datasets = self._load_datasets(
            "drug_metadata_minimal.json", "drug_list_good.csv"
        )
        output_df = drug_info.transform_drug_info(datasets=datasets)
        assert set(output_df["chembl_id"]) == {"CHEMBL111", "CHEMBL222"}

        drug_a = output_df.loc[output_df["chembl_id"] == "CHEMBL111"].iloc[0]
        assert drug_a["common_name"] == "DrugA"
        assert pd.isna(drug_a["description"])
        assert pd.isna(drug_a["modality"])
        assert len(drug_a["drug_nominations"]) == 2

        drug_b = output_df.loc[output_df["chembl_id"] == "CHEMBL222"].iloc[0]
        assert drug_b["description"] == "Protein drug."

    def test_sets_is_nominated_target_from_target_list(self) -> None:
        """is_nominated_target is true only for ENSGs in target_list."""
        datasets = self._load_datasets("drug_metadata_good.json", "drug_list_good.csv")
        output_df = drug_info.transform_drug_info(datasets=datasets)

        drug_a = output_df.loc[output_df["chembl_id"] == "CHEMBL111"].iloc[0]
        assert drug_a["linked_targets"] == [
            {
                "ensembl_gene_id": "ENSG000001",
                "hgnc_symbol": "GENE1",
                "is_nominated_target": True,
            }
        ]

        drug_b = output_df.loc[output_df["chembl_id"] == "CHEMBL222"].iloc[0]
        assert drug_b["linked_targets"] == [
            {
                "ensembl_gene_id": "ENSG000099",
                "hgnc_symbol": "GENE9",
                "is_nominated_target": False,
            }
        ]

    def test_missing_target_list_dataset_raises(self) -> None:
        datasets = self._load_datasets("drug_metadata_good.json", "drug_list_good.csv")
        del datasets["target_list"]
        with pytest.raises(ValueError, match="Missing required datasets"):
            drug_info.transform_drug_info(datasets=datasets)

    @pytest.mark.parametrize(
        "input_datasets,error_match,error_type",
        [
            (
                {"drug_metadata": "drug_metadata_good.json"},
                "Missing required datasets",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_missing_program.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "Missing required columns",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_missing_drug_bank_id.json",
                    "drug_list": "drug_list_good.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "Missing required columns",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_unpaired_combined_with.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "combined_with_common_name",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_integrity_conflict.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "Data Integrity Error",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_invalid_phase.json",
                    "drug_list": "drug_list_good.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "maximum_clinical_trial_phase",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_bad_linkage.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "Data Integrity Error",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_invalid_chembl_id.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "matches_regex",
                ValueError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_duplicate_chembl_id.json",
                    "drug_list": "drug_list_good.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "Merge keys are not unique",
                pd.errors.MergeError,
            ),
            (
                {
                    "drug_metadata": "drug_metadata_good.json",
                    "drug_list": "drug_list_empty_contact_pi.csv",
                    "gene_metadata": "gene_metadata_good.feather",
                    "target_list": "target_list_good.csv",
                },
                "contact_pi.*not_empty",
                ValueError,
            ),
        ],
        ids=[
            "missing gene_metadata dataset",
            "missing program column",
            "missing drug_bank_id column",
            "unpaired combined_with columns",
            "chembl_id common_name conflict",
            "invalid maximum_clinical_trial_phase",
            "bad common_name to chembl_id linkage",
            "invalid chembl_id regex",
            "duplicate chembl_id in ot metadata",
            "empty contact_pi violates not_empty",
        ],
    )
    def test_transform_drug_info_should_fail(
        self,
        input_datasets: dict[str, str],
        error_match: str,
        error_type: type[BaseException],
    ) -> None:
        datasets = self._build_datasets(input_datasets)
        with pytest.raises(error_type, match=error_match):
            drug_info.transform_drug_info(datasets=datasets)
