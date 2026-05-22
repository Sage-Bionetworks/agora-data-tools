import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import nominated_drugs


class TestTransformNominatedDrugs:
    """Tests for transform_nominated_drugs pass/fail paths using nominated_drugs fixtures."""

    data_files_path = "tests/test_assets/nominated_drugs"

    pass_test_data = [
        (
            "drug_list_good_input.csv",
            "drug_metadata_good_input.json",
            "nominated_drugs_good_test_output.json",
        ),
        (
            "drug_list_whitespace_input.csv",
            "drug_metadata_good_input.json",
            "nominated_drugs_good_test_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with leading and trailing whitespace stripped before validation and grouping",
    ]

    fail_test_data = [
        (
            {"drug_list": "drug_list_good_input.csv"},
            ValueError,
            "Missing required datasets",
        ),
        (
            {
                "drug_list": "drug_list_missing_source_column_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "Missing required columns",
        ),
        (
            {
                "drug_list": "drug_list_bad_linkage_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "Data Integrity Error",
        ),
        (
            {
                "drug_list": "drug_list_bad_reverse_linkage_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "chembl_id",
        ),
        (
            {
                "drug_list": "drug_list_empty_chembl_id_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "violate rule",
        ),
        (
            {
                "drug_list": "drug_list_good_input.csv",
                "drug_metadata": "drug_metadata_invalid_modality_input.json",
            },
            ValueError,
            "violate rule",
        ),
        (
            {
                "drug_list": "drug_list_mismatched_combined_with_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "combined_with_common_name",
        ),
        (
            {
                "drug_list": "drug_list_bad_combined_with_linkage_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "combined_with_common_name",
        ),
        (
            {
                "drug_list": "drug_list_bad_reverse_combined_with_linkage_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "combined_with_chembl_id",
        ),
        (
            {
                "drug_list": "drug_list_invalid_chembl_id_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "matches_regex",
        ),
        (
            {
                "drug_list": "drug_list_good_input.csv",
                "drug_metadata": "drug_metadata_invalid_phase_input.json",
            },
            ValueError,
            "maximum_clinical_trial_phase",
        ),
        (
            {
                "drug_list": "drug_list_good_input.csv",
                "drug_metadata": "drug_metadata_duplicate_chembl_id_input.json",
            },
            pd.errors.MergeError,
            "Merge keys are not unique",
        ),
        (
            {
                "drug_list": "drug_list_cross_field_combined_with_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "common_name",
        ),
        (
            {
                "drug_list": "drug_list_empty_contact_pi_input.csv",
                "drug_metadata": "drug_metadata_good_input.json",
            },
            ValueError,
            "violate rule",
        ),
    ]
    fail_test_ids = [
        "Fail with missing drug_metadata dataset",
        "Fail with missing required column in drug_list",
        "Fail with broken common_name to chembl_id linkage",
        "Fail with broken chembl_id to common_name linkage",
        "Fail with empty chembl_id in drug_list",
        "Fail with invalid modality in drug_metadata",
        "Fail when combined_with name and ID are not both present",
        "Fail with broken combined_with_common_name to combined_with_chembl_id linkage",
        "Fail with broken combined_with_chembl_id to combined_with_common_name linkage",
        "Fail with chembl_id not matching CHEMBL prefix",
        "Fail with invalid maximum_clinical_trial_phase in drug_metadata",
        "Fail with duplicate chembl_id in drug_metadata",
        "Fail with cross-field common_name to chembl_id conflict in combined_with",
        "Fail with empty contact_pi in drug_list",
    ]

    @staticmethod
    def _load_datasets(
        drug_list_file: str, drug_metadata_file: str
    ) -> dict[str, pd.DataFrame]:
        drug_list_df = pd.read_csv(
            os.path.join(
                TestTransformNominatedDrugs.data_files_path, "input", drug_list_file
            )
        )
        drug_metadata_df = pd.read_json(
            os.path.join(
                TestTransformNominatedDrugs.data_files_path,
                "input",
                drug_metadata_file,
            )
        )
        return {"drug_list": drug_list_df, "drug_metadata": drug_metadata_df}

    @pytest.mark.parametrize(
        "drug_list_file, drug_metadata_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_nominated_drugs_should_pass(
        self,
        drug_list_file: str,
        drug_metadata_file: str,
        expected_output_file: str,
    ) -> None:
        datasets = self._load_datasets(drug_list_file, drug_metadata_file)
        output_df = nominated_drugs.transform_nominated_drugs(datasets=datasets)
        output_df = output_df.reset_index(drop=True)
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        expected_df["year_of_first_approval"] = expected_df[
            "year_of_first_approval"
        ].astype("Int64")
        expected_df = expected_df.reset_index(drop=True)
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_datasets, error_type, error_match", fail_test_data, ids=fail_test_ids
    )
    def test_transform_nominated_drugs_should_fail(
        self,
        input_datasets: dict[str, str],
        error_type: type[BaseException],
        error_match: str,
    ) -> None:
        with pytest.raises(error_type, match=error_match):
            datasets = {}
            for dataset_name, file_name in input_datasets.items():
                path = os.path.join(self.data_files_path, "input", file_name)
                if file_name.endswith(".json"):
                    datasets[dataset_name] = pd.read_json(path)
                else:
                    datasets[dataset_name] = pd.read_csv(path)
            nominated_drugs.transform_nominated_drugs(datasets=datasets)
