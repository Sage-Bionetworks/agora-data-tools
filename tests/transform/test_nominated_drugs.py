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
    ]
    pass_test_ids = [
        "Pass with good data",
    ]

    # Each tuple is (drug_list_file, drug_metadata_file, error_type, error_match).
    # drug_metadata_file is None when the test intentionally omits that dataset.
    fail_test_data = [
        # drug_metadata dataset is missing entirely.
        (
            "drug_list_good_input.csv",
            None,
            ValueError,
            "Missing required datasets",
        ),
        # drug_list is missing the required "source" column.
        (
            "drug_list_missing_source_column_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "Missing required columns",
        ),
        # Broken linkage: DrugA is paired with both CHEMBL1 and CHEMBL99 in chembl_id,
        # so one common_name maps to multiple chembl_ids.
        (
            "drug_list_bad_linkage_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "common_name.*multiple chembl_id values",
        ),
        # Reverse linkage: CHEMBL1 is shared by DrugA and DrugB, so one chembl_id
        # maps to multiple common_names.
        (
            "drug_list_bad_reverse_linkage_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "chembl_id.*multiple common_name values",
        ),
        # chembl_id is empty in drug_list (fails the not_empty rule).
        (
            "drug_list_empty_chembl_id_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "column 'chembl_id'.*not_empty",
        ),
        # drug_metadata has a modality value outside the allowed set.
        (
            "drug_list_good_input.csv",
            "drug_metadata_invalid_modality_input.json",
            ValueError,
            "column 'modality'.*one_of",
        ),
        # combined_with name is present but combined_with chembl_id is empty
        # (the two combined_with columns must be populated together).
        (
            "drug_list_mismatched_combined_with_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "have a value in only one of.*combined_with_common_name",
        ),
        # combined_with linkage: combined partner DrugA is paired with both CHEMBL1
        # and CHEMBL99, so the same name maps to multiple chembl_ids.
        (
            "drug_list_bad_combined_with_linkage_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "common_name.*multiple chembl_id values",
        ),
        # Reverse combined_with linkage: combined partner CHEMBL1 is paired with both
        # DrugA and DrugX, so the same chembl_id maps to multiple names.
        (
            "drug_list_bad_reverse_combined_with_linkage_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "chembl_id.*multiple common_name values",
        ),
        # chembl_id does not match the required CHEMBL prefix (fails matches_regex).
        (
            "drug_list_invalid_chembl_id_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "column 'chembl_id'.*matches_regex",
        ),
        # combined_with_chembl_id is present but malformed (fails matches_regex);
        # null combined_with_chembl_id values are still allowed.
        (
            "drug_list_invalid_combined_with_chembl_id_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "column 'combined_with_chembl_id'.*matches_regex",
        ),
        # drug_metadata has a clinical trial phase outside the allowed set.
        (
            "drug_list_good_input.csv",
            "drug_metadata_invalid_phase_input.json",
            ValueError,
            "column 'maximum_clinical_trial_phase'.*one_of",
        ),
        # drug_metadata has a duplicate chembl_id, breaking the m:1 merge.
        (
            "drug_list_good_input.csv",
            "drug_metadata_duplicate_chembl_id_input.json",
            pd.errors.MergeError,
            "Merge keys are not unique",
        ),
        # Cross-field conflict: DrugA maps to CHEMBL1 as a primary drug but to
        # CHEMBL999 as a combined_with partner, so the stacked name/id mapping for
        # DrugA is inconsistent.
        (
            "drug_list_cross_field_combined_with_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "common_name.*multiple chembl_id values",
        ),
        # contact_pi is empty in drug_list (fails the not_empty rule).
        (
            "drug_list_empty_contact_pi_input.csv",
            "drug_metadata_good_input.json",
            ValueError,
            "column 'contact_pi'.*not_empty",
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
        "Fail with malformed combined_with_chembl_id",
        "Fail with invalid maximum_clinical_trial_phase in drug_metadata",
        "Fail with duplicate chembl_id in drug_metadata",
        "Fail with cross-field common_name to chembl_id conflict in combined_with",
        "Fail with empty contact_pi in drug_list",
    ]

    @staticmethod
    def _load_datasets(
        drug_list_file: str, drug_metadata_file: str | None = None
    ) -> dict[str, pd.DataFrame]:
        datasets = {
            "drug_list": pd.read_csv(
                os.path.join(
                    TestTransformNominatedDrugs.data_files_path, "input", drug_list_file
                )
            )
        }
        if drug_metadata_file is not None:
            datasets["drug_metadata"] = pd.read_json(
                os.path.join(
                    TestTransformNominatedDrugs.data_files_path,
                    "input",
                    drug_metadata_file,
                )
            )
        return datasets

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
        "drug_list_file, drug_metadata_file, error_type, error_match",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_nominated_drugs_should_fail(
        self,
        drug_list_file: str,
        drug_metadata_file: str | None,
        error_type: type[BaseException],
        error_match: str,
    ) -> None:
        datasets = self._load_datasets(drug_list_file, drug_metadata_file)
        with pytest.raises(error_type, match=error_match):
            nominated_drugs.transform_nominated_drugs(datasets=datasets)
