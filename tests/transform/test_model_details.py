import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_details import transform_model_details


class TestTransformModelDetails:
    data_files_path = "tests/test_assets/model_details"
    pass_test_data = [
        (
            # Pass with good test data
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_good_test_output.json",
        ),
        (
            # Pass with missing data in some fields
            {
                "biomarkers": "model_details_biomarkers_missing_data_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_missing_data_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_missing_data_output.json",
        ),
        (
            # Pass with empty biomarkers and pathology
            {
                "biomarkers": "model_details_biomarkers_empty_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_empty_input.csv",
            },
            "model_details_transform_empty_measurements_output.json",
        ),
        (
            # Pass with extra columns
            {
                "biomarkers": "model_details_biomarkers_extra_column_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_extra_column_output.json",
        ),
        (
            # Pass with no matching human transgene alleles
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_no_match_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_no_human_match_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good test data",
        "Pass with missing data in some fields",
        "Pass with empty biomarkers and pathology",
        "Pass with extra columns",
        "Pass with no matching human transgene alleles",
    ]
    fail_test_data = [
        (
            # Fail with missing biomarkers dataset
            {
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            KeyError,
        ),
        (
            # Fail with missing required columns in biomarkers
            {
                "biomarkers": "model_details_biomarkers_missing_column_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            KeyError,
        ),
        (
            # Fail with missing required columns in model_info
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_missing_column_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            KeyError,
        ),
    ]
    fail_test_ids = [
        "Fail with missing biomarkers dataset",
        "Fail with missing required columns in biomarkers",
        "Fail with missing required columns in model_info",
    ]

    @pytest.mark.parametrize(
        "input_files, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_model_details_transform_should_pass(
        self, input_files, expected_output_file
    ):
        # Create datasets dictionary
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Transform data
        output_data = transform_model_details(datasets=datasets)
        output_df = pd.DataFrame(output_data)

        # Load expected output
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_files, error_type",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_model_details_transform_should_fail(self, input_files, error_type):
        # Create datasets dictionary
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Expect transformation to raise the specified error
        with pytest.raises(error_type):
            transform_model_details(datasets=datasets)
