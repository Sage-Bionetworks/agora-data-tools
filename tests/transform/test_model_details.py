import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_details import transform_model_details


def _load_test_measure_order_config():
    """Load the test measure order config from test assets as a DataFrame."""
    from agoradatatools.etl.extract import read_yaml_into_df

    config_path = os.path.join(
        "tests/test_assets/model_details/input",
        "immunohisto_measure_order.yaml",
    )

    config_df = read_yaml_into_df(config_path)
    # Rename generic columns from read_yaml_into_df to expected names
    config_df = config_df.rename(
        columns={"key": "dataset_name", "items": "evidence_type"}
    )
    return config_df


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
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            "model_details_transform_good_test_output.json",
        ),
        (
            # Pass with good test data requiring special URLs for gene expression
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_url_test_good_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_url_test_input.csv",
            },
            "model_details_transform_url_test_good_output.json",
        ),
        (
            # Pass with missing data in some fields
            {
                "biomarkers": "model_details_biomarkers_missing_data_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_missing_data_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_2.csv",
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
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
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
                "model_results_info": "model_details_model_results_info_good_test_input_3.csv",
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
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            "model_details_transform_no_human_match_output.json",
        ),
        (
            # Pass with missing data in allele_info
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_missing_data_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            "model_details_transform_missing_allele_info_output.json",
        ),
        (
            # Pass with missing models in different source files
            {
                "biomarkers": "model_details_biomarkers_missing_models_test.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_missing_models_test.csv",
                "allele_info": "model_details_allele_info_missing_models_test.csv",
                "model_info": "model_details_model_info_missing_models_test.csv",
                "pathology": "model_details_pathology_missing_models_test.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            "model_details_transform_missing_models_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good test data",
        "Pass with good test data requiring special URLs for gene expression link",
        "Pass with missing data in some fields",
        "Pass with empty biomarkers and pathology",
        "Pass with extra columns",
        "Pass with no matching human transgene alleles",
        "Pass with missing data in allele_info",
        "Pass with missing models in different source files",
    ]
    fail_test_data = [
        (
            # Fail with missing biomarkers dataset
            {
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in biomarkers
            {
                "biomarkers": "model_details_biomarkers_missing_column_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_info
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_good_test_input.csv",
                "model_info": "model_details_model_info_missing_column_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
                "model_results_info": "model_details_model_results_info_good_test_input_1.csv",
            },
            ValueError,
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

        # Add measure order config
        datasets["immunohisto_measure_order"] = _load_test_measure_order_config()

        # Transform data
        output_data = transform_model_details(datasets=datasets)

        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

        # Sort biomarkers and pathology by age to make comparison deterministic
        for model in output_data:
            model["biomarkers"] = sorted(
                model["biomarkers"],
                key=lambda x: (x["name"], x["evidence_type"], x["tissue"], x["age"]),
            )
            model["pathology"] = sorted(
                model["pathology"],
                key=lambda x: (x["name"], x["evidence_type"], x["tissue"], x["age"]),
            )

        for model in expected_data:
            model["biomarkers"] = sorted(
                model["biomarkers"],
                key=lambda x: (x["name"], x["evidence_type"], x["tissue"], x["age"]),
            )
            model["pathology"] = sorted(
                model["pathology"],
                key=lambda x: (x["name"], x["evidence_type"], x["tissue"], x["age"]),
            )

        # Compare output with expected
        assert output_data == expected_data

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

        # Add measure order config
        datasets["immunohisto_measure_order"] = _load_test_measure_order_config()

        # Expect transformation to raise the specified error
        with pytest.raises(error_type):
            transform_model_details(datasets=datasets)
