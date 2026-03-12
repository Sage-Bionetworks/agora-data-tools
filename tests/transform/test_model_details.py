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

    def test_jax_id_leading_zeros_preservation(self):
        """
        Test that jax_id values are properly formatted with leading zeros to ensure 6-digit format.
        This tests the specific functionality in transform_model_details that handles jax_id formatting.
        """
        # Create test datasets with various jax_id formats
        model_info_df = pd.DataFrame(
            {
                "name": ["model1", "model2", "model3", "model4", "model5", "model6"],
                "matched_controls": [
                    "control1",
                    "control2",
                    "control3",
                    "control4",
                    "control5",
                    "control6",
                ],
                "model_type": ["type1", "type2", "type3", "type4", "type5", "type6"],
                "contributing_group": [
                    "group1",
                    "group2",
                    "group3",
                    "group4",
                    "group5",
                    "group6",
                ],
                "study_synid": ["syn1", "syn2", "syn3", "syn4", "syn5", "syn6"],
                "rrid": ["rrid1", "rrid2", "rrid3", "rrid4", "rrid5", "rrid6"],
                "jax_id": [
                    123,
                    "456",
                    7890,
                    "123456",
                    "",
                    None,
                ],  # Various formats to test
                "alzforum_id": ["alz1", "alz2", "alz3", "alz4", "alz5", "alz6"],
                "genotype": ["geno1", "geno2", "geno3", "geno4", "geno5", "geno6"],
                "aliases": ["alias1", "alias2", "alias3", "alias4", "alias5", "alias6"],
                "url_categories_value": [""] * 6,
                "url_models_value": [""] * 6,
            }
        )

        # Create minimal required datasets for the transform function
        allele_info_df = pd.DataFrame(
            {
                "name": ["model1", "model2", "model3", "model4", "model5", "model6"],
                "modified_gene": ["gene1", "gene2", "gene3", "gene4", "gene5", "gene6"],
                "gene_ensembl_id": ["ens1", "ens2", "ens3", "ens4", "ens5", "ens6"],
                "allele": [
                    "allele1",
                    "allele2",
                    "allele3",
                    "allele4",
                    "allele5",
                    "allele6",
                ],
                "allele_type": ["type1", "type2", "type3", "type4", "type5", "type6"],
                "mgi_allele_id": [1, 2, 3, 4, 5, 6],
            }
        )

        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": pd.Series(dtype="object"),
                "gene_symbol": pd.Series(dtype="object"),
                "human_ensembl_id": pd.Series(dtype="object"),
            }
        )

        biomarkers_df = pd.DataFrame(
            {
                "name": pd.Series(dtype="object"),
                "evidence_type": pd.Series(dtype="object"),
                "value": pd.Series(dtype="object"),
                "units": pd.Series(dtype="object"),
                "age": pd.Series(dtype="object"),
                "tissue": pd.Series(dtype="object"),
                "sex": pd.Series(dtype="object"),
                "genotype": pd.Series(dtype="object"),
                "individual_id": pd.Series(dtype="object"),
            }
        )

        pathology_df = pd.DataFrame(
            {
                "name": pd.Series(dtype="object"),
                "evidence_type": pd.Series(dtype="object"),
                "value": pd.Series(dtype="object"),
                "units": pd.Series(dtype="object"),
                "age": pd.Series(dtype="object"),
                "tissue": pd.Series(dtype="object"),
                "sex": pd.Series(dtype="object"),
                "genotype": pd.Series(dtype="object"),
                "individual_id": pd.Series(dtype="object"),
            }
        )

        model_results_info_df = pd.DataFrame(
            {
                "name": pd.Series(dtype="object"),
                "gene_expression": pd.Series(dtype="object"),
                "disease_correlation": pd.Series(dtype="object"),
                "pathology": pd.Series(dtype="object"),
                "biomarkers": pd.Series(dtype="object"),
            }
        )

        datasets = {
            "model_info": model_info_df,
            "allele_info": allele_info_df,
            "human_transgene_allele_map": human_transgene_allele_map_df,
            "biomarkers": biomarkers_df,
            "pathology": pathology_df,
            "model_results_info": model_results_info_df,
            "immunohisto_measure_order": _load_test_measure_order_config(),
        }

        # Transform data
        output = transform_model_details(datasets=datasets)

        # Check that jax_id values are properly formatted
        expected_jax_ids = ["000123", "000456", "007890", "123456", "", ""]

        for i, model_entry in enumerate(output):
            assert (
                model_entry["jax_id"] == expected_jax_ids[i]
            ), f"Expected jax_id '{expected_jax_ids[i]}' for model{i+1}, but got '{model_entry['jax_id']}'"

        # Additional test: verify that the original DataFrame was not modified
        # (the function should work on a copy)
        assert (
            model_info_df["jax_id"].iloc[0] == 123
        ), "Original DataFrame should not be modified"
