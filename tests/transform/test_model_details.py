import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_details import (
    transform_model_details,
    process_genetic_info,
)


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

        # Transform data
        output_data = transform_model_details(datasets=datasets)

        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

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


class TestProcessGeneticInfo:
    """Test class for the process_genetic_info function."""

    def test_process_genetic_info_should_pass(self):
        # Create test input DataFrames
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [2672831, 1930937],
                "gene_symbol": ["App", "Psen1"],
                "human_ensembl_id": ["ENSG00000142192", "ENSG00000080815"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt", "Psen1"],
                "gene_ensembl_id": [
                    "ENSMUSG00000022892",
                    "ENSMUSG00000018411",
                    "ENSMUSG00000019969",
                ],
                "allele": [
                    "APP K670_M671delinsNL (Swedish)",
                    "MAPT P301L",
                    "Psen1<sup>tm1Mpm</sup>",
                ],
                "allele_type": ["Transgenic", "Transgenic", "Targeted"],
                "mgi_allele_id": [2672831, 2672831, 1930937],
            }
        )

        # Expected output
        expected_output = [
            {
                "modified_gene": "App",
                "ensembl_gene_id": "ENSG00000142192",  # Human Ensembl ID
                "allele": "APP K670_M671delinsNL (Swedish)",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Mapt",
                "ensembl_gene_id": "ENSMUSG00000018411",  # Mouse Ensembl ID (no human match)
                "allele": "MAPT P301L",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Psen1",
                "ensembl_gene_id": "ENSG00000080815",  # Human Ensembl ID
                "allele": "Psen1<sup>tm1Mpm</sup>",
                "allele_type": "Targeted",
                "mgi_allele_id": 1930937,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_with_no_human_matches(self):
        # Create test input DataFrames with no matching human transgenes
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [9999999],  # Different MGI ID
                "gene_symbol": ["DifferentGene"],
                "human_ensembl_id": ["ENSG00000000000"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt", "Psen1"],
                "gene_ensembl_id": [
                    "ENSMUSG00000022892",
                    "ENSMUSG00000018411",
                    "ENSMUSG00000019969",
                ],
                "allele": [
                    "APP K670_M671delinsNL (Swedish)",
                    "MAPT P301L",
                    "Psen1<sup>tm1Mpm</sup>",
                ],
                "allele_type": ["Transgenic", "Transgenic", "Targeted"],
                "mgi_allele_id": [2672831, 2672831, 1930937],
            }
        )

        # Expected output - all should keep mouse Ensembl IDs
        expected_output = [
            {
                "modified_gene": "App",
                "ensembl_gene_id": "ENSMUSG00000022892",
                "allele": "APP K670_M671delinsNL (Swedish)",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Mapt",
                "ensembl_gene_id": "ENSMUSG00000018411",
                "allele": "MAPT P301L",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Psen1",
                "ensembl_gene_id": "ENSMUSG00000019969",
                "allele": "Psen1<sup>tm1Mpm</sup>",
                "allele_type": "Targeted",
                "mgi_allele_id": 1930937,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_with_empty_input(self):
        # Create empty test input DataFrames
        human_transgene_allele_map_df = pd.DataFrame(
            columns=["mgi_allele_id", "gene_symbol", "human_ensembl_id"]
        )
        model_alleles = pd.DataFrame(
            columns=[
                "modified_gene",
                "gene_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
            ]
        )

        # Expected output - empty list since no alleles to process
        expected_output = []

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_case_insensitive_mapping(self):
        # Create test input DataFrames with different gene casing
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [1234567, 1234567],
                "gene_symbol": ["APP", "mapt"],  # Upper and lower case in mapping
                "human_ensembl_id": ["ENSG00000123456", "ENSG00000987654"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt"],  # Title case in alleles
                "gene_ensembl_id": [
                    "ENSMUSG00000011111",
                    "ENSMUSG00000022222",
                ],
                "allele": [
                    "APP Example Allele",
                    "MAPT Example Allele",
                ],
                "allele_type": ["Transgenic", "Transgenic"],
                "mgi_allele_id": [1234567, 1234567],
            }
        )

        # Expected output: ENSG IDs should be mapped, gene names should keep original case
        expected_output = [
            {
                "modified_gene": "APP",
                "ensembl_gene_id": "ENSG00000123456",
                "allele": "APP Example Allele",
                "allele_type": "Transgenic",
                "mgi_allele_id": 1234567,
            },
            {
                "modified_gene": "mapt",
                "ensembl_gene_id": "ENSG00000987654",
                "allele": "MAPT Example Allele",
                "allele_type": "Transgenic",
                "mgi_allele_id": 1234567,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output
