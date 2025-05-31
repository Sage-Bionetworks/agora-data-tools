import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_details import (
    transform_model_details,
    prepare_biomarker_pathology,
    process_biomarker_pathology,
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
        (
            # Pass with missing data in allele_info
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "human_transgene_allele_map": "model_details_human_transgene_allele_map_good_test_input.csv",
                "allele_info": "model_details_allele_info_missing_data_input.csv",
                "model_info": "model_details_model_info_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
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

    def test_prepare_biomarker_pathology_should_pass(self):
        # Create test input DataFrame
        input_df = pd.DataFrame(
            {
                "sex": ["male", "female"],
                "tissue": ["cerebral cortex", "hippocampus"],
                "type": ["beta amyloid", "beta amyloid"],
                "measurement": [1.0, 2.0],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", "Female"],
                "tissue": ["Cerebral Cortex", "Hippocampus"],
                "evidence_type": ["&beta; amyloid", "&beta; amyloid"],
                "value": [1.0, 2.0],
            }
        )

        # Transform data
        output_df = prepare_biomarker_pathology(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_prepare_biomarker_pathology_with_empty_values(self):
        # Create test input DataFrame with empty values
        input_df = pd.DataFrame(
            {
                "sex": ["male", ""],
                "tissue": ["cerebral cortex", ""],
                "type": ["beta amyloid", ""],
                "measurement": [1.0, 2.0],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", ""],
                "tissue": ["Cerebral Cortex", ""],
                "evidence_type": ["&beta; amyloid", ""],
                "value": [1.0, 2.0],
            }
        )

        # Transform data
        output_df = prepare_biomarker_pathology(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_prepare_biomarker_pathology_with_none_values(self):
        # Create test input DataFrame with None values
        input_df = pd.DataFrame(
            {
                "sex": ["male", None],
                "tissue": ["cerebral cortex", None],
                "type": ["beta amyloid", None],
                "measurement": [1.0, 2.0],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", ""],
                "tissue": ["Cerebral Cortex", ""],
                "evidence_type": ["&beta; amyloid", ""],
                "value": [1.0, 2.0],
            }
        )

        # Transform data
        output_df = prepare_biomarker_pathology(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_process_biomarker_pathology_should_pass(self):
        # Create test input DataFrame
        input_df = pd.DataFrame(
            {
                "model": ["model1", "model1", "model2"],
                "evidence_type": ["type1", "type1", "type2"],
                "tissue": ["tissue1", "tissue1", "tissue2"],
                "age_death": [12, 12, 24],
                "units": ["unit1", "unit1", "unit2"],
                "genotype": ["geno1", "geno2", "geno3"],
                "sex": ["Male", "Female", "Male"],
                "individual_id": ["ind1", "ind2", "ind3"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        # Expected output for model1
        expected_output = [
            {
                "model": "model1",
                "evidence_type": "type1",
                "tissue": "tissue1",
                "age": "12 months",
                "units": "unit1",
                "data": [
                    {
                        "genotype": "geno1",
                        "sex": "Male",
                        "individual_id": "ind1",
                        "value": 1.0,
                    },
                    {
                        "genotype": "geno2",
                        "sex": "Female",
                        "individual_id": "ind2",
                        "value": 2.0,
                    },
                ],
            }
        ]

        # Transform data
        output = process_biomarker_pathology(input_df, "model1")

        # Compare output with expected
        assert output == expected_output

    def test_process_biomarker_pathology_with_empty_data(self):
        # Create test input DataFrame with no data for the model
        input_df = pd.DataFrame(
            {
                "model": ["model2"],
                "evidence_type": ["type2"],
                "tissue": ["tissue2"],
                "age_death": [24],
                "units": ["unit2"],
                "genotype": ["geno3"],
                "sex": ["Male"],
                "individual_id": ["ind3"],
                "value": [3.0],
            }
        )

        # Expected output for model1 (should be empty list since no data for model1)
        expected_output = []

        # Transform data
        output = process_biomarker_pathology(input_df, "model1")

        # Compare output with expected
        assert output == expected_output

    def test_process_biomarker_pathology_with_multiple_groups(self):
        # Create test input DataFrame with multiple groups
        input_df = pd.DataFrame(
            {
                "model": ["model1", "model1", "model1"],
                "evidence_type": ["type1", "type1", "type2"],
                "tissue": ["tissue1", "tissue1", "tissue2"],
                "age_death": [12, 12, 24],
                "units": ["unit1", "unit1", "unit2"],
                "genotype": ["geno1", "geno2", "geno3"],
                "sex": ["Male", "Female", "Male"],
                "individual_id": ["ind1", "ind2", "ind3"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        # Expected output for model1
        expected_output = [
            {
                "model": "model1",
                "evidence_type": "type1",
                "tissue": "tissue1",
                "age": "12 months",
                "units": "unit1",
                "data": [
                    {
                        "genotype": "geno1",
                        "sex": "Male",
                        "individual_id": "ind1",
                        "value": 1.0,
                    },
                    {
                        "genotype": "geno2",
                        "sex": "Female",
                        "individual_id": "ind2",
                        "value": 2.0,
                    },
                ],
            },
            {
                "model": "model1",
                "evidence_type": "type2",
                "tissue": "tissue2",
                "age": "24 months",
                "units": "unit2",
                "data": [
                    {
                        "genotype": "geno3",
                        "sex": "Male",
                        "individual_id": "ind3",
                        "value": 3.0,
                    }
                ],
            },
        ]

        # Transform data
        output = process_biomarker_pathology(input_df, "model1")

        # Compare output with expected
        assert output == expected_output

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
                "modified_gene": "App",
                "ensembl_gene_id": "ENSG00000123456",
                "allele": "APP Example Allele",
                "allele_type": "Transgenic",
                "mgi_allele_id": 1234567,
            },
            {
                "modified_gene": "Mapt",
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
