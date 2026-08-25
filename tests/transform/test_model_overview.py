import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_overview import (
    get_list_of_available_data,
    get_center_link_url,
    transform_model_overview,
)


class TestTransformModelOverview:
    data_files_path = "tests/test_assets/model_overview"
    pass_test_data = [
        (
            # Pass with good test data
            {
                "model_metadata": "model_overview_model_metadata_good_test_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            "model_overview_transform_good_test_output.json",
        ),
        (
            # Pass with good test data requiring special URLs for gene expression
            {
                "model_metadata": "model_overview_model_metadata_url_test_good_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            "model_overview_transform_url_test_good_output.json",
        ),
        (
            # Pass with missing data in some fields
            {
                "model_metadata": "model_overview_model_metadata_missing_data_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_missing_data_input.csv",
            },
            "model_overview_transform_missing_data_output.json",
        ),
        (
            # Pass with no results data for some models
            {
                "model_metadata": "model_overview_model_metadata_no_results_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            "model_overview_transform_no_results_output.json",
        ),
        (
            # Pass with extra columns
            {
                "model_metadata": "model_overview_model_metadata_extra_column_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            "model_overview_transform_extra_column_output.json",
        ),
        (
            # Pass with missing models in different source files
            {
                "model_metadata": "model_overview_model_metadata_missing_models_test.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_missing_models_test.csv",
            },
            "model_overview_transform_missing_models_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good test data",
        "Pass with good test data requiring special URLs for gene expression",
        "Pass with missing data in some fields",
        "Pass with no results data for some models",
        "Pass with extra columns",
        "Pass with missing models in different source files",
    ]
    fail_test_data = [
        (
            # Fail with missing model_metadata dataset
            {
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_metadata
            {
                "model_metadata": "model_overview_model_metadata_missing_column_input.csv",
                "model_genetic_modifications": "model_overview_model_genetic_modifications_good_test_input.csv",
            },
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with missing model_metadata dataset",
        "Fail with missing required columns in model_metadata",
    ]

    @pytest.mark.parametrize(
        "input_files, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_model_overview_transform_should_pass(
        self, input_files, expected_output_file
    ):
        # Create datasets dictionary
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

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
    def test_model_overview_transform_should_fail(self, input_files, error_type):
        # Create datasets dictionary
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Expect transformation to raise the specified error
        with pytest.raises(error_type):
            transform_model_overview(datasets=datasets)

    def test_model_overview_transform_with_empty_datasets(self):
        # Create empty test datasets
        empty_model_metadata = pd.DataFrame(
            columns=[
                "name",
                "matched_controls",
                "model_type",
                "contributing_group",
                "study_synid",
                "rrid",
                "jax_id",
                "alzforum_id",
                "genotype",
                "aliases",
                "transcriptomics_url_categories_value",
                "transcriptomics_url_models_value",
                "transcriptomics",
                "disease_correlation",
                "pathology",
                "biomarkers",
            ]
        )
        empty_model_genetic_modifications = pd.DataFrame(
            columns=[
                "name",
                "modified_gene",
                "mouse_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
                "human_gene_symbol",
                "human_ensembl_id",
            ]
        )

        datasets = {
            "model_metadata": empty_model_metadata,
            "model_genetic_modifications": empty_model_genetic_modifications,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output - empty list since no models to process
        expected_output = []

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_link_generation(self):
        # Create test datasets with specific boolean values
        model_metadata = pd.DataFrame(
            {
                "name": ["test_model"],
                "matched_controls": ["C57BL6J"],
                "model_type": ["Familial AD"],
                "contributing_group": ["UCI"],
                "study_synid": ["syn123456"],
                "rrid": ["IMSR_JAX:123456"],
                "jax_id": [123456],
                "alzforum_id": ["test"],
                "genotype": ["Test Genotype"],
                "aliases": ["Test Alias"],
                "transcriptomics_url_categories_value": [None],
                "transcriptomics_url_models_value": [None],
                "transcriptomics": [True],
                "disease_correlation": [False],
                "pathology": [True],
                "biomarkers": [False],
            }
        )
        model_genetic_modifications = pd.DataFrame(
            {
                "name": ["test_model"],
                "modified_gene": ["TestGene"],
                "mgi_gene_id": [12345],
                "mouse_ensembl_id": ["ENSMUSG00000012345"],
                "allele": ["TestAllele"],
                "allele_type": ["Transgenic"],
                "mgi_allele_id": [67890],
                "human_gene_symbol": ["TestGene"],
                "human_ensembl_id": ["ENSG00000012345"],
            }
        )

        datasets = {
            "model_metadata": model_metadata,
            "model_genetic_modifications": model_genetic_modifications,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output
        expected_output = [
            {
                "name": "test_model",
                "model_type": "Familial AD",
                "matched_controls": ["C57BL6J"],
                "transcriptomics": {
                    "link_url": "comparison/expression?models=test_model"
                },
                "disease_correlation": None,
                "pathology": {"link_url": "models/test_model/pathology"},
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.synapse.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn123456"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/123456"},
                "center": "UCI",
                "modified_genes": ["TestGene"],
                "available_data": ["Transcriptomics", "Pathology"],
            }
        ]

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_with_none_values(self):
        # Create test datasets with None values
        model_metadata = pd.DataFrame(
            {
                "name": ["test_model"],
                "matched_controls": [None],
                "model_type": ["Familial AD"],
                "contributing_group": ["IU/Jax/Pitt"],
                "study_synid": ["syn123456"],
                "rrid": ["IMSR_JAX:123456"],
                "jax_id": [123456],
                "alzforum_id": [None],
                "genotype": ["Test Genotype"],
                "aliases": [None],
                "transcriptomics_url_categories_value": [None],
                "transcriptomics_url_models_value": [None],
                "transcriptomics": [None],
                "disease_correlation": [None],
                "pathology": [None],
                "biomarkers": [None],
            }
        )
        model_genetic_modifications = pd.DataFrame(
            {
                "name": ["test_model"],
                "modified_gene": [None],
                "mgi_gene_id": [None],
                "mouse_ensembl_id": [None],
                "allele": [None],
                "allele_type": [None],
                "mgi_allele_id": [None],
                "human_gene_symbol": [None],
                "human_ensembl_id": [None],
            }
        )

        datasets = {
            "model_metadata": model_metadata,
            "model_genetic_modifications": model_genetic_modifications,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output - None values should be preserved
        expected_output = [
            {
                "name": "test_model",
                "model_type": "Familial AD",
                "matched_controls": [],
                "transcriptomics": None,
                "disease_correlation": None,
                "pathology": None,
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.synapse.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn123456"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/123456"},
                "center": "IU/Jax/Pitt",
                "modified_genes": [],
                "available_data": [],
            }
        ]

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_multiple_models(self):
        # Create test datasets with multiple models
        # A column where one row has a single control and one has two.
        # Testing that the "B6129,B6130" entry is properly split into a list in the JSON output.
        model_metadata = pd.DataFrame(
            {
                "name": ["model1", "model2"],
                "matched_controls": ["C57BL6J", "B6129,B6130"],
                "model_type": ["Familial AD", "Tauopathy"],
                "contributing_group": ["UCI", "IU/Jax/Pitt"],
                "study_synid": ["syn111", "syn222"],
                "rrid": ["IMSR_JAX:111", "IMSR_JAX:222"],
                "jax_id": [111, 222],
                "alzforum_id": ["id1", "id2"],
                "genotype": ["Geno1", "Geno2"],
                "aliases": ["Alias1", "Alias2"],
                "transcriptomics_url_categories_value": [None, None],
                "transcriptomics_url_models_value": [None, None],
                "transcriptomics": [True, False],
                "disease_correlation": [False, True],
                "pathology": [True, True],
                "biomarkers": [False, True],
            }
        )
        model_genetic_modifications = pd.DataFrame(
            {
                "name": ["model1", "model1", "model2"],
                "modified_gene": ["Gene1", "Gene2", "Gene3"],
                "mgi_gene_id": [11111, 22222, 33333],
                "mouse_ensembl_id": [
                    "ENSMUSG00000011111",
                    "ENSMUSG00000022222",
                    "ENSMUSG00000033333",
                ],
                "allele": ["Allele1", "Allele2", "Allele3"],
                "allele_type": ["Transgenic", "Targeted", "Transgenic"],
                "mgi_allele_id": [111111, 222222, 333333],
                "human_gene_symbol": ["Gene1", "Gene2", "Gene3"],
                "human_ensembl_id": [
                    "ENSG00000011111",
                    "ENSG00000022222",
                    "ENSG00000033333",
                ],
            }
        )

        datasets = {
            "model_metadata": model_metadata,
            "model_genetic_modifications": model_genetic_modifications,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output
        expected_output = [
            {
                "name": "model1",
                "model_type": "Familial AD",
                "matched_controls": ["C57BL6J"],
                "transcriptomics": {"link_url": "comparison/expression?models=model1"},
                "disease_correlation": None,
                "pathology": {"link_url": "models/model1/pathology"},
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.synapse.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn111"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/000111"},
                "center": "UCI",
                "modified_genes": ["Gene1", "Gene2"],
                "available_data": ["Transcriptomics", "Pathology"],
            },
            {
                "name": "model2",
                "model_type": "Tauopathy",
                "matched_controls": ["B6129", "B6130"],
                "transcriptomics": None,
                "disease_correlation": {
                    "link_url": "comparison/correlation?models=model2"
                },
                "pathology": {"link_url": "models/model2/pathology"},
                "biomarkers": {"link_url": "models/model2/biomarkers"},
                "study_data": {
                    "link_url": "https://adknowledgeportal.synapse.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn222"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/000222"},
                "center": "IU/Jax/Pitt",
                "modified_genes": ["Gene3"],
                "available_data": ["Disease Correlation", "Pathology", "Biomarkers"],
            },
        ]

        # Compare output with expected
        assert output_data == expected_output


class TestGetListOfAvailableData:
    def test_all_data_present(self):

        model = {
            "transcriptomics": {"link_url": "url1"},
            "disease_correlation": {"link_url": "url2"},
            "pathology": {"link_url": "url3"},
            "biomarkers": {"link_url": "url4"},
        }
        result = get_list_of_available_data(model)
        assert set(result) == {
            "Transcriptomics",
            "Disease Correlation",
            "Pathology",
            "Biomarkers",
        }

    def test_some_data_missing(self):

        model = {
            "transcriptomics": {"link_url": "url1"},
            "disease_correlation": None,
            "pathology": {"link_url": "url3"},
            "biomarkers": None,
        }
        result = get_list_of_available_data(model)
        assert set(result) == {"Transcriptomics", "Pathology"}

        model = {
            "transcriptomics": None,
            "disease_correlation": {"link_url": "url2"},
            "pathology": None,
            "biomarkers": {"link_url": "url1"},
        }
        result = get_list_of_available_data(model)
        assert set(result) == {"Disease Correlation", "Biomarkers"}

    def test_all_data_missing(self):

        model = {
            "transcriptomics": None,
            "disease_correlation": None,
            "pathology": None,
            "biomarkers": None,
        }
        result = get_list_of_available_data(model)
        assert result == []

    def test_empty_model(self):

        model = {}
        result = get_list_of_available_data(model)
        assert result == []

    def test_partial_keys(self):

        model = {
            "transcriptomics": {"link_url": "url1"},
            # disease_correlation missing
            "pathology": None,
            # biomarkers missing
        }
        result = get_list_of_available_data(model)
        assert result == ["Transcriptomics"]


class TestGetCenterLinkUrl:
    def test_uci_contributing_group(self):

        result = get_center_link_url("UCI")
        expected = (
            "http://model-ad.org/uci-disease-model-development-and-phenotyping-dmp/"
        )
        assert result == expected

    def test_uci_contributing_group_lowercase(self):

        result = get_center_link_url("uci")
        expected = (
            "http://model-ad.org/uci-disease-model-development-and-phenotyping-dmp/"
        )
        assert result == expected

    def test_iu_jax_pitt_contributing_group_lowercase(self):

        result = get_center_link_url("IU/Jax/Pitt")
        expected = "https://www.model-ad.org/iu-jax-pitt-disease-modeling-project/"
        assert result == expected

    def test_iu_jax_pitt_uppercase_contributing_group(self):

        result = get_center_link_url("IU/JAX/PITT")
        expected = "https://www.model-ad.org/iu-jax-pitt-disease-modeling-project/"
        assert result == expected

    def test_invalid_contributing_group_raises_value_error(self):

        with pytest.raises(
            ValueError, match="Invalid contributing group: InvalidCenter"
        ):
            get_center_link_url("InvalidCenter")

    def test_empty_string_raises_value_error(self):

        with pytest.raises(ValueError, match="Invalid contributing group: "):
            get_center_link_url("")

    def test_none_contributing_group_raises_value_error(self):

        with pytest.raises(ValueError, match="Invalid contributing group: None"):
            get_center_link_url(None)

    def test_partial_matches_raise_value_error(self):

        # Test partial matches that should not work
        with pytest.raises(ValueError, match="Invalid contributing group: IU/Jax"):
            get_center_link_url("IU/Jax")

        with pytest.raises(ValueError, match="Invalid contributing group: IU/JAX"):
            get_center_link_url("IU/JAX")
