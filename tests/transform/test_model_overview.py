import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_overview import transform_model_overview


class TestTransformModelOverview:
    data_files_path = "tests/test_assets/model_overview"
    pass_test_data = [
        (
            # Pass with good test data
            {
                "model_info": "model_overview_model_info_good_test_input.csv",
                "model_results_info": "model_overview_model_results_info_good_test_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            "model_overview_transform_good_test_output.json",
        ),
        (
            # Pass with missing data in some fields
            {
                "model_info": "model_overview_model_info_missing_data_input.csv",
                "model_results_info": "model_overview_model_results_info_missing_data_input.csv",
                "allele_info": "model_overview_allele_info_missing_data_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            "model_overview_transform_missing_data_output.json",
        ),
        (
            # Pass with no results data for some models
            {
                "model_info": "model_overview_model_info_good_test_input.csv",
                "model_results_info": "model_overview_model_results_info_no_results_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            "model_overview_transform_no_results_output.json",
        ),
        (
            # Pass with extra columns
            {
                "model_info": "model_overview_model_info_extra_column_input.csv",
                "model_results_info": "model_overview_model_results_info_extra_column_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            "model_overview_transform_extra_column_output.json",
        ),
        (
            # Pass with missing models in different source files
            {
                "model_info": "model_overview_model_info_missing_models_test.csv",
                "model_results_info": "model_overview_model_results_info_missing_models_test.csv",
                "allele_info": "model_overview_allele_info_missing_models_test.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_missing_models_test.csv",
            },
            "model_overview_transform_missing_models_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good test data",
        "Pass with missing data in some fields",
        "Pass with no results data for some models",
        "Pass with extra columns",
        "Pass with missing models in different source files",
    ]
    fail_test_data = [
        (
            # Fail with missing model_info dataset
            {
                "model_results_info": "model_overview_model_results_info_good_test_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing model_results_info dataset
            {
                "model_info": "model_overview_model_info_good_test_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_info
            {
                "model_info": "model_overview_model_info_missing_column_input.csv",
                "model_results_info": "model_overview_model_results_info_good_test_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_results_info
            {
                "model_info": "model_overview_model_info_good_test_input.csv",
                "model_results_info": "model_overview_model_results_info_missing_column_input.csv",
                "allele_info": "model_overview_allele_info_good_test_input.csv",
                "human_transgene_allele_map": "model_overview_human_transgene_allele_map_good_test_input.csv",
            },
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with missing model_info dataset",
        "Fail with missing model_results_info dataset",
        "Fail with missing required columns in model_info",
        "Fail with missing required columns in model_results_info",
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
        empty_model_info = pd.DataFrame(
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
            ]
        )
        empty_model_results_info = pd.DataFrame(
            columns=[
                "name",
                "gene_expression",
                "disease_correlation",
                "pathology",
                "biomarkers",
            ]
        )
        empty_allele_info = pd.DataFrame(
            columns=[
                "name",
                "modified_gene",
                "mgi_gene_id",
                "gene_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
            ]
        )
        empty_human_transgene_allele_map = pd.DataFrame(
            columns=[
                "mgi_allele_id",
                "gene_symbol",
                "human_ensembl_id",
            ]
        )

        datasets = {
            "model_info": empty_model_info,
            "model_results_info": empty_model_results_info,
            "allele_info": empty_allele_info,
            "human_transgene_allele_map": empty_human_transgene_allele_map,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output - empty list since no models to process
        expected_output = []

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_link_generation(self):
        # Create test datasets with specific boolean values
        model_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "matched_controls": ["C57BL6J"],
                "model_type": ["Familial AD"],
                "contributing_group": ["Test Center"],
                "study_synid": ["syn123456"],
                "rrid": ["IMSR_JAX:123456"],
                "jax_id": [123456],
                "alzforum_id": ["test"],
                "genotype": ["Test Genotype"],
                "aliases": ["Test Alias"],
            }
        )
        model_results_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "gene_expression": [True],
                "disease_correlation": [False],
                "pathology": [True],
                "biomarkers": [False],
            }
        )
        allele_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "modified_gene": ["TestGene"],
                "mgi_gene_id": [12345],
                "gene_ensembl_id": ["ENSMUSG00000012345"],
                "allele": ["TestAllele"],
                "allele_type": ["Transgenic"],
                "mgi_allele_id": [67890],
            }
        )
        human_transgene_allele_map = pd.DataFrame(
            {
                "mgi_allele_id": [67890],
                "gene_symbol": ["TestGene"],
                "human_ensembl_id": ["ENSG00000012345"],
            }
        )

        datasets = {
            "model_info": model_info,
            "model_results_info": model_results_info,
            "allele_info": allele_info,
            "human_transgene_allele_map": human_transgene_allele_map,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output
        expected_output = [
            {
                "name": "test_model",
                "model_type": "Familial AD",
                "matched_controls": "C57BL6J",
                "gene_expression": {
                    "link_url": "comparison/expression?model=test_model"
                },
                "disease_correlation": None,
                "pathology": {"link_url": "models/test_model/pathology"},
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn123456"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/123456"},
                "center": {"link_text": "Test Center"},
                "modified_genes": ["TestGene"],
                "available_data": ["Gene Expression", "Pathology"],
            }
        ]

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_with_none_values(self):
        # Create test datasets with None values
        model_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "matched_controls": [None],
                "model_type": ["Familial AD"],
                "contributing_group": ["Test Center"],
                "study_synid": ["syn123456"],
                "rrid": ["IMSR_JAX:123456"],
                "jax_id": [123456],
                "alzforum_id": [None],
                "genotype": ["Test Genotype"],
                "aliases": [None],
            }
        )
        model_results_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "gene_expression": [None],
                "disease_correlation": [None],
                "pathology": [None],
                "biomarkers": [None],
            }
        )
        allele_info = pd.DataFrame(
            {
                "name": ["test_model"],
                "modified_gene": [None],
                "mgi_gene_id": [None],
                "gene_ensembl_id": [None],
                "allele": [None],
                "allele_type": [None],
                "mgi_allele_id": [None],
            }
        )
        human_transgene_allele_map = pd.DataFrame(
            {
                "mgi_allele_id": [None],
                "gene_symbol": [None],
                "human_ensembl_id": [None],
            }
        )

        datasets = {
            "model_info": model_info,
            "model_results_info": model_results_info,
            "allele_info": allele_info,
            "human_transgene_allele_map": human_transgene_allele_map,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output - None values should be preserved
        expected_output = [
            {
                "name": "test_model",
                "model_type": "Familial AD",
                "matched_controls": None,
                "gene_expression": None,
                "disease_correlation": None,
                "pathology": None,
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn123456"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/123456"},
                "center": {"link_text": "Test Center"},
                "modified_genes": [],
                "available_data": [],
            }
        ]

        # Compare output with expected
        assert output_data == expected_output

    def test_model_overview_transform_multiple_models(self):
        # Create test datasets with multiple models
        model_info = pd.DataFrame(
            {
                "name": ["model1", "model2"],
                "matched_controls": ["C57BL6J", "B6129"],
                "model_type": ["Familial AD", "Tauopathy"],
                "contributing_group": ["Center1", "Center2"],
                "study_synid": ["syn111", "syn222"],
                "rrid": ["IMSR_JAX:111", "IMSR_JAX:222"],
                "jax_id": [111, 222],
                "alzforum_id": ["id1", "id2"],
                "genotype": ["Geno1", "Geno2"],
                "aliases": ["Alias1", "Alias2"],
            }
        )
        model_results_info = pd.DataFrame(
            {
                "name": ["model1", "model2"],
                "gene_expression": [True, False],
                "disease_correlation": [False, True],
                "pathology": [True, True],
                "biomarkers": [False, True],
            }
        )
        allele_info = pd.DataFrame(
            {
                "name": ["model1", "model1", "model2"],
                "modified_gene": ["Gene1", "Gene2", "Gene3"],
                "mgi_gene_id": [11111, 22222, 33333],
                "gene_ensembl_id": [
                    "ENSMUSG00000011111",
                    "ENSMUSG00000022222",
                    "ENSMUSG00000033333",
                ],
                "allele": ["Allele1", "Allele2", "Allele3"],
                "allele_type": ["Transgenic", "Targeted", "Transgenic"],
                "mgi_allele_id": [111111, 222222, 333333],
            }
        )
        human_transgene_allele_map = pd.DataFrame(
            {
                "mgi_allele_id": [111111, 222222, 333333],
                "gene_symbol": ["Gene1", "Gene2", "Gene3"],
                "human_ensembl_id": [
                    "ENSG00000011111",
                    "ENSG00000022222",
                    "ENSG00000033333",
                ],
            }
        )

        datasets = {
            "model_info": model_info,
            "model_results_info": model_results_info,
            "allele_info": allele_info,
            "human_transgene_allele_map": human_transgene_allele_map,
        }

        # Transform data
        output_data = transform_model_overview(datasets=datasets)

        # Expected output
        expected_output = [
            {
                "name": "model1",
                "model_type": "Familial AD",
                "matched_controls": "C57BL6J",
                "gene_expression": {"link_url": "comparison/expression?model=model1"},
                "disease_correlation": None,
                "pathology": {"link_url": "models/model1/pathology"},
                "biomarkers": None,
                "study_data": {
                    "link_url": "https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn111"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/111"},
                "center": {"link_text": "Center1"},
                "modified_genes": ["Gene1", "Gene2"],
                "available_data": ["Gene Expression", "Pathology"],
            },
            {
                "name": "model2",
                "model_type": "Tauopathy",
                "matched_controls": "B6129",
                "gene_expression": None,
                "disease_correlation": {
                    "link_url": "comparison/correlation?model=model2"
                },
                "pathology": {"link_url": "models/model2/pathology"},
                "biomarkers": {"link_url": "models/model2/biomarkers"},
                "study_data": {
                    "link_url": "https://adknowledgeportal.org/Explore/Studies/DetailsPage/StudyDetails?Study=syn222"
                },
                "jax_strain": {"link_url": "https://jax.org/strain/222"},
                "center": {"link_text": "Center2"},
                "modified_genes": ["Gene3"],
                "available_data": ["Disease Correlation", "Pathology", "Biomarkers"],
            },
        ]

        # Compare output with expected
        assert output_data == expected_output

        # INSERT_YOUR_CODE


class TestGetListOfAvailableData:
    def test_all_data_present(self):
        from agoradatatools.etl.transform.model_overview import (
            get_list_of_available_data,
        )

        model = {
            "gene_expression": {"link_url": "url1"},
            "disease_correlation": {"link_url": "url2"},
            "pathology": {"link_url": "url3"},
            "biomarkers": {"link_url": "url4"},
        }
        result = get_list_of_available_data(model)
        assert set(result) == {
            "Gene Expression",
            "Disease Correlation",
            "Pathology",
            "Biomarkers",
        }

    def test_some_data_missing(self):
        from agoradatatools.etl.transform.model_overview import (
            get_list_of_available_data,
        )

        model = {
            "gene_expression": {"link_url": "url1"},
            "disease_correlation": None,
            "pathology": {"link_url": "url3"},
            "biomarkers": None,
        }
        result = get_list_of_available_data(model)
        assert set(result) == {"Gene Expression", "Pathology"}

    def test_all_data_missing(self):
        from agoradatatools.etl.transform.model_overview import (
            get_list_of_available_data,
        )

        model = {
            "gene_expression": None,
            "disease_correlation": None,
            "pathology": None,
            "biomarkers": None,
        }
        result = get_list_of_available_data(model)
        assert result == []

    def test_empty_model(self):
        from agoradatatools.etl.transform.model_overview import (
            get_list_of_available_data,
        )

        model = {}
        result = get_list_of_available_data(model)
        assert result == []

    def test_partial_keys(self):
        from agoradatatools.etl.transform.model_overview import (
            get_list_of_available_data,
        )

        model = {
            "gene_expression": {"link_url": "url1"},
            # disease_correlation missing
            "pathology": None,
            # biomarkers missing
        }
        result = get_list_of_available_data(model)
        assert result == ["Gene Expression"]
