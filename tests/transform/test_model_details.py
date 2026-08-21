import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.model_details import (
    nest_genetic_info,
    transform_model_details,
)


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
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_good_test_output.json",
        ),
        (
            # Pass with good test data requiring special URLs for gene expression
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_url_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_url_test_good_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_url_test_good_output.json",
        ),
        (
            # Pass with missing data in some fields
            {
                "biomarkers": "model_details_biomarkers_missing_data_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_missing_data_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_missing_data_output.json",
        ),
        (
            # Pass with empty biomarkers and pathology
            {
                "biomarkers": "model_details_biomarkers_empty_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_empty_measurements_input.csv",
                "pathology": "model_details_pathology_empty_input.csv",
            },
            "model_details_transform_empty_measurements_output.json",
        ),
        (
            # Pass with extra columns
            {
                "biomarkers": "model_details_biomarkers_extra_column_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_extra_column_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            "model_details_transform_extra_column_output.json",
        ),
        (
            # Pass with extra models in model_genetic_modifications, biomarkers, and pathology source files
            {
                "biomarkers": "model_details_biomarkers_extra_models_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_extra_models_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_extra_models_input.csv",
            },
            # Extra models not present in model_metadata are dropped in the output, so the expected output is the same
            # as the good test output
            "model_details_transform_good_test_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good test data",
        "Pass with good test data requiring special URLs for gene expression link",
        "Pass with missing data in some fields",
        "Pass with empty biomarkers and pathology",
        "Pass with extra columns",
        "Pass with extra models in model_genetic_modifications, biomarkers, and pathology source files",
    ]
    fail_test_data = [
        (
            # Fail with missing biomarkers dataset
            {
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in biomarkers
            {
                "biomarkers": "model_details_biomarkers_missing_column_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_metadata
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_missing_column_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in model_genetic_modifications
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_missing_column_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_good_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            ValueError,
        ),
        (
            # Fail with missing required columns in mouse_gene_metadata
            {
                "biomarkers": "model_details_biomarkers_good_test_input.csv",
                "model_genetic_modifications": "model_details_model_genetic_modifications_good_test_input.csv",
                "mouse_gene_metadata": "model_details_mouse_gene_metadata_missing_column_input.json",
                "model_metadata": "model_details_model_metadata_good_test_input.csv",
                "pathology": "model_details_pathology_good_test_input.csv",
            },
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with missing biomarkers dataset",
        "Fail with missing required columns in biomarkers",
        "Fail with missing required columns in model_metadata",
        "Fail with missing required columns in model_genetic_modifications",
        "Fail with missing required columns in mouse_gene_metadata",
    ]

    def _load_data(self, input_files):
        """
        Helper function to load input datasets from CSV or JSON files.
        """

        datasets = {}
        for dataset_name, file_name in input_files.items():
            full_path = os.path.join(self.data_files_path, "input", file_name)
            if file_name.endswith(".json"):
                datasets[dataset_name] = pd.read_json(full_path)
            else:
                datasets[dataset_name] = pd.read_csv(full_path)
        return datasets

    @pytest.mark.parametrize(
        "input_files, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_model_details_transform_should_pass(
        self, input_files, expected_output_file
    ):
        # Create datasets dictionary
        datasets = self._load_data(input_files)

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
        datasets = self._load_data(input_files)

        # Add measure order config
        datasets["immunohisto_measure_order"] = _load_test_measure_order_config()

        # Expect transformation to raise the specified error
        with pytest.raises(error_type):
            transform_model_details(datasets=datasets)


class TestNestGeneticInfo:
    """
    Class for unit testing the nest_genetic_info function in model_details.py.

    This suite does NOT re-test things that have already been tested for process_genetic_modifications and/or
    create_ensembl_info_df and assumes that those functions are working correctly (e.g. by removing duplicate rows,
    handling null values as expected, and raising error cases).
    """

    # Basic test data for genetic modifications: one model with two genetic modifications, one human and one mouse
    basic_genetic_modifications_df = pd.DataFrame(
        {
            "name": ["model1", "model1"],
            "ensembl_gene_id": ["ENSG000001", "ENSMUSG0000002"],
            "modified_gene": ["GENEA", "geneB"],
            "allele": ["allele1", "allele2"],
            "allele_type": ["type1", "type2"],
            "mgi_allele_id": ["MGI:1", "MGI:2"],
        }
    )

    # Basic test data for gene metadata: two genes, one human and one mouse
    basic_gene_metadata_df = pd.DataFrame(
        {
            "ensembl_gene_id": ["ENSG000001", "ENSMUSG0000002"],
            "ensembl_release": ["104", "104"],
            "ensembl_possible_replacements": [["ENSG000004"], []],
            "ensembl_permalink": [
                "http://example.com/geneA",
                "http://example.com/geneB",
            ],
        }
    )

    def test_nest_genetic_info_should_pass(self) -> None:
        """
        Test the nest_genetic_info function with a one model with two genetic modifications, one human
        and one mouse.
        """
        genetic_mods = self.basic_genetic_modifications_df
        gene_meta = self.basic_gene_metadata_df

        expected_output_df = pd.DataFrame(
            [
                {
                    "name": "model1",
                    "genetic_info": [
                        {
                            "modified_gene": "GENEA",
                            "ensembl_gene_id": "ENSG000001",
                            "allele": "allele1",
                            "allele_type": "type1",
                            "mgi_allele_id": "MGI:1",
                            "ensembl_info": {
                                "ensembl_release": "104",
                                "ensembl_possible_replacements": ["ENSG000004"],
                                "ensembl_permalink": "http://example.com/geneA",
                            },
                        },
                        {
                            "modified_gene": "geneB",
                            "ensembl_gene_id": "ENSMUSG0000002",
                            "allele": "allele2",
                            "allele_type": "type2",
                            "mgi_allele_id": "MGI:2",
                            "ensembl_info": {
                                "ensembl_release": "104",
                                "ensembl_possible_replacements": [],
                                "ensembl_permalink": "http://example.com/geneB",
                            },
                        },
                    ],
                },
            ]
        )

        output_df = nest_genetic_info(genetic_mods, gene_meta)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_output_df)

    def test_nest_genetic_info_subsets_gene_metadata(self) -> None:
        """
        Test that the output of nest_genetic_info only includes genes that are present in the model genetic
        modifications DataFrame.
        """
        # Single gene modification
        genetic_mods = pd.DataFrame(
            [
                {
                    "name": "model1",
                    "ensembl_gene_id": "ENSG000001",
                    "modified_gene": "GENEA",
                    "allele": "allele1",
                    "allele_type": "type1",
                    "mgi_allele_id": "MGI:1",
                }
            ]
        )

        # Contains an extra gene that is not in the genetic modifications data frame
        gene_meta = self.basic_gene_metadata_df

        expected_genetic_info = [
            {
                "modified_gene": "GENEA",
                "ensembl_gene_id": "ENSG000001",
                "allele": "allele1",
                "allele_type": "type1",
                "mgi_allele_id": "MGI:1",
                "ensembl_info": {
                    "ensembl_release": "104",
                    "ensembl_possible_replacements": ["ENSG000004"],
                    "ensembl_permalink": "http://example.com/geneA",
                },
            },
        ]

        output_df = nest_genetic_info(genetic_mods, gene_meta)

        assert output_df.loc[0, "genetic_info"] == expected_genetic_info

    def test_nest_genetic_info_fails_with_missing_gene_metadata(self) -> None:
        """
        Test that nest_genetic_info raises a ValueError when gene_metadata is missing genes that are present in
        model_genetic_modifications.
        """

        genetic_mods = self.basic_genetic_modifications_df

        # Missing one of the genes present in genetic_mods
        gene_meta = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSG000001"],
                "ensembl_release": ["104"],
                "ensembl_possible_replacements": [["ENSG000004"]],
                "ensembl_permalink": ["http://example.com/geneA"],
            }
        )

        with pytest.raises(
            ValueError, match="`gene_metadata_df` is missing some Ensembl IDs"
        ):
            nest_genetic_info(genetic_mods, gene_meta)
