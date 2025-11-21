import pytest
import pandas as pd
import os
import json
from agoradatatools.etl.transform.disease_correlation import (
    transform_disease_correlation,
    create_lookup,
    extract_module_name,
    process_group,
)


class TestDiseaseCorrelationAssets:
    """
    Test class for validating disease correlation transformation using test asset files.
    These tests use pre-defined CSV input files and expected JSON output files to ensure
    the transformation logic works correctly with real data structures.
    """

    data_files_path = "tests/test_assets/disease_correlation"

    # Test data configuration for asset-based tests
    pass_test_data = [
        (
            {
                "disease_correlation_results": "disease_correlation_results.csv",
                "allele_info": "model_allele_info.csv",
                "model_info": "model_info.csv",
            },
            "disease_correlation_expected_output.json",
        )
    ]

    pass_test_ids = [
        "Test assets should pass",
    ]

    @pytest.mark.parametrize(
        "input_files, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_disease_correlation_transform_assets_should_pass(
        self, input_files, expected_output_file
    ):
        """
        Test that the disease correlation transformation produces the expected output
        when using test asset files. This test validates the end-to-end transformation
        process with realistic data structures.

        Args:
            input_files: Dictionary mapping dataset names to CSV file names
            expected_output_file: JSON file containing the expected transformation output
        """
        # Create datasets dictionary by reading CSV files from test assets
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Transform data using the disease correlation transformation function
        output_data = transform_disease_correlation(datasets=datasets)

        # Load expected output from JSON file
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

        # Compare actual output with expected output
        assert output_data == expected_data


class TestTransformDiseaseCorrelation:
    """
    Test class for validating the disease correlation transformation function with various
    input scenarios. Tests both successful transformations and error conditions.
    """

    # Test data for successful transformation scenarios
    pass_test_data = [
        # Test case 1: Basic valid input with multiple models and modules
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                        {
                            "cluster": "Cluster A",
                            "module": "PHGbrown",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.6",
                            "adjusted_p_value": "0.02",
                        },
                        {
                            "cluster": "Cluster B",
                            "module": "TCXturquoise",
                            "mouse_model": "LOAD2",
                            "sex": "Male",
                            "age": "6 months",
                            "correlation": "0.7",
                            "adjusted_p_value": "0.03",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "name": "LOAD2",
                            "matched_controls": "C57BL6J",
                            "model_type": "Early Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                        {"name": "LOAD1", "gene": "TREM2"},
                        {"name": "LOAD2", "gene": "APP"},
                    ]
                ),
            },
            # Expected output structure for validation
            [
                {
                    "name": "LOAD1",
                    "matched_control": "C57BL6J",
                    "model_type": "Late Onset AD",
                    "modified_genes": ["APOE4", "TREM2"],
                    "cluster": "Cluster A",
                    "age": "4 months",
                    "sex": "Female",
                    "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
                    "PHG": {"correlation": 0.6, "adj_p_val": 0.02},
                },
                {
                    "name": "LOAD2",
                    "matched_control": "C57BL6J",
                    "model_type": "Early Onset AD",
                    "modified_genes": ["APP"],  # Single gene is returned as string, not list
                    "cluster": "Cluster B",
                    "age": "6 months",
                    "sex": "Male",
                    "TCX": {"correlation": 0.7, "adj_p_val": 0.03},
                },
            ],
        ),
        # Test case 2: Duplicate allele_info entries should be deduplicated
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                        {"name": "LOAD1", "gene": "APOE4"},  # Duplicate entry
                    ]
                ),
            },
            # Expected output: duplicate genes should be deduplicated
            # The output should contain a single entry for "LOAD1" with the
            # "modified_genes" field deduplicated to "APOE4" (as a string, not a list),
            # and the "IFG" field should contain the correct correlation and
            # adjusted p-value values (0.5 and 0.01, respectively).
            [
                {
                    "name": "LOAD1",
                    "matched_control": "C57BL6J",
                    "model_type": "Late Onset AD",
                    "modified_genes": ["APOE4"],  # Deduplicated from duplicate entries
                    "cluster": "Cluster A",
                    "age": "4 months",
                    "sex": "Female",
                    "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
                }
            ],
        ),
    ]
    pass_test_ids = [
        "Basic valid input should pass",
        "Duplicate allele_info includes all genes should pass",
    ]

    @pytest.mark.parametrize(
        "datasets, expected_output", pass_test_data, ids=pass_test_ids
    )
    def test_transform_disease_correlation_should_pass(self, datasets, expected_output):
        """
        Test that the disease correlation transformation succeeds with valid input data.

        Args:
            datasets: Dictionary containing DataFrames for disease_correlation_results,
                     model_info, and allele_info
            expected_output: Expected output structure for validation
        """
        output = transform_disease_correlation(datasets)

        # For the first test case, compare with expected output directly
        if isinstance(expected_output, list):
            assert output == expected_output
        else:
            # For other test cases that use assertion functions, call them
            assert expected_output(output)

    # Test data for dataset-level error scenarios
    dataset_error_test_data = [
        # Test case 1: Missing required model_info dataset
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
                # Note: model_info dataset is missing
            },
            ValueError,
            "Missing required datasets: model_info",
        ),
        # Test case 2: Duplicate entries in disease_correlation_results
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",  # Duplicate module for same model
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Module IFG already exists for LOAD1",
        ),
        # Test case 3: Inconsistent model_info entries for the same model
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "name": "LOAD1",  # Same model name but different values
                            "matched_controls": "CTRL2",
                            "model_type": "Wrong",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Model LOAD1 has inconsistent matched_controls values:",
        ),
    ]

    # Test data for column-level error scenarios
    column_error_test_data = [
        # Test case 1: Missing required column in disease_correlation_results
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            # Note: 'age' column is missing
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Missing required columns in disease_correlation_results dataset: age",
        ),
    ]

    dataset_error_test_ids = [
        "Missing model_info",
        "Duplicate results in disease_correlation_results",
        "Inconsistent model_info",
    ]
    column_error_test_ids = ["Missing required column in disease_correlation_results"]

    @pytest.mark.parametrize(
        "datasets, error_type, error_msg",
        dataset_error_test_data,
        ids=dataset_error_test_ids,
    )
    def test_transform_disease_correlation_missing_dataset(
        self, datasets, error_type, error_msg
    ):
        """
        Test that the disease correlation transformation raises appropriate errors
        when required datasets are missing or contain invalid data.

        Args:
            datasets: Dictionary containing incomplete or invalid DataFrames
            error_type: Expected exception type
            error_msg: Expected error message pattern
        """
        with pytest.raises(error_type, match=error_msg):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "datasets, error_type, error_msg",
        column_error_test_data,
        ids=column_error_test_ids,
    )
    def test_transform_disease_correlation_missing_column(
        self, datasets, error_type, error_msg
    ):
        """
        Test that the disease correlation transformation raises appropriate errors
        when required columns are missing from the input datasets.

        Args:
            datasets: Dictionary containing DataFrames with missing required columns
            error_type: Expected exception type
            error_msg: Expected error message pattern
        """
        with pytest.raises(error_type, match=error_msg):
            transform_disease_correlation(datasets)


class TestCreateLookup:
    """
    Test class for validating the create_lookup utility function.
    This function groups DataFrame rows by a specified column and creates
    a lookup dictionary with aggregated values.
    """

    def test_create_lookup(self):
        """
        Test that create_lookup correctly groups DataFrame rows and creates
        a lookup dictionary with appropriate value aggregation.
        """
        # Create test DataFrame with multiple rows for same group key
        input_dataframe = pd.DataFrame(
            [
                {"A": "a1", "B": "b1", "C": "c1"},
                {"A": "a1", "B": "b2", "C": "c1"},  # Same A value, different B
                {"A": "a1", "B": "b3", "C": "c1"},  # Same A value, different B
                {"A": "a2", "B": "b4", "C": "c2"},  # Different A value
            ]
        )
        group_by_col = "A"

        # Expected output: grouped by 'A', with B values as lists when multiple,
        # and C values as single values (assuming they're consistent within groups)
        expected_output = {
            "a1": {"B": ["b1", "b2", "b3"], "C": "c1"},
            "a2": {"B": "b4", "C": "c2"},
        }

        output = create_lookup(df=input_dataframe, group_by_col=group_by_col)
        assert output == expected_output


class TestExtractModuleName:
    """
    Test class for validating the extract_module_name utility function.
    This function extracts the module name from a string that may contain
    color suffixes (e.g., "IFGyellow" -> "IFG").
    """

    @pytest.mark.parametrize(
        "input_module,expected",
        [
            ("IFGyellow", "IFG"),  # Module with color suffix
            ("PHGbrown", "PHG"),  # Module with color suffix
            ("TCXturquoise", "TCX"),  # Module with color suffix
            ("IFG", "IFG"),  # Module without color suffix
            ("", ""),  # Empty string
            ("123ABC", "123ABC"),  # String that doesn't match module pattern
        ],
    )
    def test_extract_module_name(self, input_module, expected):
        """
        Test that extract_module_name correctly extracts the module name
        from strings with or without color suffixes.

        Args:
            input_module: Input string that may contain module name and color
            expected: Expected extracted module name
        """
        assert extract_module_name(input_module) == expected


class TestProcessGroup:
    """
    Test class for validating the process_group function.
    This function processes a group of disease correlation results and creates
    a structured output with model information and module data.
    """

    def test_process_group_with_valid_data(self):
        """
        Test that process_group correctly processes valid input data and
        creates the expected output structure with all fields populated.
        """
        # Create test data with multiple modules
        group = pd.DataFrame(
            [
                {
                    "module": "IFGyellow",
                    "correlation": "0.5",
                    "adjusted_p_value": "0.01",
                },
                {
                    "module": "PHGbrown",
                    "correlation": "0.6",
                    "adjusted_p_value": "0.02",
                },
            ]
        )

        # Model information dictionary
        model_info = {"matched_controls": "C57BL6J", "model_type": "Late Onset AD"}

        # Allele information with multiple genes
        allele_info = {"gene": ["APOE4", "TREM2"]}

        result = process_group(
            group=group,
            model_info=model_info,
            allele_info=allele_info,
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        # Verify the complete output structure
        assert result == {
            "name": "LOAD1",
            "matched_control": "C57BL6J",
            "model_type": "Late Onset AD",
            "modified_genes": ["APOE4", "TREM2"],
            "cluster": "Cluster A",
            "age": "4 months",
            "sex": "Female",
            "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
            "PHG": {"correlation": 0.6, "adj_p_val": 0.02},
        }

    def test_process_group_with_empty_model_info(self):
        """
        Test that process_group handles empty model_info and allele_info
        gracefully by using default empty values.
        """
        # Create test data with single module
        group = pd.DataFrame(
            [{"module": "IFGyellow", "correlation": "0.5", "adjusted_p_value": "0.01"}]
        )

        result = process_group(
            group=group,
            model_info={},  # Empty model info
            allele_info={},  # Empty allele info
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        # Verify output with empty default values
        assert result == {
            "name": "LOAD1",
            "matched_control": "",
            "model_type": "",
            "modified_genes": [],
            "cluster": "Cluster A",
            "age": "4 months",
            "sex": "Female",
            "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
        }

    def test_process_group_with_list_matched_controls(self):
        """
        Test that process_group correctly handles matched_controls as a list
        by taking the first element.
        """
        # Create test data with single module
        group = pd.DataFrame(
            [{"module": "IFGyellow", "correlation": "0.5", "adjusted_p_value": "0.01"}]
        )

        # Model info with matched_controls as a list
        model_info = {
            "matched_controls": ["C57BL6J", "CTRL2"],
            "model_type": "Late Onset AD",
        }

        result = process_group(
            group=group,
            model_info=model_info,
            allele_info={},
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        # Should take first element from the list
        assert result["matched_control"] == "C57BL6J"
