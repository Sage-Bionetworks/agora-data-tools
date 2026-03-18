import pytest
import pandas as pd
import numpy as np
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
                "human_transgene_allele_map": "human_transgene_allele_map.csv",
            },
            "disease_correlation_expected_output.json",
        ),
        (
            {
                "disease_correlation_results": "disease_correlation_results_missing_data.csv",
                "allele_info": "model_allele_info.csv",
                "model_info": "model_info.csv",
                "human_transgene_allele_map": "human_transgene_allele_map.csv",
            },
            "disease_correlation_missing_data_expected_output.json",
        ),
    ]

    pass_test_ids = [
        "Test assets should pass",
        "Rows with missing data should be removed",
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

    @pytest.fixture
    def datasets(self) -> dict[str, pd.DataFrame]:
        empty_human_transgene_allele_map = pd.DataFrame(
            {
                "mgi_allele_id": pd.Series(dtype="object"),
                "gene_symbol": pd.Series(dtype="object"),
                "ensembl_id": pd.Series(dtype="object"),
            }
        )

        model_info_df = pd.DataFrame(
            [
                {
                    "model": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
            ]
        )

        allele_info_df = pd.DataFrame(
            [
                {
                    "model": "LOAD1",
                    "gene": "APOE4",
                    "mgi_allele_id": 5810209,
                    "gene_ensembl_id": "ENSMUSG00000002985",
                }
            ]
        )

        disease_correlation_results = pd.DataFrame(
            [
                {
                    "cluster": "Cluster A",
                    "module": "IFGyellow",
                    "mouse_model": "LOAD1",
                    "sex": "Female",
                    "age": "4 months",
                    "correlation": 0.5,
                    "adjusted_p_value": 0.01,
                },
            ]
        )

        return {
            "disease_correlation_results": disease_correlation_results,
            "model_info": model_info_df,
            "allele_info": allele_info_df,
            "human_transgene_allele_map": empty_human_transgene_allele_map,
        }

    def test_transform_disease_correlation_deduplicates_allele_info(
        self, datasets: dict[str, pd.DataFrame]
    ) -> None:
        """
        Test that the disease correlation transformation succeeds with allele info that has duplicated rows.
        """
        datasets["allele_info"] = datasets["allele_info"].loc[[0, 0],]  # Duplicate row

        expected_output = [
            {
                "name": "LOAD1",
                "matched_control": "C57BL6J",
                "model_type": "Late Onset AD",
                "modified_genes": ["APOE4"],  # Deduplicated from duplicate entries
                "cluster": "Cluster A",
                "age": "4 months",
                "age_numeric": 4,
                "sex": "Female",
                "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
            }
        ]

        output = transform_disease_correlation(datasets)

        assert output == expected_output

    def test_transform_disease_correlation_fails_with_duplicate_module(
        self, datasets: dict[str, pd.DataFrame]
    ) -> None:
        """
        Test that the disease correlation transformation raises an error when there are duplicate modules
        for the same model in the disease_correlation_results dataset.
        """
        # Duplicate the first row to simulate duplicate modules for the same model
        datasets["disease_correlation_results"] = datasets[
            "disease_correlation_results"
        ].loc[
            [0, 0],
        ]
        with pytest.raises(ValueError, match="Module IFG already exists for LOAD1"):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "missing_dataset",
        [
            "human_transgene_allele_map",
            "model_info",
            "allele_info",
            "disease_correlation_results",
        ],
    )
    def test_transform_disease_correlation_fails_with_missing_dataset(
        self, missing_dataset: str, datasets: dict[str, pd.DataFrame]
    ) -> None:
        """
        Test that the disease correlation transformation raises appropriate errors
        when required datasets are missing or contain invalid data.
        """
        datasets.pop(missing_dataset)
        with pytest.raises(
            ValueError, match=f"Missing required datasets: {missing_dataset}"
        ):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "missing_column",
        [
            "cluster",
            "module",
            "mouse_model",
            "sex",
            "age",
            "correlation",
            "adjusted_p_value",
        ],
    )
    def test_transform_disease_correlation_fails_with_missing_column(
        self, missing_column: str, datasets: dict[str, pd.DataFrame]
    ) -> None:
        """
        Test that the disease correlation transformation raises appropriate errors
        when required columns are missing from the disease_correlation_results dataset.
        """
        datasets["disease_correlation_results"] = datasets[
            "disease_correlation_results"
        ].drop(columns=[missing_column])

        with pytest.raises(
            ValueError,
            match=f"Missing required columns in disease_correlation_results dataset: {missing_column}",
        ):
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
                    "correlation": 0.5,
                    "adjusted_p_value": 0.01,
                },
                {
                    "module": "PHGbrown",
                    "correlation": 0.6,
                    "adjusted_p_value": 0.02,
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
            "age_numeric": 4,
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
            [{"module": "IFGyellow", "correlation": 0.5, "adjusted_p_value": 0.01}]
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
            "age_numeric": 4,
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
            [{"module": "IFGyellow", "correlation": 0.5, "adjusted_p_value": 0.01}]
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

    def test_process_group_with_nan_values(self) -> None:
        """
        Test that process_group correctly handles NaN values in the disease correlation DataFrame.
        This tests the cases where either correlation or adjusted_p_value are NaN, and when both are NaN.
        """
        group = pd.DataFrame(
            [
                {
                    "module": "IFGyellow",
                    "correlation": np.nan,
                    "adjusted_p_value": 0.01,
                },
                {
                    "module": "PHGbrown",
                    "correlation": 0.6,
                    "adjusted_p_value": np.nan,
                },
                {
                    "module": "ABCgreen",
                    "correlation": np.nan,
                    "adjusted_p_value": np.nan,
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

        # We can't directly compare result to an expected result dictionary because np.nan == np.nan returns False, so
        # we check the parts of result that should have NaNs.

        assert np.isnan(result["IFG"]["correlation"]) and np.isclose(
            result["IFG"]["adj_p_val"], 0.01
        )
        assert np.isnan(result["PHG"]["adj_p_val"]) and np.isclose(
            result["PHG"]["correlation"], 0.6
        )

        # ABC should not be included because both correlation and adjusted_p_value are NaN
        assert "ABC" not in result.keys()
