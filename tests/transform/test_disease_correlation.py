import pytest
import pandas as pd
import os
import json
from agoradatatools.etl.transform.disease_correlation import (
    transform_disease_correlation,
    create_lookup,
    extract_module_name,
    process_group,
    map_genes_to_human_symbols,
)


class TestDiseaseCorrelation:
    """
    Test class for validating disease correlation transformation using test asset files.
    These tests use pre-defined CSV input files and expected JSON output files to ensure
    the transformation logic works correctly with real data structures.
    """

    def read_input_files(self, input_files: dict[str, str]) -> dict[str, pd.DataFrame]:
        """
        Helper function to read input CSV files from test assets and return a dictionary of DataFrames.

        Args:
            input_files: Dictionary mapping dataset names to CSV file names
        Returns:
            Dictionary mapping dataset names to loaded DataFrames
        """
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )
        return datasets

    data_files_path = "tests/test_assets/disease_correlation"

    # Test data configuration for asset-based tests
    pass_test_data = [
        (
            # Test case 1: Ideal data with complete, non-missing information in all rows/data sets.
            {
                "disease_correlation_results": "disease_correlation_results.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
                "model_metadata": "model_metadata.csv",
            },
            "disease_correlation_expected_output.json",
        ),
        (
            # Test case 2: Some rows in disease_correlation_results are missing an entry for one of the grouping columns
            # (cluster, module, mouse_model, sex, or age). All but one row is missing exactly one of these values, and
            # all grouping columns have at least one missing value in the file. One row has all non-missing data, and
            # this is the only row that should show up in the output. Rows with missing data should be removed.
            {
                "disease_correlation_results": "disease_correlation_results_missing_grouping_data.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
                "model_metadata": "model_metadata.csv",
            },
            "disease_correlation_missing_grouping_data_expected_output.json",
        ),
        (
            # Test case 3: Some rows in disease_correlation_results are missing correlation or p-value data. One row has
            # both values, one is missing correlation, one is missing p-value, and one is missing both values. All rows
            # have a unique tissue/module. Tissues with complete data or missing only one of correlation or p-value
            # should have entries in the output, while the tissue missing both values will not show up in the output.
            {
                "disease_correlation_results": "disease_correlation_results_missing_numeric_data.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
                "model_metadata": "model_metadata.csv",
            },
            "disease_correlation_missing_numeric_data_expected_output.json",
        ),
    ]

    pass_test_ids = [
        "Pass with ideal data",
        "Pass and remove rows with missing grouping data",
        "Pass with missing correlation or p-value data",
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
        datasets = self.read_input_files(input_files)

        # Transform data using the disease correlation transformation function
        output_data = transform_disease_correlation(datasets=datasets)

        # Load expected output from JSON file
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

        # Compare actual output with expected output
        assert output_data == expected_data

    def test_transform_disease_correlation_fails_with_duplicate_module(self) -> None:
        """
        Test that the disease correlation transformation raises an error when there are duplicate modules
        for the same model in the disease_correlation_results dataset.
        """
        datasets = self.read_input_files(
            {
                # This file has a row with a duplicate module (IFG) for one group, but different correlation and
                # p-value data.
                "disease_correlation_results": "disease_correlation_results_duplicated_module.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
                "model_metadata": "model_metadata.csv",
            }
        )

        with pytest.raises(ValueError, match="Module IFG already exists for LOAD1"):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "missing_dataset",
        [
            "disease_correlation_results",
            "model_metadata",
            "model_genetic_modifications",
        ],
        ids=[
            "Missing disease_correlation_results",
            "Missing model_metadata",
            "Missing model_genetic_modifications",
        ],
    )
    def test_transform_disease_correlation_fails_with_missing_dataset(
        self, missing_dataset: str
    ) -> None:
        """
        Test that the disease correlation transformation raises appropriate errors when required datasets are missing.
        """
        datasets = self.read_input_files(
            {
                "disease_correlation_results": "disease_correlation_results.csv",
                "model_metadata": "model_metadata.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
            }
        )

        datasets.pop(missing_dataset)
        with pytest.raises(
            ValueError, match=f"Missing required datasets: {missing_dataset}"
        ):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "dataset, missing_column",
        [
            ("disease_correlation_results", "cluster"),
            ("model_metadata", "model"),
            ("model_genetic_modifications", "model"),
            ("model_genetic_modifications", "mgi_allele_id"),
        ],
        ids=[
            "Missing 'cluster' from disease_correlation_results",
            "Missing 'model' from model_metadata",
            "Missing 'model' from model_genetic_modifications",
            "Missing 'mgi_allele_id' from model_genetic_modifications",
        ],
    )
    def test_transform_disease_correlation_fails_with_missing_columns(
        self, dataset: str, missing_column: str
    ) -> None:
        """
        Test that the disease correlation transformation raises appropriate errors when required datasets are missing
        a required column. This does not test every column, rather it serves to confirm that every data set is being
        validated for required columns. Testing if one required column is missing from each dataset is sufficient.
        """
        datasets = self.read_input_files(
            {
                "disease_correlation_results": "disease_correlation_results.csv",
                "model_metadata": "model_metadata.csv",
                "model_genetic_modifications": "model_genetic_modifications.csv",
            }
        )

        datasets[dataset] = datasets[dataset].drop(columns=[missing_column])

        with pytest.raises(
            ValueError,
            match=f"Missing required columns in {dataset} dataset: {missing_column}",
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

        # Model information dictionary -- matched_controls is always a list by the time process_group is called
        model_info = {"matched_controls": ["C57BL6J"], "model_type": "Late Onset AD"}

        # Allele information with multiple genes
        allele_info = {"mouse_gene_symbol": ["APOE4", "TREM2"]}

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

    def test_process_group_with_none_values(self) -> None:
        """
        Test that process_group correctly handles None and zero values in the disease correlation DataFrame. If both
        correlation and p-value are None for a given module, that module should be excluded from the output. If only one
        of the two values is None, the module should be included in the output with the None value preserved. Zero
        values should be treated as valid numeric values and not as missing data/False, and should be preserved in the
        output.
        """
        group = pd.DataFrame(
            [
                # These two tissues should both show up in the output, with None values preserved as None
                {
                    "module": "IFGyellow",
                    "correlation": None,
                    "adjusted_p_value": 0.01,
                },
                {
                    "module": "PHGbrown",
                    "correlation": 0.6,
                    "adjusted_p_value": None,
                },
                # This tissue should not show up in the output because both values are missing
                {
                    "module": "ABCgreen",
                    "correlation": None,
                    "adjusted_p_value": None,
                },
                # Zero-values should be preserved
                {
                    "module": "XYZred",
                    "correlation": 0,
                    "adjusted_p_value": 0,
                },
            ],
            dtype="O",  # Required to keep the None values as None instead of converting to float with NaN
        )

        # Model information dictionary
        model_info = {"matched_controls": ["C57BL6J"], "model_type": "Late Onset AD"}

        # Allele information with multiple genes
        allele_info = {"mouse_gene_symbol": ["APOE4", "TREM2"]}

        result = process_group(
            group=group,
            model_info=model_info,
            allele_info=allele_info,
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        expected_result = {
            "name": "LOAD1",
            "matched_control": "C57BL6J",
            "model_type": "Late Onset AD",
            "modified_genes": ["APOE4", "TREM2"],
            "cluster": "Cluster A",
            "age": "4 months",
            "age_numeric": 4,
            "sex": "Female",
            "IFG": {"correlation": None, "adj_p_val": 0.01},
            "PHG": {"correlation": 0.6, "adj_p_val": None},
            # ABC should be missing
            # XYZ should be preserved with zero-values
            "XYZ": {"correlation": 0, "adj_p_val": 0},
        }

        assert result == expected_result


class TestMapGenesToHumanSymbols:
    """
    Test class for validating the map_genes_to_human_symbols function.
    This function maps mouse gene names to human gene symbols using the pre-merged
    model_genetic_modifications dataset (human_gene_symbol column already joined in).
    """

    def test_map_genes_should_pass(self):
        """
        Test that map_genes_to_human_symbols correctly maps genes when human_gene_symbol is present,
        and preserves the original mouse gene name when human_gene_symbol is null.
        """
        model_genetic_modifications_df = pd.DataFrame(
            [
                {
                    "model": "APOE4",
                    "mouse_gene_symbol": "Apoe",
                    "human_gene_symbol": "APOE",
                    "human_ensembl_id": "ENSG00000130203",
                },
                {
                    "model": "5xFAD",
                    "mouse_gene_symbol": "App",
                    "human_gene_symbol": "APP",
                    "human_ensembl_id": "ENSG00000142192",
                },
                {
                    "model": "5xFAD",
                    "mouse_gene_symbol": "Psen1",
                    "human_gene_symbol": "PSEN1",
                    "human_ensembl_id": "ENSG00000080815",
                },
                {
                    "model": "Model1",
                    "mouse_gene_symbol": "Mapt",
                    "human_gene_symbol": None,
                    "human_ensembl_id": None,
                },
            ]
        )

        result = map_genes_to_human_symbols(model_genetic_modifications_df)

        expected_df = pd.DataFrame(
            [
                {
                    "model": "APOE4",
                    "mouse_gene_symbol": "APOE",
                    "human_ensembl_id": "ENSG00000130203",
                },
                {
                    "model": "5xFAD",
                    "mouse_gene_symbol": "APP",
                    "human_ensembl_id": "ENSG00000142192",
                },
                {
                    "model": "5xFAD",
                    "mouse_gene_symbol": "PSEN1",
                    "human_ensembl_id": "ENSG00000080815",
                },
                {
                    "model": "Model1",
                    "mouse_gene_symbol": "Mapt",  # preserved — no human mapping
                    "human_ensembl_id": None,
                },
            ]
        )

        pd.testing.assert_frame_equal(result, expected_df)

    def test_map_genes_empty_transgene_map(self):
        """
        Test that map_genes_to_human_symbols handles an empty dataframe gracefully.
        """
        model_genetic_modifications_df = pd.DataFrame(
            {
                "model": pd.Series(dtype="object"),
                "mouse_gene_symbol": pd.Series(dtype="object"),
                "mgi_allele_id": pd.Series(dtype="object"),
                "human_gene_symbol": pd.Series(dtype="object"),
                "human_ensembl_id": pd.Series(dtype="object"),
            }
        )

        result = map_genes_to_human_symbols(model_genetic_modifications_df)

        expected_df = pd.DataFrame(
            {
                "model": pd.Series(dtype="object"),
                "mouse_gene_symbol": pd.Series(dtype="object"),
                "mgi_allele_id": pd.Series(dtype="object"),
                "human_ensembl_id": pd.Series(dtype="object"),
            }
        )

        pd.testing.assert_frame_equal(result, expected_df)
