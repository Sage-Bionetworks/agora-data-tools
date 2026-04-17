"""
Test suite for RNA individual expression transformation.

This module contains comprehensive tests for the `transform_rna_de_individual` function
and its helper functions, which process individual RNA-seq expression data for mouse models
into a structured format for the Agora platform.

Test Classes:
    - TestDetermineResultOrder: Unit tests for the _determine_result_order helper function
    - TestProcessIndividualDataFileCore: Unit tests for the _process_individual_data_file_core helper function
    - TestTransformRnaDeIndividual: Integration tests for the full transformation pipeline

The tests use synthetic datasets stored in `tests/test_assets/rna_de_individual/` to verify:
- Core transformation logic (data grouping, individual expression aggregation)
- Multi-model and multi-tissue handling with model_groups
- JAX tissue name mapping (e.g., 'Right Cerebral Hemisphere' -> 'Hemibrain')
- Human gene filtering (only mouse genes with ENSMUSG* IDs should be processed)
- Age sorting (numeric ordering of age entries)
- Data precision (rounding to 5 decimal places)
- Edge cases (single row data, missing metadata, empty files)
- Error handling (missing datasets, empty files, missing columns, inconsistent model_group values)
- Result ordering logic (controls should appear first in result_order arrays)
- Matched control determination (first item in result_order list)
- File grouping by model_group (including split-file models sharing the same group)

Test Data Structure:
    Input files include:
    - RNA-seq individual expression data (*.csv)
    - synthetic_rnaseq_genotype_label_map.csv (maps genotypes to display labels, result_order, and model_groups)
    - synthetic_mouse_gene_metadata.csv (gene symbols and metadata)

    Output files are JSON-formatted expected results for comparison.
"""

import os
import json
from typing import Any, Dict, List
import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_individual import (
    transform_rna_de_individual,
    _determine_result_order,
    _process_individual_data_file_core,
)


class TestDetermineResultOrder:
    """
    Unit tests for the _determine_result_order helper function.

    This class contains focused unit tests for result order determination,
    which creates an ordered list of display labels based on result_order values.
    The function accepts a data_file DataFrame (already merged with the label map
    and filtered to a single model_group) rather than the raw label map.

    Empty display_label values are validated upstream by prepare_genotype_label_map_df
    and will never reach this function.

    Test Methods:
        - test_single_model_result_order: Tests result ordering for a single model.
        - test_model_group_result_order: Tests result ordering for a model group.
        - test_empty_data_file: Tests handling of an empty data_file DataFrame.
    """

    def test_single_model_result_order(self) -> None:
        """Test result order for a model with two genotypes."""
        data_file = pd.DataFrame(
            {
                "display_label": ["Transgenic", "Wildtype"],
                "result_order": [2, 1],
            }
        )

        result = _determine_result_order(data_file)

        assert result == ["Wildtype", "Transgenic"]

    def test_model_group_result_order(self) -> None:
        """Test result order for a model group with multiple models."""
        data_file = pd.DataFrame(
            {
                "display_label": ["Model_B", "Control_B", "Model_C"],
                "result_order": [20, 10, 30],
            }
        )

        result = _determine_result_order(data_file)

        assert result == ["Control_B", "Model_B", "Model_C"]

    def test_empty_data_file(self) -> None:
        """Test handling of an empty data_file DataFrame."""
        data_file = pd.DataFrame(columns=["display_label", "result_order"])

        result = _determine_result_order(data_file)

        assert result == []


class TestProcessIndividualDataFileCore:
    """
    Unit tests for the _process_individual_data_file_core helper function.

    This class tests the core individual transformation logic that is called after
    shared preprocessing (filtering, rounding, validation). It focuses on testing
    the transform-specific logic: enrichment, filtering, and grouping.

    Test Methods:
        - test_basic_core_processing: Tests basic processing with enrichment and grouping.
        - test_genotype_filtering: Tests filtering of invalid genotypes.
        - test_uses_preprocessed_data: Tests that function expects preprocessed data.
        - test_multiple_genotypes_with_model_group: Tests multiple genotypes in a model group.
        - test_all_genotypes_unmatched_raises_value_error: Tests ValueError is raised when all rows are dropped.
        - test_non_digit_age_raises_value_error: Tests that a mix of valid and non-digit age strings raises ValueError.
        - test_all_ages_non_digit_raises_value_error: Tests that all-non-digit age values raise ValueError.
        - test_blank_age_raises_value_error: Tests that a blank (empty-string) age raises ValueError.
        - test_wrong_unit_age_raises_value_error: Tests that an age with digits but wrong unit
          (e.g. '1 year') raises ValueError.
        - test_non_digit_age_error_message_names_offending_values: Tests that the ValueError
          message lists every offending age value.
        - test_multiple_tissues_produce_separate_output_entries: Tests that different tissues
          produce one output entry each.
        - test_name_equals_model_for_single_model_group: Verifies that name is set to model
          (not model_group) for single-model groups.
    """

    def test_basic_core_processing(self) -> None:
        """Test basic core processing with genotype enrichment and grouping."""
        # Simulate preprocessed data (already filtered and rounded)
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["Ind001"],
                "expression": [5.12345],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["Tg"],
                "model": ["Model_A"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_label_map_df
        )

        expected = [
            {
                "ensembl_gene_id": "ENSMUSG00000000001",
                "gene_symbol": "Gene1",
                "tissue": "Cortex",
                "name": "Model_A",
                "model_group": "Model_A",
                "matched_control": "Transgenic",
                "units": "Log2 Counts per Million",
                "age": "6 months",
                "age_numeric": 6,
                "result_order": ["Transgenic"],
                "data": [
                    {
                        "genotype": "Transgenic",
                        "sex": "Male",
                        "individual_id": "Ind001",
                        "value": 5.12345,
                    }
                ],
            }
        ]
        assert result == expected

    def test_genotype_filtering(self) -> None:
        """Test that invalid genotypes are filtered out during core processing."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["Tg", "InvalidGenotype"],
                "model": ["Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_label_map_df
        )

        # Should only have 1 data point (valid genotype only)
        assert len(result) == 1
        assert len(result[0]["data"]) == 1
        assert result[0]["data"][0]["genotype"] == "Transgenic"

    def test_multiple_genotypes_with_model_group(self) -> None:
        """Test processing with multiple genotypes in a model group."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 3.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["Carrier", "Non-Carrier"],
                "model": ["Model_B", "Model_B"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_B", "Model_B"],
                "genotype": ["Carrier", "Non-Carrier"],
                "display_label": ["Model_B", "Control_B"],
                "result_order": [2, 1],
                "model_group": ["GroupX", "GroupX"],
            }
        )

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_label_map_df
        )

        # Should have 1 entry with 2 data points
        assert len(result) == 1
        assert len(result[0]["data"]) == 2
        assert result[0]["model_group"] == "GroupX"
        assert result[0]["matched_control"] == "Control_B"

    def test_all_genotypes_unmatched_raises_value_error(self) -> None:
        """Test that a ValueError is raised when all rows are dropped.

        This occurs when none of the genotypes in the data file have a match in the
        genotype label map, causing every row to be dropped by dropna. This is distinct
        from an empty input file (which also raises ValueError) — it means the file had
        data but no recognised genotypes, which strongly indicates a wrong file or a
        misconfigured label map.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["Ind001"],
                "expression": [5.0],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["UnknownGenotype"],
                "model": ["Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(
            ValueError,
            match="all genotypes in this file were absent from the label map",
        ):
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

    def test_non_digit_age_raises_value_error(self) -> None:
        """Test that age strings not matching the '[N] months' format raise a clear ValueError.

        age_numeric is extracted via regex r'(\\d+) months'. If any age value does not
        match this pattern (e.g. 'neonatal', '1 year', or blank), the code validates
        explicitly and raises ValueError with the offending values listed.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "neonatal"],
                "genotype": ["Tg", "Tg"],
                "model": ["Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(ValueError, match="age_numeric extraction failed"):
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

    def test_all_ages_non_digit_raises_value_error(self) -> None:
        """Test that ValueError is raised when every age value contains no digits.

        Ensures the check fires even when there is no valid age row to contrast with
        the bad ones (i.e. the all-bad case is not silently swallowed).
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["neonatal", "adult"],
                "genotype": ["Tg", "Tg"],
                "model": ["Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(ValueError, match="age_numeric extraction failed"):
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

    def test_blank_age_raises_value_error(self) -> None:
        """Test that an empty-string age value raises ValueError.

        An empty string does not match r'(\\d+) months', so the regex returns NaN and the
        explicit validation must catch it before the int cast.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["Ind001"],
                "expression": [5.0],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": [""],
                "genotype": ["Tg"],
                "model": ["Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(ValueError, match="age_numeric extraction failed"):
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

    def test_wrong_unit_age_raises_value_error(self) -> None:
        """Test that an age string with digits but the wrong unit raises ValueError.

        A value like '1 year' contains digits but does not match r'(\\d+) months',
        so the stricter regex returns NaN and the validation must catch it.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "1 year"],
                "genotype": ["Tg", "Tg"],
                "model": ["Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(ValueError, match="age_numeric extraction failed"):
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

    def test_non_digit_age_error_message_names_offending_values(self) -> None:
        """Test that the ValueError message explicitly lists every offending age value.

        The error message must name the bad values so that the caller can identify
        and fix the input data without further debugging.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                ],
                "individualid": ["Ind001", "Ind002", "Ind003"],
                "expression": [5.0, 6.0, 7.0],
                "tissue": ["Cortex", "Cortex", "Cortex"],
                "sex": ["Male", "Female", "Male"],
                "age": ["6 months", "neonatal", "adult"],
                "genotype": ["Tg", "Tg", "Tg"],
                "model": ["Model_A", "Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [2],
                "model_group": ["Model_A"],
            }
        )

        with pytest.raises(ValueError, match="neonatal") as exc_info:
            _process_individual_data_file_core(
                data_file, gene_metadata_dict, genotype_label_map_df
            )

        # Isolate the list of offending values (before the advice sentence) to avoid
        # false positives from the "e.g., '6 months'" example in the message template.
        offending_values_section = str(exc_info.value).split(". All age strings")[0]
        assert "neonatal" in offending_values_section
        assert "adult" in offending_values_section
        assert "6 months" not in offending_values_section

    def test_multiple_tissues_produce_separate_output_entries(self) -> None:
        """Test that data from different tissues produces one output entry per tissue.

        The grouping key is (ensembl_gene_id, tissue, name, age), so the same gene
        measured in two tissues must appear as two independent output entries, each
        with its own 'data' list containing only the individuals from that tissue.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                ],
                "individualid": ["Ind001", "Ind002", "Ind003", "Ind004"],
                "expression": [5.0, 6.0, 7.0, 8.0],
                "tissue": ["Cortex", "Cortex", "Hippocampus", "Hippocampus"],
                "sex": ["Male", "Female", "Male", "Female"],
                "age": ["6 months", "6 months", "6 months", "6 months"],
                "genotype": ["Tg", "Tg", "Tg", "Tg"],
                "model": ["Model_A", "Model_A", "Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": [1],
                "model_group": ["Model_A"],
            }
        )

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_label_map_df
        )

        assert len(result) == 2

        tissues_in_output = {entry["tissue"] for entry in result}
        assert tissues_in_output == {"Cortex", "Hippocampus"}

        for entry in result:
            assert entry["ensembl_gene_id"] == "ENSMUSG00000000001"
            assert len(entry["data"]) == 2  # 2 individuals per tissue

    def test_name_equals_model_for_single_model_group(self) -> None:
        """Test that name is set to model (not model_group) for single-model groups.

        When a group contains data from exactly one model, name should reflect the
        model value directly. This differs from model_group only for UCI-style models
        where model != model_group (e.g. model='Abca7*V1599M.5xFAD',
        model_group='Abca7*V1599M'). For models where model == model_group there
        is no observable difference, so this test uses a case where they differ.
        """
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["Carrier", "NonCarrier"],
                "model": ["Model_X.5xFAD", "Model_X.5xFAD"],
            }
        )

        gene_metadata_dict = {}
        genotype_label_map_df = pd.DataFrame(
            {
                "model": ["Model_X.5xFAD", "Model_X.5xFAD"],
                "genotype": ["Carrier", "NonCarrier"],
                "display_label": ["Model_X.5xFAD", "C57BL/6J"],
                "result_order": [2, 1],
                "model_group": ["Model_X", "Model_X"],
            }
        )

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_label_map_df
        )

        assert len(result) == 1
        assert result[0]["name"] == "Model_X.5xFAD"
        assert result[0]["model_group"] == "Model_X"


class TestTransformRnaDeIndividual:
    """
    Test class for RNA individual expression transformation.

    This class contains test methods that verify the behavior of the
    `transform_rna_de_individual` function using synthetic datasets designed
    to test specific functionality and edge cases.

    Attributes:
        data_files_path (str): Path to the directory containing test assets
            (synthetic input files and expected output files).

    Test Methods:
        - test_transform_rna_de_individual_missing_required_dataset: Tests error handling
          when required datasets are missing.
        - test_synthetic_basic_data: Tests core transformation with simple dataset.
        - test_synthetic_multi_model_data: Tests handling of multiple models with model_groups.
        - test_synthetic_jax_tissue_mapping: Tests JAX-specific tissue name mapping.
        - test_synthetic_mixed_genes_filtering: Tests filtering of human genes.
        - test_synthetic_age_sorting: Tests numeric sorting of age entries.
        - test_synthetic_single_row_data: Tests minimal edge case (single row).
        - test_synthetic_empty_data_file: Tests error handling for empty data files.
        - test_synthetic_missing_columns_data: Tests error handling for missing columns.
        - test_data_file_required_columns_parameter: Tests that data_file_required_columns parameter is honoured.
        - test_synthetic_rounding_precision: Tests 5-decimal-place rounding.
        - test_inconsistent_model_group_values: Tests error handling for inconsistent model_group values.
        - test_file_with_multiple_models_raises_value_error: Tests that a file containing
          rows from more than one model raises ValueError.
        - test_multiple_models_error_message_names_file_and_conflicting_models: Tests that
          the ValueError message identifies the offending file and both model names.
        - test_multiple_models_same_model_group_raises_value_error: Tests that a
          file containing two models that share a model_group still raises ValueError
          (each file must have exactly one model regardless of model_group).

    Helper Methods:
        - _load_synthetic_test_data: Loads synthetic test data files as DataFrames with
          proper dataset key mapping.
    """

    data_files_path = "tests/test_assets/rna_de_individual"

    def test_transform_rna_de_individual_missing_required_dataset(self) -> None:
        """Test that missing required datasets raise ValueError."""
        # Load datasets without mouse_gene_metadata
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                # Missing synthetic_mouse_gene_metadata.csv
            ]
        )

        # Expect transformation to raise ValueError for missing required dataset
        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def _load_synthetic_test_data(
        self, data_files: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """Load synthetic test data files as DataFrames."""
        datasets = {}
        input_path = os.path.join(self.data_files_path, "input")

        # Mapping from file names to expected dataset keys
        file_to_key_mapping = {
            "synthetic_rnaseq_genotype_label_map.csv": "rnaseq_genotype_label_map",
            "rnaseq_genotype_label_map_inconsistent.csv": "rnaseq_genotype_label_map",
            "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
        }

        for file_name in data_files:
            if file_name.endswith(".csv"):
                # Load CSV files
                file_path = os.path.join(input_path, file_name)
                df = pd.read_csv(file_path)

                # Use the mapped key if available, otherwise use file name without extension
                key = file_to_key_mapping.get(file_name, file_name.replace(".csv", ""))
                datasets[key] = df

        return datasets

    def test_synthetic_basic_data(self) -> None:
        """Test transformation with synthetic basic data.

        Tests a simple case with 2 genes, single age, and straightforward values
        to verify core transform functionality: data aggregation by gene and age,
        proper metadata enrichment (gene symbols, display labels), and result_order
        determination.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

    def test_synthetic_jax_tissue_mapping(self) -> None:
        """Test JAX tissue mapping with synthetic data.

        Verifies that tissue names from JAX models are correctly mapped: specifically that
        'Right Cerebral Hemisphere' tissue is transformed to 'Hemibrain' in the output.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_jax_tissue_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_jax_tissue_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify tissue mapping
        assert output_data_sorted[0]["tissue"] == "Hemibrain"

    def test_synthetic_mixed_genes_filtering(self) -> None:
        """Test human gene filtering with synthetic mixed genes data.

        Tests that the transform correctly filters out human genes (ENSG*) and only
        processes mouse genes (ENSMUSG*). The input contains a mix of both human and mouse
        genes, but only mouse genes should appear in the output.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_mixed_genes_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_mixed_genes_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify only mouse genes are present
        for entry in output_data:
            assert entry["ensembl_gene_id"].startswith("ENSMUSG")

    def test_synthetic_age_sorting(self) -> None:
        """Test age sorting with synthetic data.

        Verifies that when a gene has data at multiple ages, the output entries
        are created separately for each age and can be properly organized. Input ages
        are deliberately unsorted (12, 6, 3 months) to test numeric sorting.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_age_sorting_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_age_sorting_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id and age_numeric for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["age_numeric"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["age_numeric"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify ages are in correct numeric order
        assert output_data_sorted[0]["age"] == "3 months"
        assert output_data_sorted[1]["age"] == "6 months"
        assert output_data_sorted[2]["age"] == "12 months"

    def test_synthetic_single_row_data(self) -> None:
        """Test transformation with synthetic single row data.

        Tests edge case handling of minimal input: a single data row representing one individual
        at one age/condition. Verifies the transform can handle the smallest valid dataset.
        Also tests missing gene metadata handling - the gene (ENSMUSG00000000008) is not in
        synthetic_mouse_gene_metadata.csv, so gene_symbol should be "".
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_single_row_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_single_row_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify missing metadata handling
        assert output_data_sorted[0]["gene_symbol"] == ""

    def test_synthetic_empty_data_file(self) -> None:
        """Test handling of synthetic empty data files."""
        # Load datasets with empty synthetic data file
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_empty_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Should raise ValueError for empty data file
        with pytest.raises(ValueError, match="is empty"):
            transform_rna_de_individual(datasets=datasets)

    def test_synthetic_missing_columns_data(self) -> None:
        """Test handling of synthetic data files with missing required columns."""
        # Load datasets with a synthetic data file missing required columns (missing 'age')
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_missing_columns_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Expect transformation to raise ValueError for missing required columns
        with pytest.raises(ValueError, match="Missing required columns"):
            transform_rna_de_individual(datasets=datasets)

    def test_data_file_required_columns_parameter(self) -> None:
        """Test that the data_file_required_columns parameter is honoured.

        Verifies that the parameter is wired through to column validation rather than
        the constant always being used. Passes a custom list containing a non-existent
        column and confirms that ValueError is raised referencing that column, proving
        the parameter — not the hardcoded constant — drives validation.
        """
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        custom_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
            "nonexistent_column",
        ]

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_rna_de_individual(
                datasets=datasets,
                data_file_required_columns=custom_columns,
            )

    def test_synthetic_rounding_precision(self) -> None:
        """Test that expression values are rounded to 5 decimal places.

        Tests numeric precision by providing values with 7+ decimal places and verifying
        they are correctly rounded to exactly 5 decimal places in the output.
        """
        # Load synthetic test data with high-precision values
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rounding_precision_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path,
                "output",
                "synthetic_rounding_precision_output.json",
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
        )

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Explicitly verify rounding (1.123456789 -> 1.12346, 2.987654321 -> 2.98765)
        assert output_data_sorted[0]["data"][0]["value"] == pytest.approx(1.12346)
        assert output_data_sorted[0]["data"][1]["value"] == pytest.approx(2.98765)

    def test_synthetic_multi_model_data(self) -> None:
        """Test that models sharing a model_group but split across two input files
        are combined into a single output entry with all four genotypes.

        This covers the UCI model bug where e.g. Trem2-R47H_NSS and
        Trem2-R47H_NSS.5xFAD both belong to model_group 'Trem2-R47H_NSS' but
        their expression data lives in separate files. Before the fix each file
        produced its own 2-genotype entry; after the fix they produce a single
        4-genotype entry.
        """
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_multi_model_file1.csv",
                "synthetic_multi_model_file2.csv",
            ]
        )

        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_multi_model_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        output_data = transform_rna_de_individual(datasets=datasets)

        def normalize(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
            """Sort each entry's inner data list by individual_id, then sort the
            outer list by (ensembl_gene_id, tissue, age) so that order-independent
            differences don't cause false failures."""
            for e in entries:
                e["data"] = sorted(e["data"], key=lambda x: x["individual_id"])
            return sorted(
                entries, key=lambda x: (x["ensembl_gene_id"], x["tissue"], x["age"])
            )

        assert normalize(output_data) == normalize(expected_data)

    def _build_mixed_model_datasets(
        self, data_file_key: str = "mixed_model_file"
    ) -> Dict[str, pd.DataFrame]:
        """Return a minimal datasets dict whose data file contains rows from two
        different models (Model_A and Model_B), which violates the one-model-per-file
        invariant and must always raise ValueError."""
        label_map = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B", "Model_B"],
                "genotype": ["Tg", "Wt", "Carrier", "NonCarrier"],
                "display_label": [
                    "Model_A Tg",
                    "Model_A Wt",
                    "Model_B Carrier",
                    "Model_B NonCarrier",
                ],
                "result_order": [2, 1, 2, 1],
                "model_group": ["Model_A", "Model_A", "Model_B", "Model_B"],
            }
        )
        gene_metadata = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "gene_symbol": ["Gene1"],
            }
        )
        mixed_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "expression": [5.0, 6.0],
                "model": ["Model_A", "Model_B"],
                "genotype": ["Tg", "Carrier"],
                "age": ["6 months", "6 months"],
                "sex": ["Male", "Male"],
                "tissue": ["Cortex", "Cortex"],
                "individualid": ["Ind001", "Ind002"],
            }
        )
        return {
            "rnaseq_genotype_label_map": label_map,
            "mouse_gene_metadata": gene_metadata,
            data_file_key: mixed_file,
        }

    def test_file_with_multiple_models_raises_value_error(
        self,
    ) -> None:
        """Test that a data file containing rows from more than one model raises ValueError.

        Each input file must contain data for exactly one model. A file mixing rows
        from Model_A and Model_B violates this invariant and must be rejected regardless
        of whether those models share a model_group.
        """
        datasets = self._build_mixed_model_datasets()

        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def test_multiple_models_error_message_names_file_and_conflicting_models(
        self,
    ) -> None:
        """Test that the ValueError message identifies the offending file and both models.

        The message must name the file (so the caller knows which input to fix) and
        both conflicting model names (so the caller knows why it failed).
        """
        datasets = self._build_mixed_model_datasets(data_file_key="my_mixed_file")

        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_individual(datasets=datasets)

        error_message = str(exc_info.value)
        assert "my_mixed_file" in error_message
        assert "Model_A" in error_message
        assert "Model_B" in error_message

    def test_multiple_models_same_model_group_raises_value_error(
        self,
    ) -> None:
        """Test that a file with two models raises ValueError even when they share a model_group.

        Each input file must contain data for exactly one model. Even if Model_B and
        Model_C both belong to the same model_group ('GroupX'), combining their rows in
        a single file violates the one-model-per-file invariant and must be rejected.
        """
        label_map = pd.DataFrame(
            {
                "model": ["Model_B", "Model_B", "Model_C", "Model_C"],
                "genotype": ["Carrier", "NonCarrier", "Carrier", "NonCarrier"],
                "display_label": ["Model_B", "Control", "Model_C", "Control"],
                "result_order": [2, 1, 3, 1],
                "model_group": ["GroupX", "GroupX", "GroupX", "GroupX"],
            }
        )
        gene_metadata = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "gene_symbol": ["Gene1"],
            }
        )
        combined_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "expression": [5.0, 6.0],
                "model": ["Model_B", "Model_C"],
                "genotype": ["Carrier", "Carrier"],
                "age": ["6 months", "6 months"],
                "sex": ["Male", "Male"],
                "tissue": ["Cortex", "Cortex"],
                "individualid": ["Ind001", "Ind002"],
            }
        )
        datasets = {
            "rnaseq_genotype_label_map": label_map,
            "mouse_gene_metadata": gene_metadata,
            "combined_file": combined_file,
        }

        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_individual(datasets=datasets)

        error_message = str(exc_info.value)
        assert "combined_file" in error_message
        assert "Model_B" in error_message
        assert "Model_C" in error_message

    def test_inconsistent_model_group_values(self) -> None:
        """Test error handling for inconsistent model_group values within the same model.

        Tests that when a model has different model_group values across multiple rows
        (e.g., different genotypes), a clear ValueError is raised identifying which
        models have inconsistent values.
        """
        # Load synthetic test data with inconsistent model_group values
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "rnaseq_genotype_label_map_inconsistent.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Expect transformation to raise ValueError with informative message
        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_individual(datasets=datasets)

        # Verify the error message contains expected information
        error_message = str(exc_info.value)
        assert "Each model must have a consistent model_group value" in error_message
        assert "rnaseq_genotype_label_map" in error_message
        assert "APOE4" in error_message

    def test_raises_on_empty_model_in_label_map(self) -> None:
        """Test that an empty model value in the label map raises ValueError from check_column_rules."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )
        datasets["rnaseq_genotype_label_map"].loc[0, "model"] = None
        with pytest.raises(ValueError, match="model.*not_empty"):
            transform_rna_de_individual(datasets=datasets)

    def test_raises_on_empty_model_group_in_label_map(self) -> None:
        """Test that an empty model_group value in the label map raises ValueError from check_column_rules."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )
        datasets["rnaseq_genotype_label_map"].loc[0, "model_group"] = None
        with pytest.raises(ValueError, match="model_group.*not_empty"):
            transform_rna_de_individual(datasets=datasets)

    def test_raises_on_empty_display_label_in_label_map(self) -> None:
        """Test that an empty display_label value in the label map raises ValueError from check_column_rules."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )
        datasets["rnaseq_genotype_label_map"].loc[0, "display_label"] = None
        with pytest.raises(ValueError, match="display_label.*not_empty"):
            transform_rna_de_individual(datasets=datasets)

    def test_raises_on_non_ensmusg_gene_id_in_metadata(self) -> None:
        """Test that a non-ENSMUSG ensembl_gene_id in mouse_gene_metadata raises ValueError."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )
        datasets["mouse_gene_metadata"].loc[0, "ensembl_gene_id"] = "ENSG00000000001"
        with pytest.raises(ValueError, match="ensembl_gene_id.*starts_with.*ENSMUSG"):
            transform_rna_de_individual(datasets=datasets)
