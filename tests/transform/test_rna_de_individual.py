"""
Test suite for RNA individual expression transformation.

This module contains comprehensive tests for the `transform_rna_de_individual` function
and its helper functions, which process individual RNA-seq expression data for mouse models
into a structured format for the Agora platform.

Test Classes:
    - TestCreateGenotypeMetadataDict: Unit tests for the _create_genotype_metadata_dict helper function
    - TestDetermineResultOrder: Unit tests for the _determine_result_order helper function
    - TestCreateIndividualResultsFromGroup: Unit tests for the _create_individual_results_from_group helper function
    - TestCreateOutputEntryFromGroup: Unit tests for the _create_output_entry_from_group helper function
    - TestProcessSingleDataFile: Unit tests for the _process_single_data_file helper function
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
- Matched control determination (minimum result_order value)
- Model group vs individual model handling

Test Data Structure:
    Input files include:
    - RNA-seq individual expression data (*.csv)
    - rnaseq_genotype_label_map.csv (maps genotypes to display labels, result_order, and model_groups)
    - mouse_gene_metadata.csv (gene symbols and metadata)

    Output files are JSON-formatted expected results for comparison.
"""

import os
import json
from typing import Dict, List
import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_individual import (
    transform_rna_de_individual,
    _create_genotype_metadata_dict,
    _determine_result_order,
    _create_individual_results_from_group,
    _create_output_entry_from_group,
    _process_single_data_file,
    _process_individual_data_file_core,
)


class TestCreateGenotypeMetadataDict:
    """
    Unit tests for the _create_genotype_metadata_dict helper function.

    This class contains focused unit tests for genotype metadata dictionary creation,
    which maps (model, genotype) tuples to their metadata including display labels,
    result_order, model_group, and effective_model_group.

    Test Methods:
        - test_basic_metadata_creation: Tests basic dictionary creation with single model.
        - test_with_model_groups: Tests metadata creation with model_groups.
        - test_effective_model_group_calculation: Tests effective_model_group calculation logic.
        - test_result_order_conversion: Tests that result_order is converted to int.
        - test_empty_dataframe: Tests handling of empty DataFrame input.
    """

    def test_basic_metadata_creation(self) -> None:
        """Test basic genotype metadata dictionary creation."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "genotype": ["Tg", "Wt"],
                "display_label": ["Transgenic", "Wildtype"],
                "result_order": [2, 1],
                "model_group": ["", ""],
            }
        )

        result = _create_genotype_metadata_dict(df)

        assert len(result) == 2
        assert ("Model_A", "Tg") in result
        assert ("Model_A", "Wt") in result
        assert result[("Model_A", "Tg")]["display_label"] == "Transgenic"
        assert result[("Model_A", "Tg")]["result_order"] == 2
        assert result[("Model_A", "Tg")]["model_group"] == ""
        assert result[("Model_A", "Tg")]["effective_model_group"] == "Model_A"

    def test_with_model_groups(self) -> None:
        """Test metadata creation with model_groups defined."""
        df = pd.DataFrame(
            {
                "model": ["Model_B", "Model_B", "Model_C"],
                "genotype": ["Carrier", "Non-Carrier", "Mutant"],
                "display_label": ["Model_B", "Control_B", "Model_C"],
                "result_order": [2, 1, 3],
                "model_group": ["GroupX", "GroupX", "GroupX"],
            }
        )

        result = _create_genotype_metadata_dict(df)

        assert len(result) == 3
        # All should have same effective_model_group
        assert result[("Model_B", "Carrier")]["effective_model_group"] == "GroupX"
        assert result[("Model_B", "Non-Carrier")]["effective_model_group"] == "GroupX"
        assert result[("Model_C", "Mutant")]["effective_model_group"] == "GroupX"

    def test_effective_model_group_calculation(self) -> None:
        """Test that effective_model_group is correctly calculated."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_B"],
                "genotype": ["Tg", "Carrier"],
                "display_label": ["Model_A", "Model_B"],
                "result_order": [2, 2],
                "model_group": ["", "GroupX"],
            }
        )

        result = _create_genotype_metadata_dict(df)

        # Empty model_group -> effective is model name
        assert result[("Model_A", "Tg")]["effective_model_group"] == "Model_A"
        # Non-empty model_group -> effective is model_group
        assert result[("Model_B", "Carrier")]["effective_model_group"] == "GroupX"

    def test_result_order_conversion(self) -> None:
        """Test that result_order is converted to int."""
        df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "result_order": ["2"],  # String value
                "model_group": [""],
            }
        )

        result = _create_genotype_metadata_dict(df)

        assert isinstance(result[("Model_A", "Tg")]["result_order"], int)
        assert result[("Model_A", "Tg")]["result_order"] == 2

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(
            columns=[
                "model",
                "genotype",
                "display_label",
                "result_order",
                "model_group",
            ]
        )

        result = _create_genotype_metadata_dict(df)

        assert len(result) == 0
        assert result == {}


class TestDetermineResultOrder:
    """
    Unit tests for the _determine_result_order helper function.

    This class contains focused unit tests for result order determination,
    which creates an ordered list of display labels based on result_order values.

    Test Methods:
        - test_single_model_result_order: Tests result ordering for a single model.
        - test_model_group_result_order: Tests result ordering for a model group.
        - test_result_order_sorting: Tests that display labels are sorted by result_order.
        - test_empty_genotype_metadata: Tests handling of empty metadata dictionary.
        - test_no_matching_model_group: Tests behavior when no genotypes match the model_group.
    """

    def test_single_model_result_order(self) -> None:
        """Test result order for a single model (no model_group)."""
        genotype_metadata = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
            ("Model_A", "Wt"): {
                "display_label": "Wildtype",
                "result_order": 1,
                "effective_model_group": "Model_A",
            },
        }

        result = _determine_result_order(genotype_metadata, "Model_A")

        assert result == ["Wildtype", "Transgenic"]

    def test_model_group_result_order(self) -> None:
        """Test result order for a model group with multiple models."""
        genotype_metadata = {
            ("Model_B", "Carrier"): {
                "display_label": "Model_B",
                "result_order": 2,
                "effective_model_group": "GroupX",
            },
            ("Model_B", "Non-Carrier"): {
                "display_label": "Control_B",
                "result_order": 1,
                "effective_model_group": "GroupX",
            },
            ("Model_C", "Mutant"): {
                "display_label": "Model_C",
                "result_order": 3,
                "effective_model_group": "GroupX",
            },
        }

        result = _determine_result_order(genotype_metadata, "GroupX")

        assert result == ["Control_B", "Model_B", "Model_C"]

    def test_result_order_sorting(self) -> None:
        """Test that display labels are sorted by result_order value."""
        genotype_metadata = {
            ("Model_A", "G3"): {
                "display_label": "Label_C",
                "result_order": 30,
                "effective_model_group": "Model_A",
            },
            ("Model_A", "G1"): {
                "display_label": "Label_A",
                "result_order": 10,
                "effective_model_group": "Model_A",
            },
            ("Model_A", "G2"): {
                "display_label": "Label_B",
                "result_order": 20,
                "effective_model_group": "Model_A",
            },
        }

        result = _determine_result_order(genotype_metadata, "Model_A")

        assert result == ["Label_A", "Label_B", "Label_C"]

    def test_empty_genotype_metadata(self) -> None:
        """Test handling of empty genotype metadata dictionary."""
        genotype_metadata = {}

        result = _determine_result_order(genotype_metadata, "Model_A")

        assert result == []

    def test_no_matching_model_group(self) -> None:
        """Test behavior when no genotypes match the specified model_group."""
        genotype_metadata = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
        }

        result = _determine_result_order(genotype_metadata, "Model_B")

        assert result == []


class TestCreateIndividualResultsFromGroup:
    """
    Unit tests for the _create_individual_results_from_group helper function.

    This class contains focused unit tests for individual results creation,
    which groups data by age and creates structured data points.

    Test Methods:
        - test_single_age_group: Tests creation with single age group.
        - test_multiple_age_groups: Tests creation with multiple ages.
        - test_age_sorting: Tests numeric sorting of age groups.
        - test_data_point_structure: Tests structure of individual data points.
        - test_type_conversions: Tests type conversions for output fields.
    """

    def test_single_age_group(self) -> None:
        """Test individual results creation with single age group."""
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months"],
                "genotype_display": ["Transgenic", "Wildtype"],
                "sex": ["Male", "Female"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.12345, 3.45678],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 1
        assert result[0]["age"] == "6 months"
        assert len(result[0]["data"]) == 2

    def test_multiple_age_groups(self) -> None:
        """Test individual results creation with multiple age groups."""
        group = pd.DataFrame(
            {
                "age": ["3 months", "3 months", "6 months"],
                "genotype_display": ["Transgenic", "Wildtype", "Transgenic"],
                "sex": ["Male", "Female", "Male"],
                "individualid": ["Ind001", "Ind002", "Ind003"],
                "expression": [4.0, 3.0, 5.0],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 2
        # Check ages are present
        ages = [r["age"] for r in result]
        assert "3 months" in ages
        assert "6 months" in ages

    def test_age_sorting(self) -> None:
        """Test that age groups are sorted numerically."""
        group = pd.DataFrame(
            {
                "age": ["12 months", "3 months", "6 months"],
                "genotype_display": ["Tg", "Tg", "Tg"],
                "sex": ["Male", "Male", "Male"],
                "individualid": ["Ind001", "Ind002", "Ind003"],
                "expression": [6.0, 4.0, 5.0],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 3
        assert result[0]["age"] == "3 months"
        assert result[1]["age"] == "6 months"
        assert result[2]["age"] == "12 months"

    def test_data_point_structure(self) -> None:
        """Test structure of individual data points."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype_display": ["Transgenic"],
                "sex": ["Male"],
                "individualid": ["Ind001"],
                "expression": [5.12345],
            }
        )

        result = _create_individual_results_from_group(group)

        data_point = result[0]["data"][0]
        assert "genotype" in data_point
        assert "sex" in data_point
        assert "individual_id" in data_point
        assert "value" in data_point
        assert data_point["genotype"] == "Transgenic"
        assert data_point["sex"] == "Male"
        assert data_point["individual_id"] == "Ind001"
        assert data_point["value"] == 5.12345

    def test_type_conversions(self) -> None:
        """Test that individual_id is converted to string and value to float."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype_display": ["Transgenic"],
                "sex": ["Male"],
                "individualid": [12345],  # Integer ID
                "expression": ["5.12345"],  # String expression
            }
        )

        result = _create_individual_results_from_group(group)

        data_point = result[0]["data"][0]
        assert isinstance(data_point["individual_id"], str)
        assert isinstance(data_point["value"], float)
        assert data_point["individual_id"] == "12345"
        assert data_point["value"] == pytest.approx(5.12345)


class TestCreateOutputEntryFromGroup:
    """
    Unit tests for the _create_output_entry_from_group helper function.

    This class contains focused unit tests for output entry creation logic,
    which creates complete output entries with all metadata.

    Test Methods:
        - test_basic_output_entry: Tests basic output entry creation.
        - test_jax_tissue_mapping: Tests JAX tissue name mapping.
        - test_matched_control_determination: Tests matched control selection.
        - test_model_group_handling: Tests model_group vs null handling.
        - test_multiple_ages_creates_multiple_entries: Tests that each age creates a separate entry.
    """

    def test_basic_output_entry(self) -> None:
        """Test basic output entry creation with all fields."""
        group_key = ("ENSMUSG00000000001", "Cortex", "", "Model_A")
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months"],
                "genotype": ["Tg", "Wt"],
                "genotype_display": ["Transgenic", "Wildtype"],
                "sex": ["Male", "Female"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 3.0],
                "result_order": [2, 1],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
            ("Model_A", "Wt"): {
                "display_label": "Wildtype",
                "result_order": 1,
                "effective_model_group": "Model_A",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert len(result) == 1  # One age group
        entry = result[0]
        assert entry["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert entry["gene_symbol"] == "Gene1"
        assert entry["tissue"] == "Cortex"
        assert entry["name"] == "Model_A"
        assert entry["model_group"] is None  # Empty string converted to None
        assert entry["matched_control"] == "Wildtype"
        assert entry["units"] == "Log2 Counts per Million"
        assert entry["age"] == "6 months"
        assert entry["age_numeric"] == 6
        assert entry["result_order"] == ["Wildtype", "Transgenic"]
        assert len(entry["data"]) == 2

    def test_jax_tissue_mapping(self) -> None:
        """Test that JAX tissue name is mapped correctly."""
        group_key = ("ENSMUSG00000000001", "Right Cerebral Hemisphere", "", "Model_A")
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype": ["Tg"],
                "genotype_display": ["Transgenic"],
                "sex": ["Male"],
                "individualid": ["Ind001"],
                "expression": [5.0],
                "result_order": [2],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert result[0]["tissue"] == "Hemibrain"

    def test_matched_control_determination(self) -> None:
        """Test that matched control is the genotype with minimum result_order."""
        group_key = ("ENSMUSG00000000001", "Cortex", "GroupX", "Model_B")
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months", "6 months"],
                "genotype": ["Carrier", "Non-Carrier", "Mutant"],
                "genotype_display": ["Model_B", "Control_B", "Model_C"],
                "sex": ["Male", "Female", "Male"],
                "individualid": ["Ind001", "Ind002", "Ind003"],
                "expression": [5.0, 3.0, 6.0],
                "result_order": [2, 1, 3],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        genotype_metadata_dict = {
            ("Model_B", "Carrier"): {
                "display_label": "Model_B",
                "result_order": 2,
                "effective_model_group": "GroupX",
            },
            ("Model_B", "Non-Carrier"): {
                "display_label": "Control_B",
                "result_order": 1,
                "effective_model_group": "GroupX",
            },
            ("Model_C", "Mutant"): {
                "display_label": "Model_C",
                "result_order": 3,
                "effective_model_group": "GroupX",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert result[0]["matched_control"] == "Control_B"

    def test_model_group_handling(self) -> None:
        """Test that model_group is properly set (null for empty, value for non-empty)."""
        # Test with empty model_group
        group_key = ("ENSMUSG00000000001", "Cortex", "", "Model_A")
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype": ["Tg"],
                "genotype_display": ["Transgenic"],
                "sex": ["Male"],
                "individualid": ["Ind001"],
                "expression": [5.0],
                "result_order": [2],
            }
        )

        gene_metadata_dict = {}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert result[0]["model_group"] is None

        # Test with non-empty model_group
        group_key = ("ENSMUSG00000000001", "Cortex", "GroupX", "Model_B")
        genotype_metadata_dict = {
            ("Model_B", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "GroupX",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert result[0]["model_group"] == "GroupX"

    def test_multiple_ages_creates_multiple_entries(self) -> None:
        """Test that multiple ages create separate output entries."""
        group_key = ("ENSMUSG00000000001", "Cortex", "", "Model_A")
        group = pd.DataFrame(
            {
                "age": ["3 months", "6 months"],
                "genotype": ["Tg", "Tg"],
                "genotype_display": ["Transgenic", "Transgenic"],
                "sex": ["Male", "Male"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [4.0, 5.0],
                "result_order": [2, 2],
            }
        )

        gene_metadata_dict = {}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "effective_model_group": "Model_A",
            },
        }

        result = _create_output_entry_from_group(
            group_key, group, gene_metadata_dict, genotype_metadata_dict
        )

        assert len(result) == 2  # Two separate entries
        ages = [entry["age"] for entry in result]
        assert "3 months" in ages
        assert "6 months" in ages


class TestProcessSingleDataFile:
    """
    Unit tests for the _process_single_data_file helper function.

    This class contains focused unit tests for single file processing logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_basic_file_processing: Tests basic file processing.
        - test_empty_file_raises_error: Tests error handling for empty files.
        - test_filters_human_genes: Tests filtering of human genes.
        - test_rounding: Tests numeric rounding to 5 decimal places.
        - test_genotype_filtering: Tests filtering of invalid genotypes.
    """

    def test_basic_file_processing(self) -> None:
        """Test basic processing of a single data file."""
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
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_single_data_file(
            "test_file.csv",
            data_file,
            gene_metadata_dict,
            genotype_metadata_dict,
            0,
            1,
        )

        assert len(result) == 1
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"

    def test_empty_file_raises_error(self) -> None:
        """Test that empty file raises ValueError."""
        data_file = pd.DataFrame()

        gene_metadata_dict = {}
        genotype_metadata_dict = {}

        with pytest.raises(ValueError, match="is empty"):
            _process_single_data_file(
                "empty_file.csv",
                data_file,
                gene_metadata_dict,
                genotype_metadata_dict,
                0,
                1,
            )

    def test_filters_human_genes(self) -> None:
        """Test that human genes (ENSG*) are filtered out."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSG00000000001"],
                "individualid": ["Ind001", "Ind002"],
                "expression": [5.0, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["Tg", "Tg"],
                "model": ["Model_A", "Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_single_data_file(
            "mixed_genes.csv",
            data_file,
            gene_metadata_dict,
            genotype_metadata_dict,
            0,
            1,
        )

        # Should only have 1 entry (mouse gene only)
        assert len(result) == 1
        assert result[0]["ensembl_gene_id"].startswith("ENSMUSG")

    def test_rounding(self) -> None:
        """Test that numeric values are rounded to 5 decimal places."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["Ind001"],
                "expression": [1.123456789],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["Tg"],
                "model": ["Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_single_data_file(
            "rounding_test.csv",
            data_file,
            gene_metadata_dict,
            genotype_metadata_dict,
            0,
            1,
        )

        # Value should be rounded to 5 decimal places
        assert result[0]["data"][0]["value"] == pytest.approx(1.12346, abs=1e-6)

    def test_genotype_filtering(self) -> None:
        """Test that genotypes not in the model_group are filtered out."""
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
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_single_data_file(
            "genotype_filter_test.csv",
            data_file,
            gene_metadata_dict,
            genotype_metadata_dict,
            0,
            1,
        )

        # Should only have 1 data point (valid genotype only)
        assert len(result) == 1
        assert len(result[0]["data"]) == 1
        assert result[0]["data"][0]["genotype"] == "Transgenic"


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
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_metadata_dict
        )

        assert len(result) == 1
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert result[0]["gene_symbol"] == "Gene1"
        assert len(result[0]["data"]) == 1
        assert result[0]["data"][0]["genotype"] == "Transgenic"

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
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_metadata_dict
        )

        # Should only have 1 data point (valid genotype only)
        assert len(result) == 1
        assert len(result[0]["data"]) == 1
        assert result[0]["data"][0]["genotype"] == "Transgenic"

    def test_uses_preprocessed_data(self) -> None:
        """Test that function works with preprocessed data (no human genes, rounded values)."""
        # Data should already be filtered (no human genes) and rounded
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["Ind001"],
                "expression": [1.12346],  # Already rounded to 5 decimals
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["Tg"],
                "model": ["Model_A"],
            }
        )

        gene_metadata_dict = {}
        genotype_metadata_dict = {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "result_order": 2,
                "model_group": "",
                "effective_model_group": "Model_A",
            },
        }

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_metadata_dict
        )

        # Value should remain as provided (already preprocessed)
        assert result[0]["data"][0]["value"] == pytest.approx(1.12346, abs=1e-6)

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
        genotype_metadata_dict = {
            ("Model_B", "Carrier"): {
                "display_label": "Model_B",
                "result_order": 2,
                "model_group": "GroupX",
                "effective_model_group": "GroupX",
            },
            ("Model_B", "Non-Carrier"): {
                "display_label": "Control_B",
                "result_order": 1,
                "model_group": "GroupX",
                "effective_model_group": "GroupX",
            },
        }

        result = _process_individual_data_file_core(
            data_file, gene_metadata_dict, genotype_metadata_dict
        )

        # Should have 1 entry with 2 data points
        assert len(result) == 1
        assert len(result[0]["data"]) == 2
        assert result[0]["model_group"] == "GroupX"
        assert result[0]["matched_control"] == "Control_B"


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
        - test_synthetic_rounding_precision: Tests 5-decimal-place rounding.
        - test_inconsistent_model_group_values: Tests error handling for inconsistent model_group values.

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
                "rnaseq_genotype_label_map.csv",
                # Missing mouse_gene_metadata.csv
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
            "rnaseq_genotype_label_map.csv": "rnaseq_genotype_label_map",
            "rnaseq_genotype_label_map_inconsistent.csv": "rnaseq_genotype_label_map",
            "mouse_gene_metadata.csv": "mouse_gene_metadata",
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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        mouse_gene_metadata.csv, so gene_symbol should be "".
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_single_row_data.csv",
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
            ]
        )

        # Expect transformation to raise ValueError for missing required columns
        with pytest.raises(ValueError, match="Missing required columns"):
            transform_rna_de_individual(datasets=datasets)

    def test_synthetic_rounding_precision(self) -> None:
        """Test that expression values are rounded to 5 decimal places.

        Tests numeric precision by providing values with 7+ decimal places and verifying
        they are correctly rounded to exactly 5 decimal places in the output.
        """
        # Load synthetic test data with high-precision values
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rounding_precision_data.csv",
                "rnaseq_genotype_label_map.csv",
                "mouse_gene_metadata.csv",
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
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Explicitly verify rounding (1.123456789 -> 1.12346, 2.987654321 -> 2.98765)
        assert output_data_sorted[0]["data"][0]["value"] == pytest.approx(1.12346)
        assert output_data_sorted[0]["data"][1]["value"] == pytest.approx(2.98765)

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
                "mouse_gene_metadata.csv",
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
