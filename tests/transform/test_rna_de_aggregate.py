"""
Test suite for RNA differential expression aggregate transformation.

This module contains comprehensive tests for the `transform_rna_de_aggregate` function
and its helper functions, which aggregate mouse model RNA-seq differential expression
data into a structured format for the Agora platform.

Test Classes:
    - TestValidateAndSortAgeEntries: Unit tests for the _validate_and_sort_age_entries helper function
    - TestCreateAgeEntriesFromGroup: Unit tests for the _create_age_entries_from_group helper function
    - TestCreateOutputEntryFromGroup: Unit tests for the _create_output_entry_from_group helper function
    - TestProcessSingleDataFile: Unit tests for the _process_single_data_file helper function
    - TestTransformRnaDeAggregate: Integration tests for the full transformation pipeline

The tests use synthetic datasets stored in `tests/test_assets/rna_de_aggregate/` to verify:
- Core transformation logic (data aggregation, metadata enrichment)
- Multi-model and multi-tissue handling
- JAX tissue name mapping (e.g., 'Right Cerebral Hemisphere' -> 'Hemibrain')
- Human gene filtering (only mouse genes with ENSMUSG* IDs should be processed)
- Age sorting (numeric ordering of age entries)
- Age validation (empty strings, whitespace, invalid formats)
- NaN handling (NaN adjusted p-values are coerced to 0.0)
- Negative zero handling (negative zero log2foldchange values are normalized to positive zero)
- Negative p-value validation (negative adjusted p-values raise ValueError)
- Edge cases (single row data, missing metadata, empty biodomains)
- Error handling (missing datasets, empty files, missing columns, invalid age formats, inconsistent model_group values, inconsistent model_type values)
- Data precision (rounding to 5 decimal places)
- Multiple biodomain assignments per gene
- Null/empty model_group handling

Test Data Structure:
    Input files include:
    - RNA-seq differential expression data (*.csv)
    - genotype_label_map.csv (maps genotypes to model labels, includes model_type)
    - mouse_gene_metadata.csv (gene symbols and metadata)
    - biodom_genes_mm.csv (biodomain assignments for mouse genes)

    Output files are JSON-formatted expected results for comparison.
"""

import os
import json
import math
from typing import Dict, List
import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_aggregate import (
    transform_rna_de_aggregate,
    _validate_and_sort_age_entries,
    _create_age_entries_from_group,
    _create_output_entry_from_group,
    _process_single_data_file,
)


class TestValidateAndSortAgeEntries:
    """
    Unit tests for the _validate_and_sort_age_entries helper function.

    This class contains focused unit tests for age validation and sorting logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_valid_single_age: Tests sorting with a single valid age entry.
        - test_valid_multiple_ages_sorted: Tests sorting multiple ages in numeric order.
        - test_valid_multiple_ages_unsorted: Tests sorting handles unsorted input correctly.
        - test_valid_ages_with_extra_whitespace: Tests handling of extra whitespace in age strings.
        - test_empty_string_age: Tests error handling for empty string age values.
        - test_whitespace_only_age: Tests error handling for whitespace-only age values.
        - test_age_without_space: Tests error handling for age format without space (e.g., "6months").
        - test_non_numeric_age: Tests error handling for non-numeric age values.
        - test_negative_age: Tests error handling for negative age values.
        - test_float_age: Tests error handling for float age values.
        - test_age_with_wrong_unit: Tests handling of age with units other than "months".
        - test_empty_age_entries_dict: Tests handling of empty age entries dictionary.
    """

    def test_valid_single_age(self) -> None:
        """Test that a single valid age entry is handled correctly."""
        age_entries = {"6 months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        assert result == {"6 months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

    def test_valid_multiple_ages_sorted(self) -> None:
        """Test that multiple ages already in order remain sorted."""
        age_entries = {
            "4 months": {"log2_fc": 0.3, "adj_p_val": 0.02},
            "6 months": {"log2_fc": 0.5, "adj_p_val": 0.01},
            "12 months": {"log2_fc": 0.8, "adj_p_val": 0.005},
        }

        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # Verify order is preserved
        assert list(result.keys()) == ["4 months", "6 months", "12 months"]

    def test_valid_multiple_ages_unsorted(self) -> None:
        """Test that multiple unsorted ages are sorted numerically."""
        age_entries = {
            "12 months": {"log2_fc": 0.8, "adj_p_val": 0.005},
            "4 months": {"log2_fc": 0.3, "adj_p_val": 0.02},
            "6 months": {"log2_fc": 0.5, "adj_p_val": 0.01},
        }

        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # Verify ages are sorted numerically
        assert list(result.keys()) == ["4 months", "6 months", "12 months"]
        assert result["4 months"] == {"log2_fc": 0.3, "adj_p_val": 0.02}
        assert result["6 months"] == {"log2_fc": 0.5, "adj_p_val": 0.01}
        assert result["12 months"] == {"log2_fc": 0.8, "adj_p_val": 0.005}

    def test_valid_ages_with_extra_whitespace(self) -> None:
        """Test that ages with extra whitespace are handled correctly."""
        # Note: The function receives age after str() conversion,
        # so extra whitespace in the middle is preserved
        age_entries = {
            "6  months": {"log2_fc": 0.5, "adj_p_val": 0.01},  # Two spaces
        }

        # This should work because split()[0] handles multiple spaces
        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        assert "6  months" in result

    def test_empty_string_age(self) -> None:
        """Test that empty string age raises ValueError with specific message."""
        age_entries = {"": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        with pytest.raises(ValueError) as exc_info:
            _validate_and_sort_age_entries(
                age_entries=age_entries,
                ensembl_gene_id="ENSMUSG00000000001",
                model="TestModel",
                tissue="Cortex",
                sex="Male",
            )

        error_message = str(exc_info.value)
        assert "Empty or whitespace-only age value" in error_message
        assert "ENSMUSG00000000001" in error_message
        assert "TestModel" in error_message
        assert "Cortex" in error_message
        assert "Male" in error_message

    def test_whitespace_only_age(self) -> None:
        """Test that whitespace-only age raises ValueError with specific message."""
        age_entries = {"   ": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        with pytest.raises(ValueError) as exc_info:
            _validate_and_sort_age_entries(
                age_entries=age_entries,
                ensembl_gene_id="ENSMUSG00000000002",
                model="Model_X",
                tissue="Hippocampus",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Empty or whitespace-only age value" in error_message
        assert "ENSMUSG00000000002" in error_message
        assert "Model_X" in error_message
        assert "Hippocampus" in error_message
        assert "Female" in error_message

    def test_age_without_space(self) -> None:
        """Test that age format without space raises ValueError."""
        age_entries = {"6months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        with pytest.raises(ValueError) as exc_info:
            _validate_and_sort_age_entries(
                age_entries=age_entries,
                ensembl_gene_id="ENSMUSG00000000004",
                model="Model_Z",
                tissue="Striatum",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Invalid age format" in error_message
        assert "ENSMUSG00000000004" in error_message
        assert "Model_Z" in error_message
        assert "Expected 'N months' format" in error_message
        assert "6months" in error_message

    def test_non_numeric_age(self) -> None:
        """Test that non-numeric age value raises ValueError."""
        age_entries = {"unknown months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        with pytest.raises(ValueError) as exc_info:
            _validate_and_sort_age_entries(
                age_entries=age_entries,
                ensembl_gene_id="ENSMUSG00000000005",
                model="Model_A",
                tissue="Thalamus",
                sex="Male",
            )

        error_message = str(exc_info.value)
        assert "Invalid age format" in error_message
        assert "ENSMUSG00000000005" in error_message
        assert "Expected 'N months' format" in error_message
        assert "unknown months" in error_message

    def test_negative_age(self) -> None:
        """Test that negative age value raises ValueError."""
        age_entries = {"-6 months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        # This actually won't raise an error because int("-6") works
        # But it will sort correctly
        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000006",
            model="Model_B",
            tissue="Cortex",
            sex="Female",
        )

        assert "-6 months" in result

    def test_float_age(self) -> None:
        """Test that float age value raises ValueError."""
        age_entries = {"6.5 months": {"log2_fc": 0.5, "adj_p_val": 0.01}}

        # This will raise because int("6.5") fails
        with pytest.raises(ValueError) as exc_info:
            _validate_and_sort_age_entries(
                age_entries=age_entries,
                ensembl_gene_id="ENSMUSG00000000007",
                model="Model_C",
                tissue="Cortex",
                sex="Male",
            )

        error_message = str(exc_info.value)
        assert "Invalid age format" in error_message
        assert "6.5 months" in error_message

    def test_age_with_wrong_unit(self) -> None:
        """Test that age with units other than 'months' still works if numeric."""
        # The function only validates the numeric part, not the unit
        age_entries = {
            "6 years": {"log2_fc": 0.5, "adj_p_val": 0.01},
            "12 days": {"log2_fc": 0.3, "adj_p_val": 0.02},
        }

        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000008",
            model="Model_D",
            tissue="Cortex",
            sex="Female",
        )

        # Should sort by numeric value
        assert list(result.keys()) == ["6 years", "12 days"]

    def test_empty_age_entries_dict(self) -> None:
        """Test that empty age_entries dictionary is handled gracefully."""
        age_entries = {}

        result = _validate_and_sort_age_entries(
            age_entries=age_entries,
            ensembl_gene_id="ENSMUSG00000000009",
            model="Model_E",
            tissue="Cortex",
            sex="Male",
        )

        assert result == {}


class TestCreateAgeEntriesFromGroup:
    """
    Unit tests for the _create_age_entries_from_group helper function.

    This class contains focused unit tests for age entry creation logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_create_age_entries_single_row: Tests creating age entries from a single row.
        - test_create_age_entries_multiple_rows: Tests creating age entries from multiple rows.
        - test_create_age_entries_with_nan_padj: Tests handling of NaN adjusted p-values.
        - test_create_age_entries_with_negative_zero: Tests handling of negative zero log2foldchange.
        - test_create_age_entries_negative_padj_raises_error: Tests error handling for negative p-values.
    """

    def test_create_age_entries_single_row(self) -> None:
        """Test creating age entries from a single row DataFrame."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [0.01],
            }
        )

        result = _create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        assert result == {"6 months": {"log2_fc": 1.5, "adj_p_val": 0.01}}

    def test_create_age_entries_multiple_rows(self) -> None:
        """Test creating age entries from multiple rows with different ages."""
        group = pd.DataFrame(
            {
                "age": ["3 months", "6 months", "12 months"],
                "log2foldchange": [0.5, 1.0, 1.5],
                "padj": [0.05, 0.01, 0.001],
            }
        )

        result = _create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000002",
            model="TestModel",
            tissue="Hippocampus",
            sex="Female",
        )

        assert len(result) == 3
        assert "3 months" in result
        assert "6 months" in result
        assert "12 months" in result
        assert result["3 months"] == {"log2_fc": 0.5, "adj_p_val": 0.05}
        assert result["6 months"] == {"log2_fc": 1.0, "adj_p_val": 0.01}
        assert result["12 months"] == {"log2_fc": 1.5, "adj_p_val": 0.001}

    def test_create_age_entries_with_nan_padj(self) -> None:
        """Test that NaN adjusted p-values are converted to 0.0.

        This verifies that:
        - NA padj values are converted to 0.0
        - NA values don't trigger negative p-value validation check
        - The NA check happens before float conversion, preventing TypeError
          when pd.NA is passed to float()
        - log2_fc values are still processed correctly when padj is NA
        """
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [pd.NA],
            }
        )

        # Should not raise TypeError or ValueError, should handle NA gracefully
        result = _create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000003",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # NA should be converted to 1.0 without errors
        assert result["6 months"]["adj_p_val"] == pytest.approx(1.0)
        assert result["6 months"]["log2_fc"] == pytest.approx(1.5)

    def test_create_age_entries_mixed_na_and_valid_padj(self) -> None:
        """Test that groups with both NA and valid padj values are handled correctly.

        This verifies that NA values are skipped in negative check but valid
        negative values still raise errors.
        """
        # Group with NA and valid positive values
        group_valid = pd.DataFrame(
            {
                "age": ["3 months", "6 months"],
                "log2foldchange": [0.5, 1.5],
                "padj": [pd.NA, 0.01],
            }
        )

        result = _create_age_entries_from_group(
            group=group_valid,
            ensembl_gene_id="ENSMUSG00000000010",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # Both entries should be created without errors
        assert len(result) == 2
        assert result["3 months"]["adj_p_val"] == pytest.approx(1.0)
        assert result["6 months"]["adj_p_val"] == pytest.approx(0.01)

        # Group with NA and negative value should still raise error for negative
        group_negative = pd.DataFrame(
            {
                "age": ["3 months", "6 months"],
                "log2foldchange": [0.5, 1.5],
                "padj": [pd.NA, -0.01],
            }
        )

        with pytest.raises(ValueError) as exc_info:
            _create_age_entries_from_group(
                group=group_negative,
                ensembl_gene_id="ENSMUSG00000000011",
                model="TestModel",
                tissue="Cortex",
                sex="Male",
            )

        # Should still catch negative p-value even when NA is present
        error_message = str(exc_info.value)
        assert "Negative adjusted p-value found in data" in error_message
        assert "ENSMUSG00000000011" in error_message

    def test_create_age_entries_with_negative_zero(self) -> None:
        """Test that negative zero log2foldchange is normalized to positive zero."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [-0.0],
                "padj": [0.01],
            }
        )

        result = _create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000004",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # normalize_zero should convert -0.0 to 0.0
        assert math.isclose(result["6 months"]["log2_fc"], 0.0, abs_tol=1e-12)
        assert math.copysign(1.0, result["6 months"]["log2_fc"]) > 0

    def test_create_age_entries_negative_padj_raises_error(self) -> None:
        """Test that negative adjusted p-values raise ValueError with informative message."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [-0.01],
            }
        )

        with pytest.raises(ValueError) as exc_info:
            _create_age_entries_from_group(
                group=group,
                ensembl_gene_id="ENSMUSG00000000005",
                model="TestModel",
                tissue="Cortex",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Negative adjusted p-value found in data" in error_message
        assert "ENSMUSG00000000005" in error_message
        assert "TestModel" in error_message
        assert "Cortex" in error_message
        assert "Female" in error_message


class TestCreateOutputEntryFromGroup:
    """
    Unit tests for the _create_output_entry_from_group helper function.

    This class contains focused unit tests for output entry creation logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_create_output_entry_basic: Tests basic output entry creation.
        - test_create_output_entry_missing_metadata: Tests handling of missing metadata.
        - test_create_output_entry_jax_tissue_mapping: Tests JAX tissue name mapping.
        - test_create_output_entry_empty_model_group: Tests empty model_group conversion to None.
        - test_create_output_entry_multiple_biodomains: Tests multiple biodomain assignments.
    """

    def test_create_output_entry_basic(self) -> None:
        """Test basic output entry creation with all metadata present."""
        group_key = ("ENSMUSG00000000001", "Model_A", "Cortex", "Male", "Tg", "Wt")
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [0.01],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }
        model_group_dict = {"Model_A": "Group1"}
        biodomain_dict = {"ENSMUSG00000000001": ["Synaptic"]}
        model_type_dict = {"Model_A": "knockout"}

        result = _create_output_entry_from_group(
            group_key=group_key,
            group=group,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
        )

        assert result["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert result["gene_symbol"] == "Gene1"
        assert result["biodomains"] == ["Synaptic"]
        assert result["name"] == {
            "link_url": "models/Transgenic",
            "link_text": "Transgenic",
        }
        assert result["matched_control"] == "Wildtype"
        assert result["model_group"] == "Group1"
        assert result["model_type"] == "knockout"
        assert result["tissue"] == "Cortex"
        assert result["sex"] == "Male"
        assert "6 months" in result
        assert result["6 months"]["log2_fc"] == pytest.approx(1.5)
        assert result["6 months"]["adj_p_val"] == pytest.approx(0.01)

    def test_create_output_entry_missing_metadata(self) -> None:
        """Test output entry creation with missing metadata (empty strings/lists for gene_metadata, biodomain, model_type).

        Note: label_map_dict must contain entries for the genotypes used, as the function
        now raises an error if they are missing.
        """
        group_key = (
            "ENSMUSG00000000002",
            "Model_B",
            "Hippocampus",
            "Female",
            "Tg",
            "Wt",
        )
        group = pd.DataFrame(
            {
                "age": ["3 months"],
                "log2foldchange": [0.5],
                "padj": [0.05],
            }
        )

        # Empty dictionaries simulate missing metadata
        # label_map_dict must have entries for genotypes to avoid errors
        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_B", "Tg"): "Transgenic_B",
            ("Model_B", "Wt"): "Wildtype_B",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        result = _create_output_entry_from_group(
            group_key=group_key,
            group=group,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
        )

        assert result["ensembl_gene_id"] == "ENSMUSG00000000002"
        assert result["gene_symbol"] == ""  # Default for missing
        assert result["biodomains"] == []  # Default for missing
        assert result["name"] == {
            "link_url": "models/Transgenic_B",
            "link_text": "Transgenic_B",
        }  # Falls back to case genotype
        assert result["matched_control"] == "Wildtype_B"  # From label_map_dict
        assert result["model_group"] is None  # Default for missing model_group
        assert result["model_type"] == ""  # Default for missing model_type
        assert result["tissue"] == "Hippocampus"
        assert result["sex"] == "Female"

    def test_create_output_entry_jax_tissue_mapping(self) -> None:
        """Test that JAX tissue name 'Right Cerebral Hemisphere' is mapped to 'Hemibrain'."""
        group_key = (
            "ENSMUSG00000000003",
            "Model_C",
            "Right Cerebral Hemisphere",
            "Male",
            "Tg",
            "Wt",
        )
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.0],
                "padj": [0.01],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000003": "Gene3"}
        label_map_dict = {
            ("Model_C", "Tg"): "Transgenic_C",
            ("Model_C", "Wt"): "Wildtype_C",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        result = _create_output_entry_from_group(
            group_key=group_key,
            group=group,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
        )

        assert result["tissue"] == "Hemibrain"  # Should be mapped

    def test_create_output_entry_multiple_biodomains(self) -> None:
        """Test output entry with multiple biodomain assignments."""
        group_key = ("ENSMUSG00000000005", "Model_E", "Cortex", "Male", "Tg", "Wt")
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [0.01],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_E", "Tg"): "Transgenic_E",
            ("Model_E", "Wt"): "Wildtype_E",
        }
        model_group_dict = {}
        biodomain_dict = {"ENSMUSG00000000005": ["Synaptic", "Metabolic"]}
        model_type_dict = {}

        result = _create_output_entry_from_group(
            group_key=group_key,
            group=group,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
        )

        assert len(result["biodomains"]) == 2
        assert set(result["biodomains"]) == {"Synaptic", "Metabolic"}

    def test_create_output_entry_missing_case_label_raises_error(self) -> None:
        """Test that missing case genotype in label_map_dict raises ValueError."""
        group_key = ("ENSMUSG00000000006", "Model_F", "Cortex", "Male", "Tg", "Wt")
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [0.01],
            }
        )

        gene_metadata_dict = {}
        # label_map_dict is missing entry for ("Model_F", "Tg")
        label_map_dict = {
            ("Model_F", "Wt"): "Wildtype",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        with pytest.raises(ValueError) as exc_info:
            _create_output_entry_from_group(
                group_key=group_key,
                group=group,
                gene_metadata_dict=gene_metadata_dict,
                label_map_dict=label_map_dict,
                model_group_dict=model_group_dict,
                biodomain_dict=biodomain_dict,
                model_type_dict=model_type_dict,
            )

        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_F" in error_message
        assert "Tg" in error_message
        assert "ENSMUSG00000000006" in error_message
        assert "Cortex" in error_message
        assert "Male" in error_message

    def test_create_output_entry_missing_control_label_raises_error(self) -> None:
        """Test that missing control genotype in label_map_dict raises ValueError."""
        group_key = (
            "ENSMUSG00000000007",
            "Model_G",
            "Hippocampus",
            "Female",
            "Tg",
            "Wt",
        )
        group = pd.DataFrame(
            {
                "age": ["3 months"],
                "log2foldchange": [2.0],
                "padj": [0.001],
            }
        )

        gene_metadata_dict = {}
        # label_map_dict is missing entry for ("Model_G", "Wt")
        label_map_dict = {
            ("Model_G", "Tg"): "Transgenic",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        with pytest.raises(ValueError) as exc_info:
            _create_output_entry_from_group(
                group_key=group_key,
                group=group,
                gene_metadata_dict=gene_metadata_dict,
                label_map_dict=label_map_dict,
                model_group_dict=model_group_dict,
                biodomain_dict=biodomain_dict,
                model_type_dict=model_type_dict,
            )

        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_G" in error_message
        assert "Wt" in error_message
        assert "ENSMUSG00000000007" in error_message
        assert "Hippocampus" in error_message
        assert "Female" in error_message

    def test_create_output_entry_missing_both_labels_raises_case_error_first(
        self,
    ) -> None:
        """Test that when both case and control are missing, case error is raised first."""
        group_key = ("ENSMUSG00000000008", "Model_H", "Cortex", "Male", "Tg", "Wt")
        group = pd.DataFrame(
            {
                "age": ["12 months"],
                "log2foldchange": [0.5],
                "padj": [0.05],
            }
        )

        gene_metadata_dict = {}
        # label_map_dict is completely empty
        label_map_dict = {}
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        with pytest.raises(ValueError) as exc_info:
            _create_output_entry_from_group(
                group_key=group_key,
                group=group,
                gene_metadata_dict=gene_metadata_dict,
                label_map_dict=label_map_dict,
                model_group_dict=model_group_dict,
                biodomain_dict=biodomain_dict,
                model_type_dict=model_type_dict,
            )

        # Should raise error for case first, since case is checked before control
        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_H" in error_message
        assert "Tg" in error_message


class TestProcessSingleDataFile:
    """
    Unit tests for the _process_single_data_file helper function.

    This class contains focused unit tests for single file processing logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_process_single_data_file_basic: Tests basic file processing.
        - test_process_single_data_file_empty_raises_error: Tests error handling for empty files.
        - test_process_single_data_file_filters_human_genes: Tests filtering of human genes.
        - test_process_single_data_file_filters_combined_sex_cohort: Tests filtering of combined-cohort (Females & Males) rows.
        - test_process_single_data_file_rounding: Tests numeric rounding to 5 decimal places.
        - test_process_single_data_file_multiple_groups: Tests processing multiple groups.
    """

    def test_process_single_data_file_basic(self) -> None:
        """Test basic processing of a single data file."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "log2foldchange": [1.5],
                "padj": [0.01],
                "model": ["Model_A"],
                "case": ["Tg"],
                "control": ["Wt"],
                "age": ["6 months"],
                "sex": ["Male"],
                "tissue": ["Cortex"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }
        model_group_dict = {"Model_A": "Group1"}
        biodomain_dict = {"ENSMUSG00000000001": ["Synaptic"]}
        model_type_dict = {"Model_A": "knockout"}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
            file_index=0,
            total_files=1,
        )

        assert len(result) == 1
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert result[0]["gene_symbol"] == "Gene1"

    def test_process_single_data_file_empty_raises_error(self) -> None:
        """Test that processing an empty file raises ValueError with correct message.

        This verifies that the empty file check happens before column validation,
        ensuring the error message is about the file being empty, not about missing columns.
        """
        data_file = pd.DataFrame()

        gene_metadata_dict = {}
        label_map_dict = {}  # Empty is OK since no data will be processed
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        with pytest.raises(ValueError) as exc_info:
            _process_single_data_file(
                file_name="empty_file.csv",
                data_file=data_file,
                data_file_required_columns=data_file_required_columns,
                gene_metadata_dict=gene_metadata_dict,
                label_map_dict=label_map_dict,
                model_group_dict=model_group_dict,
                biodomain_dict=biodomain_dict,
                model_type_dict=model_type_dict,
                file_index=0,
                total_files=1,
            )

        # Verify the error message is about empty file, not missing columns
        error_message = str(exc_info.value)
        assert "Data file empty_file.csv is empty" in error_message
        assert "Missing required columns" not in error_message

    def test_process_single_data_file_filters_human_genes(self) -> None:
        """Test that human genes (ENSG*) are filtered out, only mouse genes (ENSMUSG*) are kept."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSG00000000001",
                    "ENSMUSG00000000002",
                ],
                "log2foldchange": [1.5, 2.0, 0.5],
                "padj": [0.01, 0.02, 0.03],
                "model": ["Model_A", "Model_A", "Model_A"],
                "case": ["Tg", "Tg", "Tg"],
                "control": ["Wt", "Wt", "Wt"],
                "age": ["6 months", "6 months", "6 months"],
                "sex": ["Male", "Male", "Male"],
                "tissue": ["Cortex", "Cortex", "Cortex"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        result = _process_single_data_file(
            file_name="mixed_genes.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
            file_index=0,
            total_files=1,
        )

        # Should only have 2 entries (mouse genes only)
        assert len(result) == 2
        assert all(entry["ensembl_gene_id"].startswith("ENSMUSG") for entry in result)
        assert not any(entry["ensembl_gene_id"].startswith("ENSG") for entry in result)

    def test_process_single_data_file_filters_combined_sex_cohort(self) -> None:
        """Test that combined-cohort rows (sex == "Females & Males") are filtered out."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000002",
                    "ENSMUSG00000000003",
                ],
                "log2foldchange": [1.5, 2.0, 0.5],
                "padj": [0.01, 0.02, 0.03],
                "model": ["Model_A", "Model_A", "Model_A"],
                "case": ["Tg", "Tg", "Tg"],
                "control": ["Wt", "Wt", "Wt"],
                "age": ["6 months", "6 months", "6 months"],
                "sex": ["Males", "Females", "Females & Males"],
                "tissue": ["Cortex", "Cortex", "Cortex"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        result = _process_single_data_file(
            file_name="sex_cohort.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
            file_index=0,
            total_files=1,
        )

        # Should only have 2 entries (single-sex rows only), with singular sex labels
        assert len(result) == 2
        assert {entry["sex"] for entry in result} == {"Male", "Female"}

    def test_process_single_data_file_rounding(self) -> None:
        """Test that numeric values are rounded to 5 decimal places."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "log2foldchange": [1.123456789],
                "padj": [0.0123456789],
                "model": ["Model_A"],
                "case": ["Tg"],
                "control": ["Wt"],
                "age": ["6 months"],
                "sex": ["Male"],
                "tissue": ["Cortex"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        result = _process_single_data_file(
            file_name="rounding_test.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
            file_index=0,
            total_files=1,
        )

        # Values should be rounded to 5 decimal places
        assert result[0]["6 months"]["log2_fc"] == pytest.approx(1.12346, abs=1e-6)
        assert result[0]["6 months"]["adj_p_val"] == pytest.approx(0.01235, abs=1e-6)

    def test_process_single_data_file_multiple_groups(self) -> None:
        """Test processing a file with multiple groups (different genes/models/tissues)."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                "log2foldchange": [1.5, 0.5],
                "padj": [0.01, 0.05],
                "model": ["Model_A", "Model_B"],
                "case": ["Tg", "Tg"],
                "control": ["Wt", "Wt"],
                "age": ["6 months", "3 months"],
                "sex": ["Male", "Female"],
                "tissue": ["Cortex", "Hippocampus"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic_A",
            ("Model_A", "Wt"): "Wildtype_A",
            ("Model_B", "Tg"): "Transgenic_B",
            ("Model_B", "Wt"): "Wildtype_B",
        }
        model_group_dict = {}
        biodomain_dict = {}
        model_type_dict = {}

        data_file_required_columns = [
            "ensembl_gene_id",
            "log2foldchange",
            "padj",
            "model",
            "case",
            "control",
            "age",
            "sex",
            "tissue",
        ]

        result = _process_single_data_file(
            file_name="multiple_groups.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            biodomain_dict=biodomain_dict,
            model_type_dict=model_type_dict,
            file_index=0,
            total_files=1,
        )

        # Should have 2 separate output entries
        assert len(result) == 2
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert result[1]["ensembl_gene_id"] == "ENSMUSG00000000002"


class TestTransformRnaDeAggregate:
    """
    Test class for RNA differential expression aggregate transformation.

    This class contains test methods that verify the behavior of the
    `transform_rna_de_aggregate` function using synthetic datasets designed
    to test specific functionality and edge cases.

    Attributes:
        data_files_path (str): Path to the directory containing test assets
            (synthetic input files and expected output files).

    Test Methods:
        - test_transform_rna_de_aggregate_missing_required_dataset: Tests error handling
          when required datasets are missing.
        - test_synthetic_basic_data: Tests core transformation with simple 2-gene dataset.
        - test_synthetic_multi_model_data: Tests handling of multiple models and tissues.
        - test_synthetic_jax_tissue_mapping: Tests JAX-specific tissue name mapping.
        - test_synthetic_mixed_genes_filtering: Tests filtering of human genes.
        - test_synthetic_sex_cohort_filtering: Tests filtering of combined-cohort (Females & Males) rows.
        - test_synthetic_age_sorting: Tests numeric sorting of age entries (integration test).
        - test_synthetic_single_row_data: Tests minimal edge case (single row).
        - test_synthetic_empty_data_file: Tests error handling for empty data files.
        - test_synthetic_missing_columns_data: Tests error handling for missing columns.
        - test_synthetic_rounding_precision: Tests 5-decimal-place rounding.
        - test_synthetic_multiple_biodomains: Tests genes with multiple biodomain assignments.
        - test_synthetic_null_model_group: Tests handling of null/empty model_group values.
        - test_inconsistent_model_group_values: Tests error handling for inconsistent model_group values.
        - test_inconsistent_model_type_values: Tests error handling for inconsistent model_type values.

    Helper Methods:
        - _load_synthetic_test_data: Loads synthetic test data files as DataFrames with
          proper column name normalization and dataset key mapping.
    """

    data_files_path = "tests/test_assets/rna_de_aggregate"

    def test_transform_rna_de_aggregate_missing_required_dataset(self) -> None:
        """Test that missing required datasets raise ValueError."""
        # Load datasets without one required dataset (mouse_gene_metadata)
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_biodom_genes_mm.csv",
                # Missing synthetic_mouse_gene_metadata.csv
            ]
        )

        # Expect transformation to raise ValueError for missing required dataset
        with pytest.raises(ValueError):
            transform_rna_de_aggregate(datasets=datasets)

    def _load_synthetic_test_data(
        self, data_files: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """Load synthetic test data files as DataFrames."""
        datasets = {}
        input_path = os.path.join(self.data_files_path, "input")

        # Mapping from file names to expected dataset keys
        file_to_key_mapping = {
            "synthetic_genotype_label_map.csv": "genotype_label_map",
            "synthetic_genotype_label_map_no_group.csv": "genotype_label_map",
            "synthetic_genotype_label_map_inconsistent.csv": "genotype_label_map",
            "synthetic_genotype_label_map_inconsistent_model_type.csv": "genotype_label_map",
            "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
            "synthetic_mouse_gene_metadata_multi.csv": "mouse_gene_metadata",
            "synthetic_mouse_gene_metadata_missing_symbols.csv": "mouse_gene_metadata",
            "synthetic_biodom_genes_mm.csv": "biodom_genes_mm",
            "synthetic_biodom_genes_mm_multiple.csv": "biodom_genes_mm",
        }

        for file_name in data_files:
            if file_name.endswith(".csv"):
                # Load CSV files
                file_path = os.path.join(input_path, file_name)
                df = pd.read_csv(file_path)

                # Fix column name mismatch: log2FoldChange -> log2foldchange
                if "log2FoldChange" in df.columns:
                    df = df.rename(columns={"log2FoldChange": "log2foldchange"})

                # Use the mapped key if available, otherwise use file name without extension
                key = file_to_key_mapping.get(file_name, file_name.replace(".csv", ""))
                datasets[key] = df

        return datasets

    def test_synthetic_basic_data(self) -> None:
        """Test transformation with synthetic basic data.

        Tests a simple case with 2 genes, 2 ages (3 and 6 months), and straightforward values
        to verify core transform functionality: data aggregation by gene, age sorting,
        and proper metadata enrichment (biodomains, gene symbols, model labels).
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

    def test_synthetic_multi_model_data(self) -> None:
        """Test transformation with synthetic multi-model data.

        Tests handling of multiple mouse models (Model_B, Model_C) with different tissues
        (Hippocampus, Cortex) and varying ages. Verifies that the transform correctly
        creates separate output entries for each unique combination of gene+model+tissue+sex+case+control.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_multi_model_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_multi_model_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id and name for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["name"]["link_text"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["name"]["link_text"])
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
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
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
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

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
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
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
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify only mouse genes are present
        for entry in output_data:
            assert entry["ensembl_gene_id"].startswith("ENSMUSG")

    def test_synthetic_sex_cohort_filtering(self) -> None:
        """Test combined-cohort filtering with synthetic sex cohort data.

        Tests that the transform correctly filters out rows where sex is
        "Females & Males", keeping only single-sex rows (Females, Males). The input
        contains a mix of all three cohort values, but only single-sex rows should
        appear in the output.
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_sex_cohort_filter_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path,
                "output",
                "synthetic_sex_cohort_filter_output.json",
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Verify no combined-cohort rows are present
        for entry in output_data:
            assert entry["sex"] != "Females & Males"

    def test_nan_adj_p_values_are_coerced_to_one(self) -> None:
        """NaN adjusted p-values in source data should be exported as 1.0."""

        datasets = self._load_synthetic_test_data(
            [
                "synthetic_nan_negative_zero_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        output_data = transform_rna_de_aggregate(datasets=datasets)

        assert len(output_data) == 1
        result_entry = output_data[0]
        twelve_month_entry = result_entry["12 months"]
        assert twelve_month_entry["adj_p_val"] == pytest.approx(1.0)

    def test_negative_zero_log2foldchange_are_coerced_to_zero(self) -> None:
        """-0.0 log2 fold change in source data should be exported as 0."""

        datasets = self._load_synthetic_test_data(
            [
                "synthetic_nan_negative_zero_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        output_data = transform_rna_de_aggregate(datasets=datasets)

        assert len(output_data) == 1
        result_entry = output_data[0]
        eighteen_month_entry = result_entry["18 months"]
        assert math.isclose(eighteen_month_entry["adj_p_val"], 0.0, abs_tol=1e-12)
        assert math.copysign(1.0, eighteen_month_entry["adj_p_val"]) > 0

    def test_negative_adj_p_values_raise_error(self) -> None:
        """Negative adjusted p-values in source data should raise ValueError."""

        # Load required metadata datasets
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Create a dataframe with a negative p-value directly
        negative_padj_data = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "log2foldchange": [1.00000],
                "padj": [-0.10000],
                "model": ["Model_A"],
                "case": ["Tg"],
                "control": ["Wt"],
                "age": ["3 months"],
                "sex": ["Female"],
                "tissue": ["Brain"],
            }
        )

        datasets["negative_padj_data"] = negative_padj_data

        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_aggregate(datasets=datasets)

        error_message = str(exc_info.value)
        assert "Negative adjusted p-value found in data" in error_message
        assert "ENSMUSG00000000001" in error_message
        assert "Model_A" in error_message
        assert "Brain" in error_message
        assert "Female" in error_message
        assert "-0.10000" in error_message or "-0.1" in error_message

    def test_synthetic_age_sorting(self) -> None:
        """Test age sorting with synthetic data.

        Verifies that age entries within each output record are sorted numerically (3, 6, 12 months)
        rather than alphabetically or in input order. Input ages are deliberately unsorted (12, 3, 6 months).
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_age_sorting_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
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
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

    def test_synthetic_single_row_data(self) -> None:
        """Test transformation with synthetic single row data.

        Tests edge case handling of minimal input: a single data row representing one gene
        at one age/condition. Verifies the transform can handle the smallest valid dataset.
        Also tests missing gene metadata handling - the gene (ENSMUSG00000000008) is not in
        mouse_gene_metadata.csv, so gene_symbol should be "" and biodomains should be [].
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_single_row_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
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
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

    def test_synthetic_empty_data_file(self) -> None:
        """Test handling of synthetic empty data files."""
        # Load datasets with empty synthetic data file
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_empty_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Should raise ValueError for empty data file
        with pytest.raises(ValueError, match="Data file .* is empty"):
            transform_rna_de_aggregate(datasets=datasets)

    def test_synthetic_missing_columns_data(self) -> None:
        """Test handling of synthetic data files with missing required columns."""
        # Load datasets with a synthetic data file missing required columns
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_missing_columns_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Expect transformation to raise ValueError for missing required columns
        with pytest.raises(ValueError, match="Missing required columns"):
            transform_rna_de_aggregate(datasets=datasets)

    def test_synthetic_rounding_precision(self) -> None:
        """Test that log2foldchange and padj values are rounded to 5 decimal places.

        Tests numeric precision by providing values with 7+ decimal places and verifying
        they are correctly rounded to exactly 5 decimal places in the output.
        """
        # Load synthetic test data with high-precision values
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rounding_precision_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
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
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Explicitly verify rounding
        assert output_data_sorted[0]["3 months"]["log2_fc"] == pytest.approx(1.12346)
        assert output_data_sorted[0]["3 months"]["adj_p_val"] == pytest.approx(0.01235)
        assert output_data_sorted[0]["6 months"]["log2_fc"] == pytest.approx(2.98765)
        assert output_data_sorted[0]["6 months"]["adj_p_val"] == pytest.approx(0.98765)

    def test_synthetic_multiple_biodomains(self) -> None:
        """Test handling of genes with multiple biodomain assignments.

        Verifies that genes assigned to multiple biodomains (e.g., both 'Synaptic' and 'Metabolic')
        correctly include all biodomains in the output as a list.
        """
        # Load synthetic test data with gene having multiple biodomains
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_multiple_biodomains_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata_multi.csv",
                "synthetic_biodom_genes_mm_multiple.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path,
                "output",
                "synthetic_multiple_biodomains_output.json",
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Explicitly verify multiple biodomains are present
        assert len(output_data_sorted[0]["biodomains"]) == 2
        assert set(output_data_sorted[0]["biodomains"]) == {"Metabolic", "Synaptic"}

    def test_synthetic_null_model_group(self) -> None:
        """Test that empty/null model_group is converted to None in output.

        Tests the specific logic that converts empty string model_group values to None
        to maintain JSON null representation in the output.
        """
        # Load synthetic test data with model having no model_group
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_null_model_group_data.csv",
                "synthetic_genotype_label_map_no_group.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_null_model_group_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Sort output data by ensembl_gene_id for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: x["ensembl_gene_id"])
        expected_data_sorted = sorted(expected_data, key=lambda x: x["ensembl_gene_id"])

        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

        # Explicitly verify model_group is None (not empty string)
        assert output_data_sorted[0]["model_group"] is None

    def test_inconsistent_model_group_values(self) -> None:
        """Test error handling for inconsistent model_group values within the same model.

        Tests that when a model has different model_group values across multiple rows
        (e.g., different genotypes), a clear ValueError is raised identifying which
        models have inconsistent values. Uses synthetic_genotype_label_map_inconsistent.csv
        where Model_A has GroupX for Tg genotype and GroupY for Wt genotype.
        """
        # Load synthetic test data with inconsistent model_group values
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map_inconsistent.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Expect transformation to raise ValueError with informative message
        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_aggregate(datasets=datasets)

        # Verify the error message contains expected information
        error_message = str(exc_info.value)
        assert "Each model must have a consistent model_group value" in error_message
        assert "genotype_label_map" in error_message
        assert "Model_A" in error_message
        # Model_B should not be in the error since it's consistent
        assert "Model_B" not in error_message

    def test_inconsistent_model_type_values(self) -> None:
        """Test error handling for inconsistent model_type values within the same model.

        Tests that when a model has different non-empty model_type values across multiple
        rows (e.g., different genotypes), a clear ValueError is raised identifying which
        models have inconsistent values. Uses
        synthetic_genotype_label_map_inconsistent_model_type.csv where Model_A has
        'Familial AD' for Tg genotype and 'Late Onset AD' for Wt genotype.
        """
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map_inconsistent_model_type.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        with pytest.raises(ValueError) as exc_info:
            transform_rna_de_aggregate(datasets=datasets)

        error_message = str(exc_info.value)
        assert "Each model must have a consistent model_type value" in error_message
        assert "genotype_label_map" in error_message
        assert "Model_A" in error_message
        # Model_B should not be in the error since it's consistent
        assert "Model_B" not in error_message

    def test_missing_gene_symbol_normalized(self) -> None:
        """
        Test that missing gene_symbol values in mouse_gene_metadata are converted to empty strings in the output.
        Uses basic synthetic data with two genes (ENSMUSG00000000001 and ENSMUSG00000000002), but the
        mouse_gene_metadata file has a missing symbol for ENSMUSG00000000002.
        """
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata_missing_symbols.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        output = transform_rna_de_aggregate(datasets=datasets)

        # Load expected output
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_missing_symbol_output.json"
            )
        ) as f:
            expected_output = json.load(f)

        assert output == expected_output
