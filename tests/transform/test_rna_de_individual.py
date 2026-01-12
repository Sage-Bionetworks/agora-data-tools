"""
Test suite for RNA individual expression transformation.

This module contains comprehensive tests for the `transform_rna_de_individual` function
and its helper functions, which transform mouse model individual RNA-seq expression
data into a structured format for the Agora platform.

Test Classes:
    - TestCreateIndividualResultsFromGroup: Unit tests for _create_individual_results_from_group
    - TestCreateOutputEntryFromGroup: Unit tests for _create_output_entry_from_group
    - TestProcessSingleDataFile: Unit tests for _process_single_data_file
    - TestTransformRnaDeIndividual: Integration tests for the full transformation pipeline

The tests use synthetic datasets stored in `tests/test_assets/rna_de_individual/` to verify:
- Core transformation logic (data grouping, metadata enrichment)
- Model_group handling (single control vs multiple control paradigms)
- JAX tissue name mapping (e.g., 'Right Cerebral Hemisphere' -> 'Hemibrain')
- Human gene filtering (only mouse genes with ENSMUSG* IDs should be processed)
- Age sorting (numeric ordering of age entries)
- Edge cases (single row data, missing metadata, empty files)
- Error handling (missing datasets, empty files, missing columns)
"""

import os
import json
from typing import Dict, List
import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_individual import (
    transform_rna_de_individual,
    _create_individual_results_from_group,
    _create_output_entry_from_group,
    _process_single_data_file,
    _extract_age_numeric,
    _determine_result_order,
)


class TestExtractAgeNumeric:
    """
    Unit tests for the _extract_age_numeric helper function.
    """

    def test_extract_age_numeric_standard_format(self) -> None:
        """Test extracting age from standard format like '6 months'."""
        assert _extract_age_numeric("6 months") == 6
        assert _extract_age_numeric("12 months") == 12
        assert _extract_age_numeric("4 months") == 4
        assert _extract_age_numeric("18 months") == 18

    def test_extract_age_numeric_single_digit(self) -> None:
        """Test extracting age with single digit."""
        assert _extract_age_numeric("3 months") == 3
        assert _extract_age_numeric("9 months") == 9

    def test_extract_age_numeric_with_different_units(self) -> None:
        """Test extracting age with different time units."""
        assert _extract_age_numeric("6 weeks") == 6
        assert _extract_age_numeric("2 years") == 2

    def test_extract_age_numeric_invalid_format(self) -> None:
        """Test that invalid formats return 0."""
        assert _extract_age_numeric("invalid") == 0
        assert _extract_age_numeric("") == 0
        assert _extract_age_numeric("months only") == 0

    def test_extract_age_numeric_none(self) -> None:
        """Test that None returns 0."""
        assert _extract_age_numeric(None) == 0  # type: ignore[arg-type]

    def test_extract_age_numeric_negative_age(self) -> None:
        """Test that negative age values return the negative number."""
        assert _extract_age_numeric("-5 months") == -5

    def test_extract_age_numeric_float_age(self) -> None:
        """Test that float age strings fail gracefully and return 0."""
        assert _extract_age_numeric("3.5 months") == 0
        assert _extract_age_numeric("6.9 months") == 0

    def test_extract_age_numeric_very_large_age(self) -> None:
        """Test that very large age values are extracted correctly."""
        assert _extract_age_numeric("1000 months") == 1000
        assert _extract_age_numeric("999999 days") == 999999

    def test_extract_age_numeric_zero_age(self) -> None:
        """Test that zero age is extracted correctly."""
        assert _extract_age_numeric("0 months") == 0

    def test_extract_age_numeric_whitespace_handling(self) -> None:
        """Test that extra whitespace is handled correctly."""
        assert _extract_age_numeric("  6   months  ") == 6
        assert _extract_age_numeric("12  months") == 12

    def test_extract_age_numeric_no_space_between(self) -> None:
        """Test age format without space between number and unit returns 0."""
        assert _extract_age_numeric("6months") == 0
        assert _extract_age_numeric("12weeks") == 0

    def test_extract_age_numeric_mixed_case_units(self) -> None:
        """Test that mixed case units are handled."""
        assert _extract_age_numeric("6 Months") == 6
        assert _extract_age_numeric("12 WEEKS") == 12

    def test_extract_age_numeric_with_decimals_and_commas(self) -> None:
        """Test age with unusual number formats."""
        # These should fail gracefully and return 0
        assert _extract_age_numeric("1,000 months") == 0  # Comma causes ValueError
        assert _extract_age_numeric("six months") == 0  # Word not parseable

    def test_extract_age_numeric_special_formats(self) -> None:
        """Test special age formats that might appear in data."""
        assert _extract_age_numeric("P30") == 0  # Postnatal day format
        assert _extract_age_numeric("3-6 months") == 0  # Range format fails
        assert _extract_age_numeric("~4 months") == 0  # Approximate fails


class TestDetermineResultOrder:
    """
    Unit tests for the _determine_result_order helper function.
    """

    def test_two_genotype_simple_model_carrier_first(self) -> None:
        """Test 2-genotype model: carrier should come before noncarrier."""
        label_map_dict = {
            ("5xFAD (UCI)", "5XFAD_carrier"): "5xFAD (UCI)",
            ("5xFAD (UCI)", "5XFAD_noncarrier"): "C57BL/6J",
        }
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier", "5XFAD_noncarrier"]
        }

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="5xFAD (UCI)",
            model_group="",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        assert result == ["5xFAD (UCI)", "C57BL/6J"]

    def test_two_genotype_uses_model_as_effective_group(self) -> None:
        """Test that empty model_group uses model name as effective group."""
        label_map_dict = {
            ("TestModel", "carrier"): "Case",
            ("TestModel", "noncarrier"): "Control",
        }
        genotypes_by_model_group = {"TestModel": ["carrier", "noncarrier"]}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="TestModel",
            model_group="",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        assert result == ["Case", "Control"]

    def test_four_genotype_matrixed_model_correct_order(self) -> None:
        """Test 4-genotype model: base control, base model, fancy control, compound."""
        label_map_dict = {
            ("Abca7*V1599M", "5XFAD_noncarrier"): "C57BL/6J",
            ("Abca7*V1599M", "Abca7-V1599M_homozygous"): "Abca7*V1599M",
            ("Abca7*V1599M", "5XFAD_carrier"): "5xFAD",
            (
                "Abca7*V1599M",
                "5XFAD_carrier; Abca7-V1599M_homozygous",
            ): "Abca7*V1599M.5xFAD",
        }
        genotypes_by_model_group = {
            "Abca7*V1599M": [
                "5XFAD_noncarrier",
                "Abca7-V1599M_homozygous",
                "5XFAD_carrier",
                "5XFAD_carrier; Abca7-V1599M_homozygous",
            ]
        }

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Abca7*V1599M",
            model_group="Abca7*V1599M",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        assert result == ["C57BL/6J", "Abca7*V1599M", "5xFAD", "Abca7*V1599M.5xFAD"]

    def test_four_genotype_with_mixed_order_input(self) -> None:
        """Test that 4-genotype model sorts correctly regardless of input order."""
        label_map_dict = {
            ("Model", "compound; mutation"): "Compound",
            ("Model", "fancy_carrier"): "Fancy",
            ("Model", "mutation"): "Base",
            ("Model", "control_noncarrier"): "Control",
        }
        genotypes_by_model_group = {
            "Model": [
                "compound; mutation",
                "fancy_carrier",
                "mutation",
                "control_noncarrier",
            ]
        }

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="Model",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        # Expected: Control (noncarrier), Base (no carrier/noncarrier), Fancy (carrier), Compound (;)
        assert result == ["Control", "Base", "Fancy", "Compound"]

    def test_three_genotype_fallback_alphabetical(self) -> None:
        """Test 3-genotype model falls back to alphabetical sorting."""
        label_map_dict = {
            ("Model", "genotype_a"): "Zebra",
            ("Model", "genotype_b"): "Apple",
            ("Model", "genotype_c"): "Mango",
        }
        genotypes_by_model_group = {"Model": ["genotype_a", "genotype_b", "genotype_c"]}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="Model",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        # Should be alphabetically sorted by display label
        assert result == ["Apple", "Mango", "Zebra"]

    def test_one_genotype_single_entry(self) -> None:
        """Test single genotype returns single element list."""
        label_map_dict = {("Model", "only_genotype"): "OnlyOne"}
        genotypes_by_model_group = {"Model": ["only_genotype"]}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        assert result == ["OnlyOne"]

    def test_five_genotype_uses_matrixed_logic(self) -> None:
        """Test 5-genotype model still uses matrixed sorting logic."""
        label_map_dict = {
            ("Model", "noncarrier"): "Control",
            ("Model", "mutation"): "Base",
            ("Model", "fancy_carrier"): "Fancy",
            ("Model", "compound; mutation"): "Compound",
            ("Model", "extra"): "Extra",
        }
        genotypes_by_model_group = {
            "Model": [
                "noncarrier",
                "mutation",
                "fancy_carrier",
                "compound; mutation",
                "extra",
            ]
        }

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="Model",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        # First should be noncarrier (0), then non-semicolon/non-carrier (1),
        # then carrier (2), then semicolon (3)
        assert result[0] == "Control"
        assert result[-1] == "Compound"
        assert "Fancy" in result

    def test_empty_genotypes_list(self) -> None:
        """Test empty genotypes list returns empty result."""
        label_map_dict = {}
        genotypes_by_model_group = {}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="Model",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        assert result == []

    def test_genotypes_not_in_label_map(self) -> None:
        """Test genotypes without display labels are filtered out."""
        label_map_dict = {
            ("Model", "genotype_a"): "LabelA",
        }
        genotypes_by_model_group = {"Model": ["genotype_a", "genotype_b", "genotype_c"]}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="Model",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        # Only genotype_a has a label
        assert result == ["LabelA"]

    def test_case_sensitivity_in_noncarrier(self) -> None:
        """Test case-insensitive detection of 'noncarrier'."""
        label_map_dict = {
            ("Model", "NoNcArRiEr"): "Control",
            ("Model", "CARRIER"): "Case",
        }
        genotypes_by_model_group = {"Model": ["NoNcArRiEr", "CARRIER"]}

        result = _determine_result_order(
            label_map_dict=label_map_dict,
            model="Model",
            model_group="",
            genotypes_by_model_group=genotypes_by_model_group,
        )

        # Noncarrier should be second (control)
        assert result == ["Case", "Control"]


class TestCreateIndividualResultsFromGroup:
    """
    Unit tests for the _create_individual_results_from_group helper function.
    """

    def test_create_individual_results_single_age(self) -> None:
        """Test creating individual_results from a single age group."""
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_noncarrier"],
                "sex": ["Male", "Female"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 1
        assert result[0]["age"] == "6 months"
        assert len(result[0]["data"]) == 2
        assert result[0]["data"][0]["genotype"] == "5XFAD_carrier"
        assert result[0]["data"][0]["value"] == pytest.approx(5.5)
        assert result[0]["data"][0]["individual_id"] == "1001"

    def test_create_individual_results_multiple_ages(self) -> None:
        """Test creating individual_results from multiple age groups."""
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months", "12 months", "12 months"],
                "genotype": [
                    "5XFAD_carrier",
                    "5XFAD_noncarrier",
                    "5XFAD_carrier",
                    "5XFAD_noncarrier",
                ],
                "sex": ["Male", "Female", "Male", "Female"],
                "individualid": ["1001", "1002", "2001", "2002"],
                "expression": [5.5, 4.9, 6.1, 5.0],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 2
        assert result[0]["age"] == "6 months"
        assert result[1]["age"] == "12 months"
        assert len(result[0]["data"]) == 2
        assert len(result[1]["data"]) == 2

    def test_create_individual_results_age_sorting(self) -> None:
        """Test that ages are sorted numerically."""
        group = pd.DataFrame(
            {
                "age": ["12 months", "6 months", "18 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier", "5XFAD_carrier"],
                "sex": ["Male", "Male", "Male"],
                "individualid": ["2001", "1001", "3001"],
                "expression": [6.1, 5.5, 7.0],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 3
        assert result[0]["age"] == "6 months"
        assert result[1]["age"] == "12 months"
        assert result[2]["age"] == "18 months"

    def test_create_individual_results_with_mixed_case_genotypes(self) -> None:
        """Test that mixed case genotypes are preserved as-is."""
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months"],
                "genotype": ["5xFaD_CaRrIeR", "5XFAD_noncarrier"],
                "sex": ["Male", "Female"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 1
        assert result[0]["data"][0]["genotype"] == "5xFaD_CaRrIeR"
        assert result[0]["data"][1]["genotype"] == "5XFAD_noncarrier"

    def test_create_individual_results_single_data_point(self) -> None:
        """Test with only one data point."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "sex": ["Male"],
                "individualid": ["1001"],
                "expression": [5.5],
            }
        )

        result = _create_individual_results_from_group(group)

        assert len(result) == 1
        assert len(result[0]["data"]) == 1

    def test_create_individual_results_with_special_characters_in_sex(self) -> None:
        """Test that special characters in sex field are preserved."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "sex": ["Unknown/Mixed"],
                "individualid": ["1001"],
                "expression": [5.5],
            }
        )

        result = _create_individual_results_from_group(group)

        assert result[0]["data"][0]["sex"] == "Unknown/Mixed"

    def test_create_individual_results_with_invalid_age_format(self) -> None:
        """Test that invalid age formats are kept but may not sort correctly."""
        group = pd.DataFrame(
            {
                "age": ["invalid", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier"],
                "sex": ["Male", "Male"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 6.0],
            }
        )

        result = _create_individual_results_from_group(group)

        # Both ages should be present
        assert len(result) == 2
        ages = [r["age"] for r in result]
        assert "invalid" in ages
        assert "6 months" in ages


class TestCreateOutputEntryFromGroup:
    """
    Unit tests for the _create_output_entry_from_group helper function.
    """

    def test_create_output_entry_basic(self) -> None:
        """Test basic output entry creation."""
        # group_key contains (ensembl_gene_id, tissue, model_group, name)
        # When model_group is empty, it should be set to empty string in group_key
        group_key = ("ENSMUSG00000000001", "Cortex", "", "5xFAD (UCI)")
        group = pd.DataFrame(
            {
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_noncarrier"],
                "sex": ["Male", "Female"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        label_map_dict = {
            ("5xFAD (UCI)", "5XFAD_carrier"): "5xFAD (UCI)",
            ("5xFAD (UCI)", "5XFAD_noncarrier"): "C57BL/6J",
        }
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier", "5XFAD_noncarrier"]
        }

        result = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            label_map_dict,
            genotypes_by_model_group,
        )

        # Now returns a list of entries (one per age)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert result[0]["gene_symbol"] == "Gene1"
        assert result[0]["tissue"] == "Cortex"
        assert result[0]["name"] == "5xFAD (UCI)"
        assert result[0]["model_group"] is None
        assert result[0]["matched_control"] == "C57BL/6J"
        assert result[0]["result_order"] == ["5xFAD (UCI)", "C57BL/6J"]
        assert result[0]["units"] == "Log2 Counts per Million"
        assert result[0]["age"] == "6 months"
        assert result[0]["age_numeric"] == 6
        assert len(result[0]["data"]) == 2

    def test_create_output_entry_jax_tissue_mapping(self) -> None:
        """Test that JAX tissue name is mapped correctly."""
        group_key = (
            "ENSMUSG00000000001",
            "Right Cerebral Hemisphere",
            "Model_A",
            "Model_A",
        )
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "sex": ["Male"],
                "individualid": ["1001"],
                "expression": [5.5],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        genotypes_by_model_group = {"Model_A": ["5XFAD_carrier"]}

        result = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            label_map_dict,
            genotypes_by_model_group,
        )

        assert len(result) == 1
        assert result[0]["tissue"] == "Hemibrain"

    def test_create_output_entry_with_model_group(self) -> None:
        """Test output entry creation with model_group."""
        group_key = (
            "ENSMUSG00000000003",
            "Hippocampus",
            "Abca7*V1599M",
            "Abca7*V1599M",
        )
        group = pd.DataFrame(
            {
                "age": ["4 months"],
                "genotype": ["Abca7-V1599M_homozygous"],
                "sex": ["Male"],
                "individualid": ["3001"],
                "expression": [4.5],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000003": "Gene3"}
        label_map_dict = {
            ("Abca7*V1599M", "Abca7-V1599M_homozygous"): "Abca7*V1599M",
            ("Abca7*V1599M", "5XFAD_noncarrier"): "C57BL/6J",
            ("Abca7*V1599M", "5XFAD_carrier"): "5xFAD",
            (
                "Abca7*V1599M",
                "5XFAD_carrier; Abca7-V1599M_homozygous",
            ): "Abca7*V1599M.5xFAD",
        }
        genotypes_by_model_group = {
            "Abca7*V1599M": [
                "5XFAD_noncarrier",
                "Abca7-V1599M_homozygous",
                "5XFAD_carrier",
                "5XFAD_carrier; Abca7-V1599M_homozygous",
            ]
        }

        result = _create_output_entry_from_group(
            group_key,
            group,
            gene_metadata_dict,
            label_map_dict,
            genotypes_by_model_group,
        )

        assert len(result) == 1
        assert result[0]["model_group"] == "Abca7*V1599M"
        assert result[0]["age_numeric"] == 4
        # For matrixed model, should have order: base control, base model, fancy control, compound
        assert result[0]["result_order"] == [
            "C57BL/6J",
            "Abca7*V1599M",
            "5xFAD",
            "Abca7*V1599M.5xFAD",
        ]


class TestProcessSingleDataFile:
    """
    Unit tests for the _process_single_data_file helper function.
    """

    def test_process_single_data_file_basic(self) -> None:
        """Test basic processing of a single data file."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_noncarrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}
        label_map_dict = {
            ("5xFAD (UCI)", "5XFAD_carrier"): "5xFAD (UCI)",
            ("5xFAD (UCI)", "5XFAD_noncarrier"): "C57BL/6J",
        }
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier", "5XFAD_noncarrier"]
        }

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        assert len(result) == 1
        assert result[0]["ensembl_gene_id"] == "ENSMUSG00000000001"

    def test_process_single_data_file_filters_human_genes(self) -> None:
        """Test that human genes are filtered out."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSG00000000001"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {"5xFAD (UCI)": ["5XFAD_carrier"]}

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        # Only one entry (mouse gene only)
        assert len(result) == 1
        assert result[0]["ensembl_gene_id"].startswith("ENSMUSG")

    def test_process_single_data_file_empty_raises_error(self) -> None:
        """Test that empty file raises ValueError."""
        data_file = pd.DataFrame()

        with pytest.raises(ValueError, match="Data file .* is empty"):
            _process_single_data_file(
                file_name="empty_file.csv",
                data_file=data_file,
                data_file_required_columns=["ensembl_gene_id"],
                gene_metadata_dict={},
                label_map_dict={},
                model_group_dict={},
                genotypes_by_model_group={},
                file_index=0,
                total_files=1,
            )

    def test_process_single_data_file_with_nan_expression(self) -> None:
        """Test that NaN expression values are handled (should be preserved as NaN)."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, float("nan")],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_noncarrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier", "5XFAD_noncarrier"]
        }

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        # Verify NaN is preserved in output
        assert len(result) == 1
        data_points = result[0]["data"]
        has_nan = any(pd.isna(point["value"]) for point in data_points)
        assert has_nan

    def test_process_single_data_file_with_infinite_expression(self) -> None:
        """Test that infinite expression values are handled."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["1001", "1002"],
                "expression": [float("inf"), 5.5],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_noncarrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier", "5XFAD_noncarrier"]
        }

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        # Verify infinite value is in output
        assert len(result) == 1
        data_points = result[0]["data"]
        has_inf = any(point["value"] == float("inf") for point in data_points)
        assert has_inf

    def test_process_single_data_file_missing_gene_in_metadata(self) -> None:
        """Test that genes not in metadata dict get empty gene_symbol."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG99999999999"],
                "individualid": ["1001"],
                "expression": [5.5],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "model": ["5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {"ENSMUSG00000000001": "Gene1"}  # Different gene
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {"5xFAD (UCI)": ["5XFAD_carrier"]}

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        assert len(result) == 1
        assert result[0]["gene_symbol"] == ""

    def test_process_single_data_file_duplicate_individual_ids(self) -> None:
        """Test that duplicate individual IDs in same group are preserved."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["1001", "1001"],  # Duplicate
                "expression": [5.5, 5.6],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Male"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {"5xFAD (UCI)": ["5XFAD_carrier"]}

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        # Both duplicates should be preserved
        assert len(result) == 1
        assert len(result[0]["data"]) == 2

    def test_process_single_data_file_negative_expression(self) -> None:
        """Test that negative expression values are preserved."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["1001"],
                "expression": [-2.5],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "model": ["5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {"5xFAD (UCI)": ["5XFAD_carrier"]}

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        assert len(result) == 1
        assert result[0]["data"][0]["value"] == pytest.approx(-2.5)

    def test_process_single_data_file_rounding_to_5_decimals(self) -> None:
        """Test that expression values are rounded to 5 decimal places."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["1001"],
                "expression": [5.123456789],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["5XFAD_carrier"],
                "model": ["5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {"5xFAD (UCI)": ["5XFAD_carrier"]}

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        assert len(result) == 1
        assert result[0]["data"][0]["value"] == pytest.approx(5.12346)

    def test_process_single_data_file_genotype_not_in_model_group(self) -> None:
        """Test that genotypes not in model_group are filtered out."""
        data_file = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000001"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 4.9],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "invalid_genotype"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )

        gene_metadata_dict = {}
        label_map_dict = {}
        model_group_dict = {"5xFAD (UCI)": ""}
        genotypes_by_model_group = {
            "5xFAD (UCI)": ["5XFAD_carrier"]
        }  # Only carrier allowed

        data_file_required_columns = [
            "ensembl_gene_id",
            "expression",
            "model",
            "genotype",
            "age",
            "sex",
            "tissue",
            "individualid",
        ]

        result = _process_single_data_file(
            file_name="test_file.csv",
            data_file=data_file,
            data_file_required_columns=data_file_required_columns,
            gene_metadata_dict=gene_metadata_dict,
            label_map_dict=label_map_dict,
            model_group_dict=model_group_dict,
            genotypes_by_model_group=genotypes_by_model_group,
            file_index=0,
            total_files=1,
        )

        # Only valid genotype should remain
        assert len(result) == 1
        assert len(result[0]["data"]) == 1
        assert result[0]["data"][0]["genotype"] == "5XFAD_carrier"


class TestTransformRnaDeIndividual:
    """
    Integration tests for RNA individual expression transformation.
    """

    data_files_path = "tests/test_assets/rna_de_individual"

    def _load_synthetic_test_data(
        self, data_files: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """Load synthetic test data files as DataFrames."""
        datasets = {}
        input_path = os.path.join(self.data_files_path, "input")

        file_to_key_mapping = {
            "synthetic_rnaseq_genotype_label_map.csv": "rnaseq_genotype_label_map",
            "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
        }

        for file_name in data_files:
            if file_name.endswith(".csv"):
                file_path = os.path.join(input_path, file_name)
                df = pd.read_csv(file_path)
                key = file_to_key_mapping.get(file_name, file_name.replace(".csv", ""))
                datasets[key] = df

        return datasets

    def test_synthetic_basic_data(self) -> None:
        """Test transformation with synthetic basic data."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_individual_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected_data = json.load(f)

        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )

        assert output_data_sorted == expected_data_sorted

    def test_synthetic_model_group_data(self) -> None:
        """Test transformation with model_group data (multiple genotypes)."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_model_group_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_model_group_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )

        assert output_data_sorted == expected_data_sorted

    def test_transform_rna_de_individual_missing_required_dataset(self) -> None:
        """Test that missing required datasets raise ValueError."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_individual_data.csv",
                "synthetic_mouse_gene_metadata.csv",
                # Missing rnaseq_genotype_label_map
            ]
        )

        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def test_synthetic_jax_tissue_mapping(self) -> None:
        """Test JAX tissue name mapping: 'Right Cerebral Hemisphere' -> 'Hemibrain'."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_jax_tissue_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_jax_tissue_output.json"
            )
        ) as f:
            expected_data = json.load(f)

        output_data = transform_rna_de_individual(datasets=datasets)

        # Sort for deterministic comparison
        output_data_sorted = sorted(
            output_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["tissue"])
        )

        assert output_data_sorted == expected_data_sorted
        # Verify tissue is mapped to Hemibrain
        assert all(entry["tissue"] == "Hemibrain" for entry in output_data)

    def test_multiple_data_files_processing(self) -> None:
        """Test that multiple data files are processed and combined correctly."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_multifile_data1.csv",
                "synthetic_multifile_data2.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        output_data = transform_rna_de_individual(datasets=datasets)

        # Should have entries from both files
        assert len(output_data) >= 2

        # Check that we have data from both genes
        gene_ids = {entry["ensembl_gene_id"] for entry in output_data}
        assert "ENSMUSG00000000001" in gene_ids
        assert "ENSMUSG00000000002" in gene_ids

        # Check that we have data from both tissues
        tissues = {entry["tissue"] for entry in output_data}
        assert "Cortex" in tissues
        assert "Hippocampus" in tissues

    def test_mixed_human_and_mouse_genes(self) -> None:
        """Test that human genes (ENSG*) are filtered out, keeping only mouse genes (ENSMUSG*)."""
        # Create test data with both human and mouse genes
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Add a mixed dataset manually
        mixed_data = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",  # Mouse
                    "ENSG00000000001",  # Human
                    "ENSMUSG00000000002",  # Mouse
                ],
                "individualid": ["1001", "1002", "1003"],
                "expression": [5.5, 6.0, 4.5],
                "tissue": ["Cortex", "Cortex", "Cortex"],
                "sex": ["Male", "Female", "Male"],
                "age": ["6 months", "6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier", "5XFAD_carrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )
        datasets["mixed_genes.csv"] = mixed_data

        output_data = transform_rna_de_individual(datasets=datasets)

        # Only mouse genes should be in output
        gene_ids = [entry["ensembl_gene_id"] for entry in output_data]
        assert all(gene_id.startswith("ENSMUSG") for gene_id in gene_ids)
        assert "ENSG00000000001" not in gene_ids

    def test_empty_model_group_vs_null_model_group(self) -> None:
        """Test that empty string and null model_group are handled correctly."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_individual_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        output_data = transform_rna_de_individual(datasets=datasets)

        # Basic data uses empty model_group, should become null in output
        for entry in output_data:
            if entry["name"] == "5xFAD (UCI)":
                assert entry["model_group"] is None

    def test_missing_required_column_in_data_file(self) -> None:
        """Test that missing required columns in data file raise ValueError."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Create data file missing 'age' column
        incomplete_data = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["1001"],
                "expression": [5.5],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                # Missing 'age'
                "genotype": ["5XFAD_carrier"],
                "model": ["5xFAD (UCI)"],
            }
        )
        datasets["incomplete_data.csv"] = incomplete_data

        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def test_missing_rnaseq_genotype_label_map(self) -> None:
        """Test that missing rnaseq_genotype_label_map raises ValueError."""
        datasets = {
            "data.csv": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "individualid": ["1001"],
                    "expression": [5.5],
                    "tissue": ["Cortex"],
                    "sex": ["Male"],
                    "age": ["6 months"],
                    "genotype": ["5XFAD_carrier"],
                    "model": ["5xFAD (UCI)"],
                }
            ),
            "mouse_gene_metadata": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": ["Gene1"],
                    "alias": [""],
                }
            ),
        }

        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def test_missing_mouse_gene_metadata(self) -> None:
        """Test that missing mouse_gene_metadata raises ValueError."""
        datasets = {
            "data.csv": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "individualid": ["1001"],
                    "expression": [5.5],
                    "tissue": ["Cortex"],
                    "sex": ["Male"],
                    "age": ["6 months"],
                    "genotype": ["5XFAD_carrier"],
                    "model": ["5xFAD (UCI)"],
                }
            ),
            "rnaseq_genotype_label_map": pd.DataFrame(
                {
                    "model": ["5xFAD (UCI)"],
                    "model_group": [""],
                    "display_label": ["5xFAD (UCI)"],
                    "genotype": ["5XFAD_carrier"],
                }
            ),
        }

        with pytest.raises(ValueError):
            transform_rna_de_individual(datasets=datasets)

    def test_only_metadata_no_data_files(self) -> None:
        """Test that having only metadata files (no data files) returns empty list."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        output_data = transform_rna_de_individual(datasets=datasets)

        # Should return empty list when no data files are provided
        assert output_data == []

    def test_data_file_with_all_human_genes(self) -> None:
        """Test that data file with only human genes returns empty output."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Create data file with only human genes
        human_only_data = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSG00000000001", "ENSG00000000002"],
                "individualid": ["1001", "1002"],
                "expression": [5.5, 6.0],
                "tissue": ["Cortex", "Cortex"],
                "sex": ["Male", "Female"],
                "age": ["6 months", "6 months"],
                "genotype": ["5XFAD_carrier", "5XFAD_carrier"],
                "model": ["5xFAD (UCI)", "5xFAD (UCI)"],
            }
        )
        datasets["human_only.csv"] = human_only_data

        output_data = transform_rna_de_individual(datasets=datasets)

        # Should return empty list since all genes are filtered out
        assert output_data == []

    def test_model_not_in_label_map(self) -> None:
        """Test behavior when model in data is not in label map."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
            ]
        )

        # Create data with a model not in the label map
        unknown_model_data = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "individualid": ["1001"],
                "expression": [5.5],
                "tissue": ["Cortex"],
                "sex": ["Male"],
                "age": ["6 months"],
                "genotype": ["unknown_genotype"],
                "model": ["UnknownModel"],
            }
        )
        datasets["unknown_model.csv"] = unknown_model_data

        output_data = transform_rna_de_individual(datasets=datasets)

        # Should handle gracefully - data gets filtered out due to genotype filtering
        # The output might be empty or have entries with empty matched_control
        assert isinstance(output_data, list)
