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
        assert _extract_age_numeric(None) == 0


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
