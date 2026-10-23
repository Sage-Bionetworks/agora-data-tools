"""
Test suite for RNA differential expression aggregate transformation.

This module contains comprehensive tests for the `transform_rna_de_aggregate` function
and its helper functions, which aggregate mouse model RNA-seq differential expression
data into a structured format for the Agora platform.

The tests use synthetic datasets stored in `tests/test_assets/rna_de_aggregate/` to verify:
- Core transformation logic (data aggregation, metadata enrichment)
- Multi-model and multi-tissue handling
- JAX tissue name mapping (e.g., 'Right Cerebral Hemisphere' -> 'Hemibrain')
- Human gene filtering (only mouse genes with ENSMUSG* IDs should be processed)
- Age sorting (numeric ordering of age entries)
- Age validation (empty strings, whitespace, invalid formats)
- NaN handling (NaN adjusted p-values are coerced to 1.0)
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
    COLUMN_RULES,
    REQUIRED_INPUT,
    transform_rna_de_aggregate,
    _create_output_entry_from_group,
    _process_single_data_file,
)
from agoradatatools.etl.utils import ContainsSubstringRule
from tests.synthetic_loader import load_synthetic_test_data


class TestCreateOutputEntryFromGroup:
    """
    Unit tests for the _create_output_entry_from_group helper function.

    This class contains focused unit tests for output entry creation logic,
    testing the function in isolation from the full transformation pipeline.
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

    _FILE_TO_KEY = {
        "synthetic_genotype_label_map.csv": "genotype_label_map",
        "synthetic_genotype_label_map_inconsistent.csv": "genotype_label_map",
        "synthetic_genotype_label_map_inconsistent_model_type.csv": "genotype_label_map",
        "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
        "synthetic_mouse_gene_metadata_multi.csv": "mouse_gene_metadata",
        "synthetic_mouse_gene_metadata_missing_symbols.csv": "mouse_gene_metadata",
        "synthetic_biodom_genes_mm.csv": "biodom_genes_mm",
        "synthetic_biodom_genes_mm_multiple.csv": "biodom_genes_mm",
    }

    def _load_synthetic_test_data(
        self, data_files: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """Load synthetic test data files as DataFrames."""
        return load_synthetic_test_data(
            self.data_files_path, data_files, self._FILE_TO_KEY
        )

    def _build_minimal_datasets(self) -> Dict[str, pd.DataFrame]:
        """Build the smallest datasets that pass required-column checks."""
        biodom = {column: ["x"] for column in REQUIRED_INPUT["biodom_genes_mm"]}
        biodom["ensembl_id"] = ["ENSMUSG00000000001"]
        biodom["biodomain"] = ["Synaptic"]
        return {
            "genotype_label_map": pd.DataFrame(
                {
                    "model": ["Model_A", "Model_A"],
                    "genotype": ["Tg", "Wt"],
                    "display_label": ["Transgenic", "Wildtype"],
                    "model_group": ["Group_A", "Group_A"],
                    "result_order": [2, 1],
                    "model_type": ["Familial AD", "Familial AD"],
                }
            ),
            "mouse_gene_metadata": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": ["Gene1"],
                    "alias": [""],
                }
            ),
            "biodom_genes_mm": pd.DataFrame(biodom),
            "data_file": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "log2foldchange": [1.0],
                    "padj": [0.01],
                    "model": ["Model_A"],
                    "case": ["Tg"],
                    "control": ["Wt"],
                    "age": ["6 months"],
                    "sex": ["Male"],
                    "tissue": ["Cortex"],
                }
            ),
        }

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

    def test_invalid_age_format_raises(self) -> None:
        """An age that does not match N months fails the data file column rule before grouping."""
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )
        datasets["synthetic_basic_data"].loc[0, "age"] = "6months"

        with pytest.raises(ValueError, match="column 'age'.*matches_regex"):
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

    @pytest.mark.parametrize("column", sorted(COLUMN_RULES["genotype_label_map"]))
    def test_label_map_empty_value_raises(self, column: str) -> None:
        """Empty values in any gentotype label map column should raise an error.

        Columns come from COLUMN_RULES, so a newly required column is covered
        without a hand-written test.
        """
        datasets = self._build_minimal_datasets()
        # result_order is int; cast before writing "" so pandas does not warn.
        label_map = datasets["genotype_label_map"]
        label_map[column] = label_map[column].astype(object)
        label_map.loc[0, column] = ""
        with pytest.raises(ValueError, match="not_empty"):
            transform_rna_de_aggregate(datasets=datasets)

    def test_label_map_missing_model_type_raises(self) -> None:
        """model_type is a required label-map column for this transform."""
        datasets = self._build_minimal_datasets()
        datasets["genotype_label_map"] = datasets["genotype_label_map"].drop(
            columns=["model_type"]
        )
        with pytest.raises(ValueError, match="Missing required columns"):
            transform_rna_de_aggregate(datasets=datasets)

    def test_column_rules_parameter_honored(self) -> None:
        """A custom column_rules parameter overrides the module-level default."""
        datasets = self._build_minimal_datasets()
        custom_column_rules = {
            "genotype_label_map": {
                "model": [ContainsSubstringRule(value="IMPOSSIBLE")],
            }
        }
        with pytest.raises(ValueError, match="contains"):
            transform_rna_de_aggregate(
                datasets=datasets, column_rules=custom_column_rules
            )

    def test_data_file_required_columns_parameter(self) -> None:
        """data_file_required_columns is wired through to column validation."""
        datasets = self._build_minimal_datasets()
        with pytest.raises(ValueError, match="nonexistent_column"):
            transform_rna_de_aggregate(
                datasets=datasets,
                data_file_required_columns=["ensembl_gene_id", "nonexistent_column"],
            )

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
        assert "multiple model_group values" in error_message
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
        assert "multiple model_type values" in error_message
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
