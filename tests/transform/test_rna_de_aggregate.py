"""
Test suite for RNA differential expression aggregate transformation.

This module contains comprehensive tests for the `transform_rna_de_aggregate` function,
which aggregates mouse model RNA-seq differential expression data into a structured
format for the Agora platform.

The tests use synthetic datasets stored in `tests/test_assets/rna_de_aggregate/` to verify:
- Core transformation logic (data aggregation, metadata enrichment)
- Multi-model and multi-tissue handling
- JAX tissue name mapping (e.g., 'Right Cerebral Hemisphere' -> 'Hemibrain')
- Human gene filtering (only mouse genes with ENSMUSG* IDs should be processed)
- Age sorting (numeric ordering of age entries)
- Edge cases (single row data, missing metadata, empty biodomains)
- Error handling (missing datasets, empty files, missing columns)
- Data precision (rounding to 5 decimal places)
- Multiple biodomain assignments per gene
- Null/empty model_group handling

Test Data Structure:
    Input files include:
    - RNA-seq differential expression data (*.csv)
    - rnaseq_genotype_label_map.csv (maps genotypes to model labels)
    - mouse_gene_metadata.csv (gene symbols and metadata)
    - model_info.csv (model metadata including tissue and group)
    - biodom_genes_mm.csv (biodomain assignments for mouse genes)

    Output files are JSON-formatted expected results for comparison.
"""

import os
import json
from typing import Dict, List
import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_aggregate import transform_rna_de_aggregate


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
        - test_synthetic_age_sorting: Tests numeric sorting of age entries.
        - test_synthetic_single_row_data: Tests minimal edge case (single row).
        - test_synthetic_empty_data_file: Tests error handling for empty data files.
        - test_synthetic_missing_columns_data: Tests error handling for missing columns.
        - test_synthetic_rounding_precision: Tests 5-decimal-place rounding.
        - test_synthetic_multiple_biodomains: Tests genes with multiple biodomain assignments.
        - test_synthetic_null_model_group: Tests handling of null/empty model_group values.

    Helper Methods:
        - _load_synthetic_test_data: Loads synthetic test data files as DataFrames with
          proper column name normalization and dataset key mapping.
    """

    data_files_path = "tests/test_assets/rna_de_aggregate"

    def test_transform_rna_de_aggregate_missing_required_dataset(self) -> None:
        """Test that missing required datasets raise ValueError."""
        # Load datasets without one required dataset (model_info)
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_basic_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_biodom_genes_mm.csv"
                # Missing synthetic_model_info.csv
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
            "synthetic_rnaseq_genotype_label_map.csv": "rnaseq_genotype_label_map",
            "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
            "synthetic_model_info.csv": "model_info",
            "synthetic_biodom_genes_mm.csv": "biodom_genes_mm",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
            output_data, key=lambda x: (x["ensembl_gene_id"], x["name"])
        )
        expected_data_sorted = sorted(
            expected_data, key=lambda x: (x["ensembl_gene_id"], x["name"])
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
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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

    def test_synthetic_age_sorting(self) -> None:
        """Test age sorting with synthetic data.

        Verifies that age entries within each output record are sorted numerically (3, 6, 12 months)
        rather than alphabetically or in input order. Input ages are deliberately unsorted (12, 3, 6 months).
        """
        # Load synthetic test data
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_age_sorting_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
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
        assert output_data[0]["3 months"]["log2_fc"] == 1.12346
        assert output_data[0]["3 months"]["adj_p_val"] == 0.01235
        assert output_data[0]["6 months"]["log2_fc"] == 2.98765
        assert output_data[0]["6 months"]["adj_p_val"] == 0.98765

    def test_synthetic_multiple_biodomains(self) -> None:
        """Test handling of genes with multiple biodomain assignments.

        Verifies that genes assigned to multiple biodomains (e.g., both 'Synaptic' and 'Metabolic')
        correctly include all biodomains in the output as a list.
        """
        # Load synthetic test data with gene having multiple biodomains
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_multiple_biodomains_data.csv",
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata_multi.csv",
                "synthetic_model_info.csv",
                "synthetic_biodom_genes_mm_multiple.csv",
            ]
        )

        # Override the biodom_genes_mm dataset with the multi-biodomain version
        datasets["biodom_genes_mm"] = datasets.pop("synthetic_biodom_genes_mm_multiple")
        datasets["mouse_gene_metadata"] = datasets.pop(
            "synthetic_mouse_gene_metadata_multi"
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
        assert len(output_data[0]["biodomains"]) == 2
        assert set(output_data[0]["biodomains"]) == {"Metabolic", "Synaptic"}

    def test_synthetic_null_model_group(self) -> None:
        """Test that empty/null model_group is converted to None in output.

        Tests the specific logic that converts empty string model_group values to None
        to maintain JSON null representation in the output.
        """
        # Load synthetic test data with model having no model_group
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_null_model_group_data.csv",
                "synthetic_rnaseq_genotype_label_map_no_group.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info_no_group.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Override the datasets with the no_group versions
        datasets["rnaseq_genotype_label_map"] = datasets.pop(
            "synthetic_rnaseq_genotype_label_map_no_group"
        )
        datasets["model_info"] = datasets.pop("synthetic_model_info_no_group")

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
        assert output_data[0]["model_group"] is None
