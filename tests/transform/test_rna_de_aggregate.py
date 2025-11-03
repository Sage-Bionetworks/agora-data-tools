import os
import json

import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_aggregate import transform_rna_de_aggregate


class TestTransformRnaDeAggregate:
    data_files_path = "tests/test_assets/rna_de_aggregate"

    def test_transform_rna_de_aggregate_missing_required_dataset(self):
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

    def _load_synthetic_test_data(self, data_files):
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

    def test_synthetic_basic_data(self):
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

    def test_synthetic_multi_model_data(self):
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

    def test_synthetic_jax_tissue_mapping(self):
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

    def test_synthetic_mixed_genes_filtering(self):
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

    def test_synthetic_age_sorting(self):
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

    def test_synthetic_single_row_data(self):
        """Test transformation with synthetic single row data.

        Tests edge case handling of minimal input: a single data row representing one gene
        at one age/condition. Verifies the transform can handle the smallest valid dataset.
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

    def test_synthetic_empty_data_file(self):
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

    def test_synthetic_missing_columns_data(self):
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
