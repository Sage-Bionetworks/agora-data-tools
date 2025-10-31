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

    def test_transform_rna_de_aggregate_human_genes_filtered(self):
        """Test that human genes (ENSG*) are filtered out, keeping only mouse genes (ENSMUSG*)."""
        # Create test data with both human and mouse genes
        mixed_data = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000022892",
                    "ENSG00000142192",
                    "ENSMUSG00000019969",
                ],
                "log2foldchange": [1.234567, 2.345678, -0.876543],
                "padj": [0.001, 0.002, 0.003],
                "model": ["APP/PS1", "APP/PS1", "APP/PS1"],
                "case": ["Tg", "Tg", "Tg"],
                "control": ["Wt", "Wt", "Wt"],
                "age": ["6 months", "6 months", "6 months"],
                "sex": ["Female", "Female", "Female"],
                "tissue": ["Hippocampus", "Hippocampus", "Hippocampus"],
            }
        )

        # Load required datasets
        datasets = self._load_synthetic_test_data(
            [
                "synthetic_rnaseq_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_model_info.csv",
                "synthetic_biodom_genes_mm.csv",
            ]
        )

        # Add the mixed data as a data file
        datasets["test_data_mixed"] = mixed_data

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Should only contain mouse genes (ENSMUSG*), not human genes (ENSG*)
        assert len(output_data) == 2  # Only 2 mouse genes
        for entry in output_data:
            assert entry["ensembl_gene_id"].startswith("ENSMUSG")

    def test_transform_rna_de_aggregate_jax_tissue_mapping(self):
        """Test that JAX models with 'Right Cerebral Hemisphere' tissue are mapped to 'Hemibrain'."""
        # Create test data with JAX model and Right Cerebral Hemisphere tissue
        jax_data = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000022892"],
                "log2foldchange": [1.234567],
                "padj": [0.001],
                "model": ["JAX_model"],
                "case": ["Tg"],
                "control": ["Wt"],
                "age": ["6 months"],
                "sex": ["Female"],
                "tissue": ["Right Cerebral Hemisphere"],
            }
        )

        # Create JAX-specific datasets
        datasets = {
            "test_data_jax": jax_data,
            "rnaseq_genotype_label_map": pd.DataFrame(
                {
                    "model": ["JAX_model"],
                    "model_group": ["AD"],
                    "display_label": ["JAX_model (Tg)"],
                    "genotype": ["Tg"],
                }
            ),
            "mouse_gene_metadata": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000022892"],
                    "gene_symbol": ["App"],
                    "alias": [""],
                }
            ),
            "model_info": pd.DataFrame(
                {
                    "model": ["JAX_model"],
                    "matched_controls": ["JAX_model (Wt)"],
                    "model_type": ["Transgenic"],
                }
            ),
            "biodom_genes_mm": pd.DataFrame(
                {
                    "biodomain": ["Synaptic"],
                    "abbr": ["Axon"],
                    "label": ["Axon Function"],
                    "color": ["#FF6B6B"],
                    "go_id": ["GO:0030424"],
                    "goterm_name": ["axon"],
                    "n_symbol": [1],
                    "symbol": ["App"],
                    "ensembl_id": ["ENSMUSG00000022892"],
                }
            ),
        }

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Should map tissue from "Right Cerebral Hemisphere" to "Hemibrain"
        assert len(output_data) == 1
        assert output_data[0]["tissue"] == "Hemibrain"
        assert "jax" in output_data[0]["name"].lower()

    def test_transform_rna_de_aggregate_age_sorting(self):
        """Test that age entries are sorted numerically."""
        # Create test data with ages in non-numerical order
        age_data = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000022892",
                    "ENSMUSG00000022892",
                    "ENSMUSG00000022892",
                ],
                "log2foldchange": [1.234567, 2.345678, 0.876543],
                "padj": [0.001, 0.002, 0.003],
                "model": ["APP/PS1", "APP/PS1", "APP/PS1"],
                "case": ["Tg", "Tg", "Tg"],
                "control": ["Wt", "Wt", "Wt"],
                "age": ["12 months", "3 months", "6 months"],  # Non-numerical order
                "sex": ["Female", "Female", "Female"],
                "tissue": ["Hippocampus", "Hippocampus", "Hippocampus"],
            }
        )

        # Create datasets with age data
        datasets = {
            "test_data_age": age_data,
            "rnaseq_genotype_label_map": pd.DataFrame(
                {
                    "model": ["APP/PS1"],
                    "model_group": ["AD"],
                    "display_label": ["APP/PS1 (Tg)"],
                    "genotype": ["Tg"],
                }
            ),
            "mouse_gene_metadata": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000022892"],
                    "gene_symbol": ["App"],
                    "alias": [""],
                }
            ),
            "model_info": pd.DataFrame(
                {
                    "model": ["APP/PS1"],
                    "matched_controls": ["APP/PS1 (Wt)"],
                    "model_type": ["Transgenic"],
                }
            ),
            "biodom_genes_mm": pd.DataFrame(
                {
                    "biodomain": ["Synaptic"],
                    "abbr": ["Axon"],
                    "label": ["Axon Function"],
                    "color": ["#FF6B6B"],
                    "go_id": ["GO:0030424"],
                    "goterm_name": ["axon"],
                    "n_symbol": [1],
                    "symbol": ["App"],
                    "ensembl_id": ["ENSMUSG00000022892"],
                }
            ),
        }

        # Transform data
        output_data = transform_rna_de_aggregate(datasets=datasets)

        # Should have ages sorted numerically
        assert len(output_data) == 1
        entry = output_data[0]
        age_keys = [
            key
            for key in entry.keys()
            if key
            not in [
                "ensembl_gene_id",
                "gene_symbol",
                "biodomains",
                "name",
                "matched_control",
                "model_group",
                "model_type",
                "tissue",
                "sex",
            ]
        ]
        # Age keys should be in numerical order: 3 months, 6 months, 12 months
        assert age_keys == ["3 months", "6 months", "12 months"]

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
        """Test transformation with synthetic basic data."""
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
        """Test transformation with synthetic multi-model data."""
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
        """Test JAX tissue mapping with synthetic data."""
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
        """Test human gene filtering with synthetic mixed genes data."""
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
        """Test age sorting with synthetic data."""
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
        """Test transformation with synthetic single row data."""
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
