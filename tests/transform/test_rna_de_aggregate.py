import os
import json
from unittest import mock

import pandas as pd
import pytest

from agoradatatools.etl.transform.rna_de_aggregate import (
    transform_rna_de_aggregate,
    _quick_validate_data_file,
)


class TestTransformRnaDeAggregate:
    data_files_path = "tests/test_assets/rna_de_aggregate"
    
    pass_test_data = [
        (
            # Pass with good test data
            {
                "rna_de_aggregate_data_files": "rna_de_aggregate_data_files_good.csv",
                "rnaseq_genotype_label_map": "rnaseq_genotype_label_map_good.csv",
                "mouse_gene_metadata": "mouse_gene_metadata_good.csv",
                "model_info": "model_info_good.csv",
                "biodom_genes_mm": "biodom_genes_mm_good.csv",
            },
            "rna_de_aggregate_good_output.json",
            ["test_data_1.csv", "test_data_2.csv"],
        ),
    ]
    
    pass_test_ids = [
        "Pass with good test data",
    ]
    
    fail_test_data = [
        (
            # Fail with missing required dataset
            {
                "rnaseq_genotype_label_map": "rnaseq_genotype_label_map_good.csv",
                "mouse_gene_metadata": "mouse_gene_metadata_good.csv",
                "model_info": "model_info_good.csv",
                "biodom_genes_mm": "biodom_genes_mm_good.csv",
            },
            ValueError,
            ["test_data_1.csv"],
        ),
        (
            # Fail with missing required columns in data files
            {
                "rna_de_aggregate_data_files": "rna_de_aggregate_data_files_missing_column.csv",
                "rnaseq_genotype_label_map": "rnaseq_genotype_label_map_good.csv",
                "mouse_gene_metadata": "mouse_gene_metadata_good.csv",
                "model_info": "model_info_good.csv",
                "biodom_genes_mm": "biodom_genes_mm_good.csv",
            },
            ValueError,
            ["test_data_1.csv"],
        ),
    ]
    
    fail_test_ids = [
        "Fail with missing required dataset",
        "Fail with missing required columns in data files",
    ]

    def _load_test_data(self, input_files, mock_data_files):
        """Helper method to load test data and create datasets dictionary."""
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )
        return datasets

    def _mock_synapse_data(self, mock_data_files):
        """Helper method to mock Synapse data downloads."""
        def mock_get_entity_as_df(syn_id, source, syn):
            # Map syn_id to actual test data file
            if syn_id == "syn123456":
                return pd.read_csv(os.path.join(self.data_files_path, "input", "test_data_1.csv"))
            elif syn_id == "syn789012":
                return pd.read_csv(os.path.join(self.data_files_path, "input", "test_data_2.csv"))
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        return mock_get_entity_as_df

    @pytest.mark.parametrize(
        "input_files, expected_output_file, mock_data_files",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_rna_de_aggregate_should_pass(
        self, input_files, expected_output_file, mock_data_files
    ):
        datasets = self._load_test_data(input_files, mock_data_files)
        
        # Mock the Synapse client and data download
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = self._mock_synapse_data(mock_data_files)
            
            # Transform data
            output_data = transform_rna_de_aggregate(datasets=datasets)
        
        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)
        
        # Sort output data by ensembl_gene_id and model for deterministic comparison
        output_data_sorted = sorted(output_data, key=lambda x: (x["ensembl_gene_id"], x["name"]))
        expected_data_sorted = sorted(expected_data, key=lambda x: (x["ensembl_gene_id"], x["name"]))
        
        # Compare output with expected
        assert output_data_sorted == expected_data_sorted

    @pytest.mark.parametrize(
        "input_files, error_type, mock_data_files",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_rna_de_aggregate_should_fail(
        self, input_files, error_type, mock_data_files
    ):
        datasets = self._load_test_data(input_files, mock_data_files)
        
        # Mock the Synapse client
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = self._mock_synapse_data(mock_data_files)
            
            # Expect transformation to raise the specified error
            with pytest.raises(error_type):
                transform_rna_de_aggregate(datasets=datasets)

    def test_transform_rna_de_aggregate_empty_data_file(self):
        """Test handling of empty data files."""
        datasets = {
            "rna_de_aggregate_data_files": pd.DataFrame({
                "file_name": ["test_data_empty.csv"],
                "syn_id": ["syn_empty"]
            }),
            "rnaseq_genotype_label_map": pd.DataFrame({
                "model": ["APP/PS1"],
                "model_group": ["AD"],
                "display_label": ["APP/PS1 (Tg)"],
                "genotype": ["Tg"]
            }),
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG00000022892"],
                "gene_symbol": ["App"],
                "alias": [""]
            }),
            "model_info": pd.DataFrame({
                "model": ["APP/PS1"],
                "matched_controls": ["APP/PS1 (Wt)"],
                "model_type": ["Transgenic"]
            }),
            "biodom_genes_mm": pd.DataFrame({
                "biodomain": ["Synaptic"],
                "abbr": ["Axon"],
                "label": ["Axon Function"],
                "color": ["#FF6B6B"],
                "go_id": ["GO:0030424"],
                "goterm_name": ["axon"],
                "n_symbol": [1],
                "symbol": ["App"],
                "ensembl_id": ["ENSMUSG00000022892"]
            }),
        }
        
        def mock_get_entity_as_df_empty(syn_id, source, syn):
            if syn_id == "syn_empty":
                return pd.read_csv(os.path.join(self.data_files_path, "input", "test_data_empty.csv"))
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = mock_get_entity_as_df_empty
            
            # Should raise ValueError for empty data file
            with pytest.raises(ValueError, match="Data file is empty"):
                transform_rna_de_aggregate(datasets=datasets)

    def test_transform_rna_de_aggregate_missing_columns_in_data_file(self):
        """Test handling of data files with missing required columns."""
        datasets = {
            "rna_de_aggregate_data_files": pd.DataFrame({
                "file_name": ["test_data_missing_columns.csv"],
                "syn_id": ["syn_missing"]
            }),
            "rnaseq_genotype_label_map": pd.DataFrame({
                "model": ["APP/PS1"],
                "model_group": ["AD"],
                "display_label": ["APP/PS1 (Tg)"],
                "genotype": ["Tg"]
            }),
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG00000022892"],
                "gene_symbol": ["App"],
                "alias": [""]
            }),
            "model_info": pd.DataFrame({
                "model": ["APP/PS1"],
                "matched_controls": ["APP/PS1 (Wt)"],
                "model_type": ["Transgenic"]
            }),
            "biodom_genes_mm": pd.DataFrame({
                "biodomain": ["Synaptic"],
                "abbr": ["Axon"],
                "label": ["Axon Function"],
                "color": ["#FF6B6B"],
                "go_id": ["GO:0030424"],
                "goterm_name": ["axon"],
                "n_symbol": [1],
                "symbol": ["App"],
                "ensembl_id": ["ENSMUSG00000022892"]
            }),
        }
        
        def mock_get_entity_as_df_missing(syn_id, source, syn):
            if syn_id == "syn_missing":
                return pd.read_csv(os.path.join(self.data_files_path, "input", "test_data_missing_columns.csv"))
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = mock_get_entity_as_df_missing
            
            # Should raise ValueError for missing columns
            with pytest.raises(ValueError, match="Missing required columns"):
                transform_rna_de_aggregate(datasets=datasets)

    def test_transform_rna_de_aggregate_human_genes_filtered(self):
        """Test that human genes (ENSG*) are filtered out, keeping only mouse genes (ENSMUSG*)."""
        datasets = {
            "rna_de_aggregate_data_files": pd.DataFrame({
                "file_name": ["test_data_mixed_genes.csv"],
                "syn_id": ["syn_mixed"]
            }),
            "rnaseq_genotype_label_map": pd.DataFrame({
                "model": ["APP/PS1"],
                "model_group": ["AD"],
                "display_label": ["APP/PS1 (Tg)"],
                "genotype": ["Tg"]
            }),
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG00000022892", "ENSG00000142192"],
                "gene_symbol": ["App", "APP"],
                "alias": ["", ""]
            }),
            "model_info": pd.DataFrame({
                "model": ["APP/PS1"],
                "matched_controls": ["APP/PS1 (Wt)"],
                "model_type": ["Transgenic"]
            }),
            "biodom_genes_mm": pd.DataFrame({
                "biodomain": ["Synaptic"],
                "abbr": ["Axon"],
                "label": ["Axon Function"],
                "color": ["#FF6B6B"],
                "go_id": ["GO:0030424"],
                "goterm_name": ["axon"],
                "n_symbol": [1],
                "symbol": ["App"],
                "ensembl_id": ["ENSMUSG00000022892"]
            }),
        }
        
        # Create test data with both human and mouse genes
        mixed_data = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892", "ENSG00000142192", "ENSMUSG00000019969"],
            "log2FoldChange": [1.234567, 2.345678, -0.876543],
            "padj": [0.001, 0.002, 0.003],
            "model": ["APP/PS1", "APP/PS1", "APP/PS1"],
            "case": ["Tg", "Tg", "Tg"],
            "control": ["Wt", "Wt", "Wt"],
            "age": ["6 months", "6 months", "6 months"],
            "sex": ["Female", "Female", "Female"],
            "tissue": ["Hippocampus", "Hippocampus", "Hippocampus"]
        })
        
        def mock_get_entity_as_df_mixed(syn_id, source, syn):
            if syn_id == "syn_mixed":
                return mixed_data
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = mock_get_entity_as_df_mixed
            
            # Transform data
            output_data = transform_rna_de_aggregate(datasets=datasets)
        
        # Should only contain mouse genes (ENSMUSG*), not human genes (ENSG*)
        assert len(output_data) == 2  # Only 2 mouse genes
        for entry in output_data:
            assert entry["ensembl_gene_id"].startswith("ENSMUSG")

    def test_transform_rna_de_aggregate_jax_tissue_mapping(self):
        """Test that JAX models with 'Right Cerebral Hemisphere' tissue are mapped to 'Hemibrain'."""
        datasets = {
            "rna_de_aggregate_data_files": pd.DataFrame({
                "file_name": ["test_data_jax.csv"],
                "syn_id": ["syn_jax"]
            }),
            "rnaseq_genotype_label_map": pd.DataFrame({
                "model": ["JAX_model"],
                "model_group": ["AD"],
                "display_label": ["JAX_model (Tg)"],
                "genotype": ["Tg"]
            }),
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG00000022892"],
                "gene_symbol": ["App"],
                "alias": [""]
            }),
            "model_info": pd.DataFrame({
                "model": ["JAX_model"],
                "matched_controls": ["JAX_model (Wt)"],
                "model_type": ["Transgenic"]
            }),
            "biodom_genes_mm": pd.DataFrame({
                "biodomain": ["Synaptic"],
                "abbr": ["Axon"],
                "label": ["Axon Function"],
                "color": ["#FF6B6B"],
                "go_id": ["GO:0030424"],
                "goterm_name": ["axon"],
                "n_symbol": [1],
                "symbol": ["App"],
                "ensembl_id": ["ENSMUSG00000022892"]
            }),
        }
        
        # Create test data with JAX model and Right Cerebral Hemisphere tissue
        jax_data = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892"],
            "log2FoldChange": [1.234567],
            "padj": [0.001],
            "model": ["JAX_model"],
            "case": ["Tg"],
            "control": ["Wt"],
            "age": ["6 months"],
            "sex": ["Female"],
            "tissue": ["Right Cerebral Hemisphere"]
        })
        
        def mock_get_entity_as_df_jax(syn_id, source, syn):
            if syn_id == "syn_jax":
                return jax_data
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = mock_get_entity_as_df_jax
            
            # Transform data
            output_data = transform_rna_de_aggregate(datasets=datasets)
        
        # Should map tissue from "Right Cerebral Hemisphere" to "Hemibrain"
        assert len(output_data) == 1
        assert output_data[0]["tissue"] == "Hemibrain"
        assert "jax" in output_data[0]["name"].lower()

    def test_transform_rna_de_aggregate_age_sorting(self):
        """Test that age entries are sorted numerically."""
        datasets = {
            "rna_de_aggregate_data_files": pd.DataFrame({
                "file_name": ["test_data_age_sorting.csv"],
                "syn_id": ["syn_age"]
            }),
            "rnaseq_genotype_label_map": pd.DataFrame({
                "model": ["APP/PS1"],
                "model_group": ["AD"],
                "display_label": ["APP/PS1 (Tg)"],
                "genotype": ["Tg"]
            }),
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG00000022892"],
                "gene_symbol": ["App"],
                "alias": [""]
            }),
            "model_info": pd.DataFrame({
                "model": ["APP/PS1"],
                "matched_controls": ["APP/PS1 (Wt)"],
                "model_type": ["Transgenic"]
            }),
            "biodom_genes_mm": pd.DataFrame({
                "biodomain": ["Synaptic"],
                "abbr": ["Axon"],
                "label": ["Axon Function"],
                "color": ["#FF6B6B"],
                "go_id": ["GO:0030424"],
                "goterm_name": ["axon"],
                "n_symbol": [1],
                "symbol": ["App"],
                "ensembl_id": ["ENSMUSG00000022892"]
            }),
        }
        
        # Create test data with ages in non-numerical order
        age_data = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892", "ENSMUSG00000022892", "ENSMUSG00000022892"],
            "log2FoldChange": [1.234567, 2.345678, 0.876543],
            "padj": [0.001, 0.002, 0.003],
            "model": ["APP/PS1", "APP/PS1", "APP/PS1"],
            "case": ["Tg", "Tg", "Tg"],
            "control": ["Wt", "Wt", "Wt"],
            "age": ["12 months", "3 months", "6 months"],  # Non-numerical order
            "sex": ["Female", "Female", "Female"],
            "tissue": ["Hippocampus", "Hippocampus", "Hippocampus"]
        })
        
        def mock_get_entity_as_df_age(syn_id, source, syn):
            if syn_id == "syn_age":
                return age_data
            else:
                raise ValueError(f"Unknown syn_id: {syn_id}")
        
        with mock.patch('agoradatatools.etl.transform.rna_de_aggregate._login_to_synapse') as mock_login, \
             mock.patch('agoradatatools.etl.transform.rna_de_aggregate.get_entity_as_df') as mock_get_entity:
            
            mock_syn = mock.MagicMock()
            mock_login.return_value = mock_syn
            mock_get_entity.side_effect = mock_get_entity_as_df_age
            
            # Transform data
            output_data = transform_rna_de_aggregate(datasets=datasets)
        
        # Should have ages sorted numerically
        assert len(output_data) == 1
        entry = output_data[0]
        age_keys = [key for key in entry.keys() if key not in [
            "ensembl_gene_id", "gene_symbol", "biodomains", "name", 
            "matched_control", "model_group", "model_type", "tissue", "sex"
        ]]
        # Age keys should be in numerical order: 3 months, 6 months, 12 months
        assert age_keys == ["3 months", "6 months", "12 months"]


class TestQuickValidateDataFile:
    """Test class for the _quick_validate_data_file function."""

    def test_quick_validate_data_file_should_pass(self):
        """Test validation with good data file."""
        data_file = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892"],
            "log2FoldChange": [1.234567],
            "padj": [0.001],
            "model": ["APP/PS1"],
            "case": ["Tg"],
            "control": ["Wt"],
            "age": ["6 months"],
            "sex": ["Female"],
            "tissue": ["Hippocampus"]
        })
        
        # Should not raise any exception
        _quick_validate_data_file("test_file.csv", data_file)

    def test_quick_validate_data_file_empty_dataframe(self):
        """Test validation with empty data file."""
        data_file = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Data file is empty"):
            _quick_validate_data_file("test_file.csv", data_file)

    def test_quick_validate_data_file_missing_columns(self):
        """Test validation with missing required columns."""
        data_file = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892"],
            "log2FoldChange": [1.234567],
            "model": ["APP/PS1"],
            "case": ["Tg"],
            "control": ["Wt"],
            "age": ["6 months"],
            "sex": ["Female"],
            "tissue": ["Hippocampus"]
            # Missing 'padj' column
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            _quick_validate_data_file("test_file.csv", data_file)

    def test_quick_validate_data_file_custom_required_columns(self):
        """Test validation with custom required columns."""
        data_file = pd.DataFrame({
            "ensembl_gene_id": ["ENSMUSG00000022892"],
            "log2FoldChange": [1.234567],
            "padj": [0.001],
            "model": ["APP/PS1"],
            "case": ["Tg"],
            "control": ["Wt"],
            "age": ["6 months"],
            "sex": ["Female"],
            "tissue": ["Hippocampus"],
            "extra_column": ["extra_value"]
        })
        
        custom_required_columns = ["ensembl_gene_id", "log2FoldChange", "model"]
        
        # Should not raise any exception
        _quick_validate_data_file("test_file.csv", data_file, custom_required_columns)
