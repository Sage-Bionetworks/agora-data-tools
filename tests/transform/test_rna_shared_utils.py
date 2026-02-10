"""
Test suite for RNA-seq shared utilities.

This module contains comprehensive tests for the shared utility functions used by
multiple RNA-seq transforms (rna_de_aggregate and rna_de_individual).
"""

import pandas as pd
import pytest
import logging

from agoradatatools.etl.transform.rna_shared_utils import (
    filter_mouse_genes,
    map_jax_tissue_name,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    log_file_processing_info,
    validate_data_file_not_empty,
    normalize_model_group_value,
    extract_common_metadata,
)


class TestFilterMouseGenes:
    """Tests for filter_mouse_genes function."""

    def test_filters_human_genes(self) -> None:
        """Test that human genes (ENSG*) are filtered out."""
        df = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSG00000000001",
                    "ENSMUSG00000000002",
                ],
                "value": [1, 2, 3],
            }
        )

        result = filter_mouse_genes(df)

        assert len(result) == 2
        assert all(result["ensembl_gene_id"].str.startswith("ENSMUSG"))
        assert "ENSG00000000001" not in result["ensembl_gene_id"].values

    def test_keeps_all_mouse_genes(self) -> None:
        """Test that all mouse genes are kept."""
        df = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000002",
                    "ENSMUSG00000000003",
                ],
                "value": [1, 2, 3],
            }
        )

        result = filter_mouse_genes(df)

        assert len(result) == 3

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({"ensembl_gene_id": pd.Series([], dtype=str), "value": []})

        result = filter_mouse_genes(df)

        assert len(result) == 0


class TestMapJaxTissueName:
    """Tests for map_jax_tissue_name function."""

    def test_maps_right_cerebral_hemisphere(self) -> None:
        """Test that 'Right Cerebral Hemisphere' is mapped to 'Hemibrain'."""
        result = map_jax_tissue_name("Right Cerebral Hemisphere")
        assert result == "Hemibrain"

    def test_keeps_other_tissue_names(self) -> None:
        """Test that other tissue names are unchanged."""
        assert map_jax_tissue_name("Cortex") == "Cortex"
        assert map_jax_tissue_name("Hippocampus") == "Hippocampus"
        assert map_jax_tissue_name("Cerebellum") == "Cerebellum"

    def test_case_sensitive(self) -> None:
        """Test that mapping is case-sensitive."""
        assert (
            map_jax_tissue_name("right cerebral hemisphere")
            == "right cerebral hemisphere"
        )


class TestValidateModelGroupConsistency:
    """Tests for validate_model_group_consistency function."""

    def test_consistent_model_groups(self) -> None:
        """Test that consistent model_group values pass validation."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B", "Model_B"],
                "model_group": ["Group1", "Group1", "Group2", "Group2"],
            }
        )

        # Should not raise
        validate_model_group_consistency(df)

    def test_inconsistent_model_groups_raises_error(self) -> None:
        """Test that inconsistent model_group values raise ValueError."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B", "Model_B"],
                "model_group": ["Group1", "Group2", "Group3", "Group3"],
            }
        )

        with pytest.raises(ValueError, match="consistent model_group value"):
            validate_model_group_consistency(df)

    def test_empty_model_groups(self) -> None:
        """Test handling of empty model_group values."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": ["", ""],
            }
        )

        # Should not raise
        validate_model_group_consistency(df)


class TestCreateGeneMetadataDict:
    """Tests for create_gene_metadata_dict function."""

    def test_creates_correct_mapping(self) -> None:
        """Test that dictionary is created correctly."""
        df = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                "gene_symbol": ["Gene1", "Gene2"],
            }
        )

        result = create_gene_metadata_dict(df)

        assert result == {
            "ENSMUSG00000000001": "Gene1",
            "ENSMUSG00000000002": "Gene2",
        }

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({"ensembl_gene_id": [], "gene_symbol": []})

        result = create_gene_metadata_dict(df)

        assert result == {}


class TestLogFileProcessingInfo:
    """Tests for log_file_processing_info function."""

    def test_logs_information(self, caplog) -> None:
        """Test that file processing information is logged."""
        df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": [4, 5, 6],
            }
        )

        with caplog.at_level(logging.INFO):
            log_file_processing_info("test.csv", 0, 5, df)

        assert "Processing test.csv (1/5)" in caplog.text
        assert "3 rows" in caplog.text
        assert "2 columns" in caplog.text


class TestValidateDataFileNotEmpty:
    """Tests for validate_data_file_not_empty function."""

    def test_raises_error_for_empty_file(self) -> None:
        """Test that empty file raises ValueError."""
        df = pd.DataFrame()

        with pytest.raises(ValueError, match="Data file test.csv is empty"):
            validate_data_file_not_empty("test.csv", df)

    def test_passes_for_non_empty_file(self) -> None:
        """Test that non-empty file passes validation."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Should not raise
        validate_data_file_not_empty("test.csv", df)


class TestNormalizeModelGroupValue:
    """Tests for normalize_model_group_value function."""

    def test_converts_empty_string_to_none(self) -> None:
        """Test that empty string is converted to None."""
        assert normalize_model_group_value("") is None

    def test_keeps_non_empty_strings(self) -> None:
        """Test that non-empty strings are kept."""
        assert normalize_model_group_value("Group1") == "Group1"
        assert normalize_model_group_value("5xFAD") == "5xFAD"

    def test_whitespace_is_not_converted(self) -> None:
        """Test that whitespace strings are not converted to None."""
        assert normalize_model_group_value("  ") == "  "


class TestExtractCommonMetadata:
    """Tests for extract_common_metadata function."""

    def test_extracts_all_metadata(self) -> None:
        """Test that all metadata fields are extracted correctly."""
        gene_metadata_dict = {
            "ENSMUSG00000000001": "Gene1",
        }

        result = extract_common_metadata(
            "ENSMUSG00000000001", "Cortex", gene_metadata_dict
        )

        assert result == {
            "ensembl_gene_id": "ENSMUSG00000000001",
            "gene_symbol": "Gene1",
            "tissue": "Cortex",
        }

    def test_maps_jax_tissue(self) -> None:
        """Test that JAX tissue name is mapped."""
        result = extract_common_metadata(
            "ENSMUSG00000000001", "Right Cerebral Hemisphere", {}
        )

        assert result["tissue"] == "Hemibrain"

    def test_handles_missing_gene_symbol(self) -> None:
        """Test that missing gene symbol returns empty string."""
        result = extract_common_metadata("ENSMUSG00000000001", "Cortex", {})

        assert result["gene_symbol"] == ""

    def test_preserves_ensembl_gene_id(self) -> None:
        """Test that ensembl_gene_id is preserved."""
        result = extract_common_metadata("ENSMUSG00000099999", "Cortex", {})

        assert result["ensembl_gene_id"] == "ENSMUSG00000099999"


class TestIntegration:
    """Integration tests for multiple functions working together."""

    def test_complete_workflow(self) -> None:
        """Test a complete workflow using multiple shared utilities."""
        # Create test data
        genotype_df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B", "Model_B"],
                "genotype": ["Geno1", "Geno2", "Geno1", "Geno2"],
                "display_label": ["Label1", "Label2", "Label3", "Label4"],
                "model_group": ["Group1", "Group1", "Group2", "Group2"],
            }
        )

        gene_metadata_df = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                "gene_symbol": ["Gene1", "Gene2"],
            }
        )

        data_df = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSG00000000001",
                    "ENSMUSG00000000002",
                ],
                "value": [1, 2, 3],
            }
        )

        # Test workflow
        validate_model_group_consistency(genotype_df)
        gene_metadata_dict = create_gene_metadata_dict(gene_metadata_df)
        filtered_data = filter_mouse_genes(data_df)

        # Verify results
        assert len(filtered_data) == 2
        assert gene_metadata_dict["ENSMUSG00000000001"] == "Gene1"

        # Test metadata extraction
        metadata = extract_common_metadata(
            "ENSMUSG00000000001", "Right Cerebral Hemisphere", gene_metadata_dict
        )
        assert metadata["gene_symbol"] == "Gene1"
        assert metadata["tissue"] == "Hemibrain"
