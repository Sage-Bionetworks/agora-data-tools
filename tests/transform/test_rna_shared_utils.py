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
    validate_and_sort_age_entries,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    create_model_group_dict,
    create_label_map_dict,
    log_file_processing_info,
    validate_data_file_not_empty,
    normalize_model_group_value,
    extract_common_metadata,
    resolve_genotypes_to_display_labels,
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


class TestValidateAndSortAgeEntries:
    """Tests for validate_and_sort_age_entries function."""

    def test_sorts_ages_numerically(self) -> None:
        """Test that ages are sorted numerically."""
        age_entries = {
            "12 months": {"log2_fc": 0.8},
            "4 months": {"log2_fc": 0.3},
            "6 months": {"log2_fc": 0.5},
        }

        result = validate_and_sort_age_entries(
            age_entries, "ENSMUSG00000000001", "Model_A", "Cortex", "Male"
        )

        assert list(result.keys()) == ["4 months", "6 months", "12 months"]

    def test_single_age(self) -> None:
        """Test handling of single age entry."""
        age_entries = {"6 months": {"log2_fc": 0.5}}

        result = validate_and_sort_age_entries(
            age_entries, "ENSMUSG00000000001", "Model_A", "Cortex", "Male"
        )

        assert result == age_entries

    def test_empty_age_raises_error(self) -> None:
        """Test that empty age string raises ValueError."""
        age_entries = {"": {"log2_fc": 0.5}}

        with pytest.raises(ValueError, match="Empty or whitespace-only age value"):
            validate_and_sort_age_entries(
                age_entries, "ENSMUSG00000000001", "Model_A", "Cortex", "Male"
            )

    def test_invalid_age_format_raises_error(self) -> None:
        """Test that invalid age format raises ValueError."""
        age_entries = {"6months": {"log2_fc": 0.5}}

        with pytest.raises(ValueError, match="Invalid age format"):
            validate_and_sort_age_entries(
                age_entries, "ENSMUSG00000000001", "Model_A", "Cortex", "Male"
            )

    def test_empty_dict(self) -> None:
        """Test handling of empty age entries dictionary."""
        age_entries = {}

        result = validate_and_sort_age_entries(
            age_entries, "ENSMUSG00000000001", "Model_A", "Cortex", "Male"
        )

        assert result == {}


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


class TestCreateModelGroupDict:
    """Tests for create_model_group_dict function."""

    def test_creates_correct_mapping(self) -> None:
        """Test that dictionary is created correctly."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B", "Model_B"],
                "model_group": ["Group1", "Group1", "Group2", "Group2"],
            }
        )

        result = create_model_group_dict(df)

        assert result == {
            "Model_A": "Group1",
            "Model_B": "Group2",
        }

    def test_takes_first_value_per_model(self) -> None:
        """Test that first value is taken when multiple exist."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": ["Group1", "Group1"],
            }
        )

        result = create_model_group_dict(df)

        assert result == {"Model_A": "Group1"}


class TestCreateLabelMapDict:
    """Tests for create_label_map_dict function."""

    def test_creates_correct_mapping(self) -> None:
        """Test that dictionary is created correctly."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B"],
                "genotype": ["Geno1", "Geno2", "Geno1"],
                "display_label": ["Label1", "Label2", "Label3"],
            }
        )

        result = create_label_map_dict(df)

        assert result == {
            ("Model_A", "Geno1"): "Label1",
            ("Model_A", "Geno2"): "Label2",
            ("Model_B", "Geno1"): "Label3",
        }

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({"model": [], "genotype": [], "display_label": []})

        result = create_label_map_dict(df)

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
        model_group_dict = create_model_group_dict(genotype_df)
        label_map_dict = create_label_map_dict(genotype_df)
        gene_metadata_dict = create_gene_metadata_dict(gene_metadata_df)
        filtered_data = filter_mouse_genes(data_df)

        # Verify results
        assert len(filtered_data) == 2
        assert model_group_dict["Model_A"] == "Group1"
        assert label_map_dict[("Model_A", "Geno1")] == "Label1"
        assert gene_metadata_dict["ENSMUSG00000000001"] == "Gene1"

        # Test metadata extraction
        metadata = extract_common_metadata(
            "ENSMUSG00000000001", "Right Cerebral Hemisphere", gene_metadata_dict
        )
        assert metadata["gene_symbol"] == "Gene1"
        assert metadata["tissue"] == "Hemibrain"


class TestResolveGenotypesToDisplayLabels:
    """Tests for resolve_genotypes_to_display_labels function."""

    def test_resolves_both_genotypes_successfully(self) -> None:
        """Test that both case and control genotypes are resolved to their display labels."""
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }

        name, matched_control = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_A",
            case="Tg",
            control="Wt",
            ensembl_gene_id="ENSMUSG00000000001",
            tissue="Cortex",
            sex="Male",
        )

        assert name == "Transgenic"
        assert matched_control == "Wildtype"

    def test_resolves_multiple_models(self) -> None:
        """Test that different models can have different display labels for the same genotype."""
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic_A",
            ("Model_A", "Wt"): "Wildtype_A",
            ("Model_B", "Tg"): "Transgenic_B",
            ("Model_B", "Wt"): "Wildtype_B",
        }

        # Test Model_A
        name_a, control_a = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_A",
            case="Tg",
            control="Wt",
            ensembl_gene_id="ENSMUSG00000000001",
            tissue="Cortex",
            sex="Male",
        )

        assert name_a == "Transgenic_A"
        assert control_a == "Wildtype_A"

        # Test Model_B
        name_b, control_b = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_B",
            case="Tg",
            control="Wt",
            ensembl_gene_id="ENSMUSG00000000002",
            tissue="Hippocampus",
            sex="Female",
        )

        assert name_b == "Transgenic_B"
        assert control_b == "Wildtype_B"

    def test_missing_case_genotype_raises_error(self) -> None:
        """Test that missing case genotype in label_map_dict raises ValueError."""
        label_map_dict = {
            ("Model_A", "Wt"): "Wildtype",
            # Missing entry for ("Model_A", "Tg")
        }

        with pytest.raises(ValueError) as exc_info:
            resolve_genotypes_to_display_labels(
                label_map_dict=label_map_dict,
                name="Model_A",
                case="Tg",
                control="Wt",
                ensembl_gene_id="ENSMUSG00000000001",
                tissue="Cortex",
                sex="Male",
            )

        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_A" in error_message
        assert "Tg" in error_message
        assert "ENSMUSG00000000001" in error_message
        assert "Cortex" in error_message
        assert "Male" in error_message

    def test_missing_control_genotype_raises_error(self) -> None:
        """Test that missing control genotype in label_map_dict raises ValueError."""
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            # Missing entry for ("Model_A", "Wt")
        }

        with pytest.raises(ValueError) as exc_info:
            resolve_genotypes_to_display_labels(
                label_map_dict=label_map_dict,
                name="Model_A",
                case="Tg",
                control="Wt",
                ensembl_gene_id="ENSMUSG00000000002",
                tissue="Hippocampus",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_A" in error_message
        assert "Wt" in error_message
        assert "ENSMUSG00000000002" in error_message
        assert "Hippocampus" in error_message
        assert "Female" in error_message

    def test_missing_both_genotypes_raises_case_error_first(self) -> None:
        """Test that when both genotypes are missing, case error is raised first."""
        label_map_dict = {}

        with pytest.raises(ValueError) as exc_info:
            resolve_genotypes_to_display_labels(
                label_map_dict=label_map_dict,
                name="Model_A",
                case="Tg",
                control="Wt",
                ensembl_gene_id="ENSMUSG00000000003",
                tissue="Striatum",
                sex="Male",
            )

        # Should raise error for case first, since case is checked before control
        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_A" in error_message
        assert "Tg" in error_message

    def test_missing_model_raises_error(self) -> None:
        """Test that missing model (not in label_map_dict) raises ValueError."""
        label_map_dict = {
            ("Model_A", "Tg"): "Transgenic",
            ("Model_A", "Wt"): "Wildtype",
        }

        with pytest.raises(ValueError) as exc_info:
            resolve_genotypes_to_display_labels(
                label_map_dict=label_map_dict,
                name="Model_B",
                case="Tg",
                control="Wt",
                ensembl_gene_id="ENSMUSG00000000004",
                tissue="Cortex",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Label mapping not found for genotype" in error_message
        assert "Model_B" in error_message

    def test_error_message_includes_all_context(self) -> None:
        """Test that error message includes all relevant context information."""
        label_map_dict = {
            ("5xFAD", "Wt"): "Wildtype",
        }

        with pytest.raises(ValueError) as exc_info:
            resolve_genotypes_to_display_labels(
                label_map_dict=label_map_dict,
                name="5xFAD",
                case="Het",
                control="Wt",
                ensembl_gene_id="ENSMUSG00000051951",
                tissue="Hemibrain",
                sex="Female",
            )

        error_message = str(exc_info.value)
        # Verify all context is in error message
        assert "5xFAD" in error_message
        assert "Het" in error_message
        assert "ENSMUSG00000051951" in error_message
        assert "Hemibrain" in error_message
        assert "Female" in error_message
        assert "rnaseq_genotype_label_map" in error_message

    def test_different_genotype_names(self) -> None:
        """Test that function works with various genotype naming conventions."""
        label_map_dict = {
            ("Model_X", "Het"): "Heterozygous",
            ("Model_X", "WT"): "Wild Type",
            ("Model_Y", "Homo"): "Homozygous",
            ("Model_Y", "Control"): "Control",
        }

        # Test Model_X with Het/WT
        name_x, control_x = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_X",
            case="Het",
            control="WT",
            ensembl_gene_id="ENSMUSG00000000005",
            tissue="Cortex",
            sex="Male",
        )

        assert name_x == "Heterozygous"
        assert control_x == "Wild Type"

        # Test Model_Y with Homo/Control
        name_y, control_y = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_Y",
            case="Homo",
            control="Control",
            ensembl_gene_id="ENSMUSG00000000006",
            tissue="Hippocampus",
            sex="Female",
        )

        assert name_y == "Homozygous"
        assert control_y == "Control"

    def test_case_sensitive_lookups(self) -> None:
        """Test that genotype lookups are case-sensitive."""
        label_map_dict = {
            ("Model_A", "tg"): "Transgenic Lower",
            ("Model_A", "Tg"): "Transgenic Title",
            ("Model_A", "wt"): "Wildtype Lower",
            ("Model_A", "Wt"): "Wildtype Title",
        }

        # Test with title case
        name_title, control_title = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_A",
            case="Tg",
            control="Wt",
            ensembl_gene_id="ENSMUSG00000000007",
            tissue="Cortex",
            sex="Male",
        )

        assert name_title == "Transgenic Title"
        assert control_title == "Wildtype Title"

        # Test with lower case
        name_lower, control_lower = resolve_genotypes_to_display_labels(
            label_map_dict=label_map_dict,
            name="Model_A",
            case="tg",
            control="wt",
            ensembl_gene_id="ENSMUSG00000000008",
            tissue="Cortex",
            sex="Female",
        )

        assert name_lower == "Transgenic Lower"
        assert control_lower == "Wildtype Lower"
