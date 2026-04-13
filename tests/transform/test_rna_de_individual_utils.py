"""
Test suite for RNA-seq individual transform utility functions.

This module contains comprehensive tests for the utility functions in rna_de_individual_utils
that are used by the rna_de_individual transform.
"""

import pandas as pd
import pytest
import logging

from agoradatatools.etl.transform.rna_de_individual_utils import (
    filter_to_mouse_genes,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    prepare_genotype_label_map_df,
    log_file_processing_info,
    validate_data_file_not_empty,
    preprocess_data_file,
)


class TestFilterMouseGenes:
    """Tests for filter_to_mouse_genes function."""

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

        result = filter_to_mouse_genes(df)

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

        result = filter_to_mouse_genes(df)

        assert len(result) == 3

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({"ensembl_gene_id": pd.Series([], dtype=str), "value": []})

        result = filter_to_mouse_genes(df)

        assert len(result) == 0


class TestTissueNameMapping:
    """Tests for the tissue name mapping applied inside preprocess_data_file."""

    _REQUIRED_COLUMNS = [
        "ensembl_gene_id",
        "expression",
        "model",
        "genotype",
        "age",
        "sex",
        "tissue",
        "individualid",
    ]

    @staticmethod
    def _make_df(tissues: list) -> pd.DataFrame:
        """Build a minimal valid DataFrame with the given tissue values."""
        n = len(tissues)
        return pd.DataFrame(
            {
                "ensembl_gene_id": [f"ENSMUSG{i:011d}" for i in range(n)],
                "expression": [1.0] * n,
                "model": ["Model_A"] * n,
                "genotype": ["Tg"] * n,
                "age": ["4 months"] * n,
                "sex": ["Male"] * n,
                "tissue": tissues,
                "individualid": [f"ID{i}" for i in range(n)],
            }
        )

    @classmethod
    def _preprocess(cls, tissues: list) -> pd.Series:
        df = cls._make_df(tissues)
        result = preprocess_data_file("test.csv", df, 0, 1, cls._REQUIRED_COLUMNS)
        return result["tissue"].reset_index(drop=True)

    def test_maps_right_cerebral_hemisphere(self) -> None:
        """'Right Cerebral Hemisphere' is mapped to 'Hemibrain'."""
        assert self._preprocess(["Right Cerebral Hemisphere"]).iloc[0] == "Hemibrain"

    def test_applies_sentence_case_to_other_tissues(self) -> None:
        """Other tissue names are converted to sentence case."""
        result = self._preprocess(
            ["cortex", "hippocampus", "cerebellum", "CORTEX", "HIPPOCAMPUS"]
        )
        expected = pd.Series(
            ["Cortex", "Hippocampus", "Cerebellum", "Cortex", "Hippocampus"]
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_preserves_sentence_case_tissues(self) -> None:
        """Properly formatted tissue names are unchanged."""
        result = self._preprocess(["Cortex", "Hippocampus"])
        expected = pd.Series(["Cortex", "Hippocampus"])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_mixed_tissue_names(self) -> None:
        """Mixed tissue names including the special mapping are all handled correctly."""
        result = self._preprocess(
            ["Right Cerebral Hemisphere", "hippocampus", "CORTEX", "Cerebellum"]
        )
        expected = pd.Series(["Hemibrain", "Hippocampus", "Cortex", "Cerebellum"])
        pd.testing.assert_series_equal(result, expected, check_names=False)


class TestPreprocessDataFileTypeCasting:
    """Tests for column type casting applied inside preprocess_data_file."""

    _REQUIRED_COLUMNS = [
        "ensembl_gene_id",
        "expression",
        "model",
        "genotype",
        "age",
        "sex",
        "tissue",
        "individualid",
    ]

    @staticmethod
    def _make_df(expression_values, individualid_values) -> pd.DataFrame:
        """Build a minimal valid DataFrame with the given expression and individualid values."""
        n = len(expression_values)
        return pd.DataFrame(
            {
                "ensembl_gene_id": [f"ENSMUSG{i:011d}" for i in range(n)],
                "expression": expression_values,
                "model": ["Model_A"] * n,
                "genotype": ["Tg"] * n,
                "age": ["4 months"] * n,
                "sex": ["Male"] * n,
                "tissue": ["Cortex"] * n,
                "individualid": individualid_values,
            }
        )

    def test_expression_string_is_cast_to_float(self) -> None:
        """expression values arriving as strings are cast to float before rounding."""
        df = self._make_df(
            expression_values=["1.123456789", "2.987654321"],
            individualid_values=["ID0", "ID1"],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS)
        assert result["expression"].dtype == float

    def test_expression_string_is_rounded_to_5_decimals(self) -> None:
        """expression values from strings are correctly rounded after casting."""
        df = self._make_df(
            expression_values=["1.123456789"],
            individualid_values=["ID0"],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS)
        assert result["expression"].iloc[0] == pytest.approx(1.12346)

    def test_individualid_numeric_is_cast_to_str(self) -> None:
        """individualid values arriving as integers are cast to str."""
        df = self._make_df(
            expression_values=[1.0, 2.0],
            individualid_values=[101, 202],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS)
        assert result["individualid"].dtype == object
        assert result["individualid"].iloc[0] == "101"
        assert result["individualid"].iloc[1] == "202"

    def test_expression_already_float_unchanged(self) -> None:
        """expression values that are already float pass through without error."""
        df = self._make_df(
            expression_values=[1.5, 2.5],
            individualid_values=["ID0", "ID1"],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS)
        assert result["expression"].dtype == float


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

    def test_none_model_groups(self) -> None:
        """Test handling of None model_group values (models with no explicit group)."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": [None, None],
            }
        )

        # Should not raise — all-None model_group is consistently "no group"
        validate_model_group_consistency(df)

    def test_empty_string_model_groups(self) -> None:
        """Test that consistent empty-string model_group values do not raise an error.

        In practice, empty strings are normalized to None by prepare_genotype_label_map_df
        before this function is called. Consistent "" values are still considered valid
        because all rows agree on the same value.
        """
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": ["", ""],
            }
        )

        # Should not raise — both rows agree on the same value
        validate_model_group_consistency(df)

    def test_mixed_none_and_real_group_raises_error(self) -> None:
        """Test that a model with both None and a real group name raises ValueError.

        Mixing None (no group) with an actual group name for the same model is
        inconsistent and should be caught by validation.
        """
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": [None, "GroupX"],
            }
        )

        with pytest.raises(ValueError, match="consistent model_group value"):
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


class TestPrepareGenotypeLabelMapDf:
    """Tests for prepare_genotype_label_map_df function."""

    def test_preserves_model_group_value(self) -> None:
        """Test that a non-empty model_group is preserved in the output."""
        df = pd.DataFrame(
            {
                "model": ["Model_B"],
                "genotype": ["Carrier"],
                "display_label": ["Model_B"],
                "model_group": ["GroupX"],
                "result_order": [2],
            }
        )

        result = prepare_genotype_label_map_df(df)

        pd.testing.assert_frame_equal(result, df)

    def test_normalizes_empty_model_group_to_none(self) -> None:
        """Test that model_group values of None or '' are normalized to None in the output.

        model_group uses None (not "") to represent "no explicit group", so that the
        output field is null rather than an empty string.
        """
        df_none = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "model_group": [None],
                "result_order": [1],
            }
        )
        df_empty = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "model_group": [""],
                "result_order": [1],
            }
        )

        assert prepare_genotype_label_map_df(df_none)["model_group"].iloc[0] is None
        assert prepare_genotype_label_map_df(df_empty)["model_group"].iloc[0] is None

    def test_converts_result_order_to_int(self) -> None:
        """Test that result_order is cast to int."""
        df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "model_group": ["GroupA"],
                "result_order": ["3"],
            }
        )

        result = prepare_genotype_label_map_df(df)

        assert result["result_order"].dtype == int
        assert result["result_order"].iloc[0] == 3

    def test_does_not_mutate_input(self) -> None:
        """Test that the original DataFrame is not modified."""
        df = pd.DataFrame(
            {
                "model": ["Model_A"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "model_group": ["GroupA"],
                "result_order": [1],
            }
        )
        original_model_group = df["model_group"].iloc[0]

        prepare_genotype_label_map_df(df)

        assert df["model_group"].iloc[0] == original_model_group

    def test_multiple_rows_preserved(self) -> None:
        """Test that all rows are preserved with their model_group values intact."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_B"],
                "genotype": ["Tg", "Carrier"],
                "display_label": ["Transgenic", "Model_B"],
                "model_group": ["GroupA", "GroupX"],
                "result_order": [2, 1],
            }
        )

        result = prepare_genotype_label_map_df(df)

        pd.testing.assert_frame_equal(result, df)


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
        filtered_data = filter_to_mouse_genes(data_df)

        # Verify results
        assert len(filtered_data) == 2
        assert gene_metadata_dict["ENSMUSG00000000001"] == "Gene1"
