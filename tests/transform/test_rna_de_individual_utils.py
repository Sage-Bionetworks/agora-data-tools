"""
Test suite for RNA-seq individual transform utility functions.

This module contains comprehensive tests for the utility functions in rna_de_individual_utils
that are used by the rna_de_individual transform.
"""

import pandas as pd
import pytest
import logging

from agoradatatools.etl.transform.rna_de_individual_utils import (
    filter_mouse_genes,
    convert_to_sentence_case,
    convert_sex_to_sentence_case,
    map_jax_tissue_name,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    create_genotype_metadata_dict,
    log_file_processing_info,
    validate_data_file_not_empty,
    normalize_model_group_value,
    extract_common_metadata,
    process_data_files,
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


class TestConvertToSentenceCase:
    """Tests for convert_to_sentence_case function."""

    def test_converts_lowercase_to_sentence_case(self) -> None:
        """Test that lowercase text is converted to sentence case."""
        assert convert_to_sentence_case("cortex") == "Cortex"
        assert convert_to_sentence_case("hippocampus") == "Hippocampus"

    def test_converts_uppercase_to_sentence_case(self) -> None:
        """Test that uppercase text is converted to sentence case."""
        assert convert_to_sentence_case("CORTEX") == "Cortex"
        assert convert_to_sentence_case("HIPPOCAMPUS") == "Hippocampus"

    def test_converts_mixed_case_to_sentence_case(self) -> None:
        """Test that mixed case text is converted to sentence case."""
        assert convert_to_sentence_case("CoRtEx") == "Cortex"
        assert convert_to_sentence_case("HiPpOcAmPuS") == "Hippocampus"

    def test_handles_empty_string(self) -> None:
        """Test that empty string is handled correctly."""
        assert convert_to_sentence_case("") == ""

    def test_handles_single_character(self) -> None:
        """Test that single character strings work."""
        assert convert_to_sentence_case("a") == "A"
        assert convert_to_sentence_case("Z") == "Z"


class TestConvertSexToSentenceCase:
    """Tests for convert_sex_to_sentence_case function."""

    def test_converts_m_to_male(self) -> None:
        """Test that 'M' is converted to 'Male'."""
        assert convert_sex_to_sentence_case("M") == "Male"
        assert convert_sex_to_sentence_case("m") == "Male"

    def test_converts_male_variations_to_male(self) -> None:
        """Test that various 'male' formats are converted to 'Male'."""
        assert convert_sex_to_sentence_case("male") == "Male"
        assert convert_sex_to_sentence_case("MALE") == "Male"
        assert convert_sex_to_sentence_case("Male") == "Male"

    def test_converts_f_to_female(self) -> None:
        """Test that 'F' is converted to 'Female'."""
        assert convert_sex_to_sentence_case("F") == "Female"
        assert convert_sex_to_sentence_case("f") == "Female"

    def test_converts_female_variations_to_female(self) -> None:
        """Test that various 'female' formats are converted to 'Female'."""
        assert convert_sex_to_sentence_case("female") == "Female"
        assert convert_sex_to_sentence_case("FEMALE") == "Female"
        assert convert_sex_to_sentence_case("Female") == "Female"

    def test_handles_empty_string(self) -> None:
        """Test that empty string is handled correctly."""
        assert convert_sex_to_sentence_case("") == ""

    def test_handles_other_values(self) -> None:
        """Test that other values are converted to sentence case."""
        assert convert_sex_to_sentence_case("unknown") == "Unknown"
        assert convert_sex_to_sentence_case("OTHER") == "Other"


class TestMapJaxTissueName:
    """Tests for map_jax_tissue_name function."""

    def test_maps_right_cerebral_hemisphere(self) -> None:
        """Test that 'Right Cerebral Hemisphere' is mapped to 'Hemibrain'."""
        result = map_jax_tissue_name("Right Cerebral Hemisphere")
        assert result == "Hemibrain"

    def test_applies_sentence_case_to_other_tissues(self) -> None:
        """Test that other tissue names are converted to sentence case."""
        assert map_jax_tissue_name("cortex") == "Cortex"
        assert map_jax_tissue_name("hippocampus") == "Hippocampus"
        assert map_jax_tissue_name("cerebellum") == "Cerebellum"
        assert map_jax_tissue_name("CORTEX") == "Cortex"
        assert map_jax_tissue_name("HIPPOCAMPUS") == "Hippocampus"

    def test_preserves_sentence_case_tissues(self) -> None:
        """Test that properly formatted tissues remain the same."""
        assert map_jax_tissue_name("Cortex") == "Cortex"
        assert map_jax_tissue_name("Hippocampus") == "Hippocampus"

    def test_special_mapping_takes_precedence(self) -> None:
        """Test that special mappings are applied before sentence case."""
        # This is already sentence case but should still be mapped to Hemibrain
        assert map_jax_tissue_name("Right Cerebral Hemisphere") == "Hemibrain"


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


class TestCreateGenotypeMetadataDict:
    """Tests for create_genotype_metadata_dict function."""

    def test_basic_metadata_without_result_order(self) -> None:
        """Test creating metadata dict without result_order (aggregate mode)."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B"],
                "genotype": ["Tg", "Wt", "Tg"],
                "display_label": ["Transgenic", "Wildtype", "Transgenic_B"],
                "model_group": ["Group1", "Group1", "Group2"],
            }
        )

        result = create_genotype_metadata_dict(df, include_result_order=False)

        assert result == {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "model_group": "Group1",
            },
            ("Model_A", "Wt"): {
                "display_label": "Wildtype",
                "model_group": "Group1",
            },
            ("Model_B", "Tg"): {
                "display_label": "Transgenic_B",
                "model_group": "Group2",
            },
        }

    def test_metadata_with_result_order(self) -> None:
        """Test creating metadata dict with result_order (individual mode)."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "genotype": ["Tg", "Wt"],
                "display_label": ["Transgenic", "Wildtype"],
                "model_group": ["Group1", "Group1"],
                "result_order": [2, 1],
            }
        )

        result = create_genotype_metadata_dict(df, include_result_order=True)

        assert result == {
            ("Model_A", "Tg"): {
                "display_label": "Transgenic",
                "model_group": "Group1",
                "result_order": 2,
                "effective_model_group": "Group1",
            },
            ("Model_A", "Wt"): {
                "display_label": "Wildtype",
                "model_group": "Group1",
                "result_order": 1,
                "effective_model_group": "Group1",
            },
        }

    def test_effective_model_group_with_empty_model_group(self) -> None:
        """Test that effective_model_group defaults to model when model_group is empty."""
        df = pd.DataFrame(
            {
                "model": ["Model_X"],
                "genotype": ["Tg"],
                "display_label": ["Transgenic"],
                "model_group": [""],
                "result_order": [1],
            }
        )

        result = create_genotype_metadata_dict(df, include_result_order=True)

        assert result[("Model_X", "Tg")]["effective_model_group"] == "Model_X"

    def test_empty_dataframe(self) -> None:
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(
            {
                "model": [],
                "genotype": [],
                "display_label": [],
                "model_group": [],
            }
        )

        result = create_genotype_metadata_dict(df, include_result_order=False)

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

    def test_converts_tissue_to_sentence_case(self) -> None:
        """Test that tissue names are converted to sentence case."""
        result1 = extract_common_metadata("ENSMUSG00000000001", "hippocampus", {})
        assert result1["tissue"] == "Hippocampus"

        result2 = extract_common_metadata("ENSMUSG00000000001", "CORTEX", {})
        assert result2["tissue"] == "Cortex"

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


class TestProcessDataFiles:
    """Tests for process_data_files function."""

    def test_basic_file_processing(self) -> None:
        """Test basic file processing with single data file."""
        # Setup datasets
        datasets = {
            "required_input": pd.DataFrame({"col1": [1, 2]}),
            "data_file_1": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                    "value": [10, 20],
                }
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        # Define callback that just returns the data
        def callback(file_name, df, idx, total):
            return [{"file": file_name, "rows": len(df)}]

        # Process files
        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert len(result) == 1
        assert result[0]["file"] == "data_file_1"
        assert result[0]["rows"] == 2

    def test_multiple_files_processing(self) -> None:
        """Test processing multiple data files."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "file1": pd.DataFrame(
                {"ensembl_gene_id": ["ENSMUSG00000000001"], "value": [1]}
            ),
            "file2": pd.DataFrame(
                {"ensembl_gene_id": ["ENSMUSG00000000002"], "value": [2]}
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            return [{"file": file_name, "index": idx, "total": total}]

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert len(result) == 2
        assert result[0]["index"] == 0
        assert result[0]["total"] == 2
        assert result[1]["index"] == 1
        assert result[1]["total"] == 2

    def test_filters_human_genes(self) -> None:
        """Test that human genes are filtered out."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001", "ENSG00000000001"],
                    "value": [1, 2],
                }
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            # Should only receive mouse genes
            assert all(df["ensembl_gene_id"].str.startswith("ENSMUSG"))
            return [{"rows": len(df)}]

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert result[0]["rows"] == 1

    def test_rounds_numeric_values(self) -> None:
        """Test that numeric values are rounded to 5 decimal places."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "value": [1.123456789],
                }
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            # Check rounding
            return [{"value": df.iloc[0]["value"]}]

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert result[0]["value"] == pytest.approx(1.12346, abs=1e-6)

    def test_converts_sex_to_sentence_case(self) -> None:
        """Test that sex values are converted to sentence case."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000002",
                        "ENSMUSG00000000003",
                    ],
                    "sex": ["M", "f", "male"],
                    "value": [1, 2, 3],
                }
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "sex", "value"]

        def callback(file_name, df, idx, total):
            # Check sex conversion
            return df["sex"].tolist()

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert result == ["Male", "Female", "Male"]

    def test_handles_missing_sex_column(self) -> None:
        """Test that files without sex column are processed normally."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "value": [1],
                }
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            return [{"has_sex": "sex" in df.columns}]

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        assert result[0]["has_sex"] is False

    def test_empty_file_raises_error(self) -> None:
        """Test that empty files raise ValueError."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame({"ensembl_gene_id": [], "value": []}),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            return []

        with pytest.raises(ValueError, match="empty"):
            process_data_files(
                datasets, required_input, data_file_required_columns, callback
            )

    def test_missing_columns_raises_error(self) -> None:
        """Test that missing required columns raise error."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "data_file": pd.DataFrame(
                {"ensembl_gene_id": ["ENSMUSG00000000001"]}  # Missing 'value' column
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            return []

        with pytest.raises(ValueError):
            process_data_files(
                datasets, required_input, data_file_required_columns, callback
            )

    def test_callback_results_are_accumulated(self) -> None:
        """Test that results from callback are accumulated across files."""
        datasets = {
            "required_input": pd.DataFrame({"col1": [1]}),
            "file1": pd.DataFrame(
                {"ensembl_gene_id": ["ENSMUSG00000000001"], "value": [1]}
            ),
            "file2": pd.DataFrame(
                {"ensembl_gene_id": ["ENSMUSG00000000002"], "value": [2]}
            ),
        }

        required_input = {"required_input": ["col1"]}
        data_file_required_columns = ["ensembl_gene_id", "value"]

        def callback(file_name, df, idx, total):
            # Return multiple entries per file
            return [{"entry": 1, "file": file_name}, {"entry": 2, "file": file_name}]

        result = process_data_files(
            datasets, required_input, data_file_required_columns, callback
        )

        # Should have 2 entries per file = 4 total
        assert len(result) == 4
        assert result[0]["entry"] == 1
        assert result[1]["entry"] == 2
