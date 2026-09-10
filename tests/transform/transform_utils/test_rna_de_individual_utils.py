"""
Test suite for the shared Model AD individual-expression transform utilities.

Covers rna_de_individual_utils, which both rna_de_individual and protein_de_individual
call into.
"""

import pandas as pd
import pytest
import logging
from typing import Any

from agoradatatools.etl.transform.transform_utils.rna_de_individual_utils import (
    build_model_to_model_group,
    determine_result_order,
    filter_to_mouse_genes,
    label_genotypes,
    normalize_tissue,
    prepare_genotype_label_map,
    validate_model_group_consistency,
    create_gene_metadata_dict,
    log_file_processing_info,
    validate_data_file_not_empty,
    preprocess_data_file,
)
from agoradatatools.etl.utils import MatchesRegexRule, NotEmptyRule


class TestNormalizeTissue:
    """Tests for the shared tissue alias mapping."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("right cerebral hemisphere", "Hemibrain"),
            ("Right Cerebral Hemisphere", "Hemibrain"),
            (" right cerebral hemisphere ", "Hemibrain"),
            ("Cortex", "Cortex"),
        ],
    )
    def test_normalize_tissue(self, value: Any, expected: str) -> None:
        assert normalize_tissue(pd.Series([value])).iloc[0] == expected

    def test_all_null_column_is_left_null(self) -> None:
        """pandas reads an entirely empty column as float, which has no str accessor."""
        assert normalize_tissue(pd.Series([None, None])).isna().all()


class TestPrepareGenotypeLabelMap:
    """Tests for the shared label map preparation."""

    _LABEL_MAP = pd.DataFrame(
        {
            "model": ["Model_A", "Model_A"],
            "model_group": ["Group_A", "Group_A"],
            "display_label": ["Case", "Control"],
            "genotype": ["Tg", "WT"],
            "result_order": ["2", "1"],
        }
    )

    def test_casts_result_order_without_mutating_the_input(self) -> None:
        prepared = prepare_genotype_label_map(self._LABEL_MAP)

        assert prepared["result_order"].tolist() == [2, 1]
        assert self._LABEL_MAP["result_order"].tolist() == ["2", "1"]

    def test_inconsistent_model_group_raises(self) -> None:
        label_map = self._LABEL_MAP.assign(model_group=["Group_A", "Group_B"])

        with pytest.raises(ValueError, match="consistent model_group"):
            prepare_genotype_label_map(label_map)


class TestLabelGenotypes:
    """Tests for the shared genotype labeling merge."""

    _LABEL_MAP = pd.DataFrame(
        {
            "model": ["Model_A", "Model_A"],
            "model_group": ["Group_A", "Group_A"],
            "display_label": ["Case", "Control"],
            "genotype": ["Tg", "WT"],
            "result_order": [2, 1],
        }
    )

    def test_labels_matched_rows_and_drops_the_rest(self) -> None:
        data_file = pd.DataFrame(
            {
                "model": ["Model_A"] * 3,
                "genotype": ["Tg", "WT", "Het"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result = label_genotypes(data_file, self._LABEL_MAP)

        assert result["display_label"].tolist() == ["Case", "Control"]
        # display_label is not renamed here; determine_result_order still reads it.
        assert "genotype" in result.columns

    def test_no_matching_genotype_raises_with_context(self) -> None:
        data_file = pd.DataFrame({"model": ["Model_A"], "genotype": ["Het"]})

        with pytest.raises(
            ValueError, match="No rows remained for model_group 'Group_A'"
        ):
            label_genotypes(data_file, self._LABEL_MAP, "model_group 'Group_A'")

    def test_duplicate_model_genotype_in_label_map_raises(self) -> None:
        """A duplicate row would fan every measurement of that genotype out into two."""
        label_map = pd.concat([self._LABEL_MAP, self._LABEL_MAP.iloc[[0]]])
        data_file = pd.DataFrame({"model": ["Model_A"], "genotype": ["Tg"]})

        with pytest.raises(ValueError, match="not a many-to-one merge"):
            label_genotypes(data_file, label_map)


class TestDetermineResultOrder:
    """Tests for determine_result_order function.

    The function accepts a data_file already merged with the label map and filtered to a
    single model_group. Empty display_label values are rejected upstream by
    check_column_rules and never reach it.
    """

    def test_orders_labels_by_result_order(self) -> None:
        """Test that display labels are ordered by their result_order value."""
        data_file = pd.DataFrame(
            {
                "display_label": ["Model_B", "Control_B", "Model_C"],
                "result_order": [20, 10, 30],
            }
        )

        assert determine_result_order(data_file) == ["Control_B", "Model_B", "Model_C"]

    def test_empty_data_file(self) -> None:
        """Test that an empty data_file yields an empty list."""
        data_file = pd.DataFrame(columns=["display_label", "result_order"])

        assert determine_result_order(data_file) == []


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
    def _make_df(tissues: list[str]) -> pd.DataFrame:
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
    def _preprocess(cls, tissues: list[str]) -> pd.Series:
        df = cls._make_df(tissues)
        result = preprocess_data_file("test.csv", df, 0, 1, cls._REQUIRED_COLUMNS, {})
        return result["tissue"].reset_index(drop=True)

    def test_maps_right_cerebral_hemisphere(self) -> None:
        """'Right Cerebral Hemisphere' is mapped to 'Hemibrain'."""
        assert self._preprocess(["Right Cerebral Hemisphere"]).iloc[0] == "Hemibrain"

    def test_mixed_tissue_names(self) -> None:
        """Mixed tissue names including the special mapping are all handled correctly."""
        result = self._preprocess(
            ["Right Cerebral Hemisphere", "Cerebellum", "Cerebral Cortex"]
        )
        expected = pd.Series(["Hemibrain", "Cerebellum", "Cerebral Cortex"])
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
    def _make_df(
        expression_values: list[Any], individualid_values: list[Any]
    ) -> pd.DataFrame:
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
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS, {})
        assert result["expression"].dtype == float

    def test_expression_string_is_rounded_to_5_decimals(self) -> None:
        """expression values from strings are correctly rounded after casting."""
        df = self._make_df(
            expression_values=["1.123456789"],
            individualid_values=["ID0"],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS, {})
        assert result["expression"].iloc[0] == pytest.approx(1.12346)

    def test_individualid_numeric_is_cast_to_str(self) -> None:
        """individualid values arriving as integers are cast to str."""
        df = self._make_df(
            expression_values=[1.0, 2.0],
            individualid_values=[101, 202],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS, {})
        assert result["individualid"].dtype == object
        assert result["individualid"].iloc[0] == "101"
        assert result["individualid"].iloc[1] == "202"

    def test_expression_already_float_unchanged(self) -> None:
        """expression values that are already float pass through without error."""
        df = self._make_df(
            expression_values=[1.5, 2.5],
            individualid_values=["ID0", "ID1"],
        )
        result = preprocess_data_file("test.csv", df, 0, 1, self._REQUIRED_COLUMNS, {})
        assert result["expression"].dtype == float


class TestPreprocessDataRemapsSexLabels:
    """Test for the sex label mapping applied inside preprocess_data_file."""

    def test_maps_sex_values(self) -> None:
        """Plural sex values are mapped to singular display labels"""
        sex_values = ["Males", "Females"]
        n = len(sex_values)
        df = pd.DataFrame(
            {
                "ensembl_gene_id": [f"ENSMUSG{i:011d}" for i in range(n)],
                "expression": [1.0] * n,
                "model": ["Model_A"] * n,
                "genotype": ["Tg"] * n,
                "age": ["4 months"] * n,
                "sex": sex_values,
                "tissue": ["Hemibrain"] * n,
                "individualid": [f"ID{i}" for i in range(n)],
            }
        )
        result = preprocess_data_file("test.csv", df, 0, 1, [], {})
        assert result["sex"].reset_index(drop=True).tolist() == ["Male", "Female"]


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

        Consistent "" values are considered valid because all rows agree on the same value.
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


class TestBuildModelToModelGroup:
    """Tests for build_model_to_model_group function."""

    def test_creates_correct_mapping(self) -> None:
        """Test that each model maps to its model_group."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_B"],
                "model_group": ["Group1", "Group2"],
            }
        )

        result = build_model_to_model_group(df)

        assert result == {"Model_A": "Group1", "Model_B": "Group2"}

    def test_repeated_model_rows_collapse(self) -> None:
        """Test that a model with one row per genotype still yields a single entry."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B"],
                "model_group": ["Group1", "Group1", "Group2"],
            }
        )

        result = build_model_to_model_group(df)

        assert result == {"Model_A": "Group1", "Model_B": "Group2"}

    def test_several_models_can_share_a_group(self) -> None:
        """Test the UCI shape, where two models are displayed as one group."""
        df = pd.DataFrame(
            {
                "model": ["Bin1-K358R", "Bin1-K358R.5xFAD"],
                "model_group": ["Bin1K358R", "Bin1K358R"],
            }
        )

        result = build_model_to_model_group(df)

        assert result == {
            "Bin1-K358R": "Bin1K358R",
            "Bin1-K358R.5xFAD": "Bin1K358R",
        }


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

    def test_logs_information(self, caplog: pytest.LogCaptureFixture) -> None:
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


class TestPreprocessDataFileColumnRules:
    """Tests for check_column_rules integration inside preprocess_data_file.

    Verifies that data_file_column_rules are applied correctly and that violations
    raise ValueError via check_column_rules before any transformations occur.

    Test Methods:
        - test_bad_age_format_raises_value_error: age values not matching
          '\\d+ months' fail the matches_regex rule.
        - test_wrong_unit_age_raises_value_error: ages with digits but the wrong
          unit (e.g. '1 year') fail the matches_regex rule.
        - test_empty_model_raises_value_error: an empty model value fails the
          not_empty rule.
        - test_valid_data_passes_column_rules: well-formed data passes without error.
    """

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

    _DEFAULT_COLUMN_RULES = {
        "model": [NotEmptyRule()],
        "age": [MatchesRegexRule(value=r"\d+ months$")],
    }

    @staticmethod
    def _make_df(**overrides: Any) -> pd.DataFrame:
        """Return a minimal valid data file DataFrame with optional column overrides."""
        data = {
            "ensembl_gene_id": ["ENSMUSG00000000001"],
            "expression": [5.0],
            "model": ["Model_A"],
            "genotype": ["Tg"],
            "age": ["6 months"],
            "sex": ["Male"],
            "tissue": ["Cortex"],
            "individualid": ["Ind001"],
        }
        data.update(overrides)
        return pd.DataFrame(data)

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        return preprocess_data_file(
            "test.csv", df, 0, 1, self._REQUIRED_COLUMNS, self._DEFAULT_COLUMN_RULES
        )

    def test_bad_age_format_raises_value_error(self) -> None:
        """age values not matching '\\d+ months' fail the matches_regex rule."""
        df = self._make_df(age=["neonatal"])

        with pytest.raises(ValueError, match="matches_regex"):
            self._preprocess(df)

    def test_wrong_unit_age_raises_value_error(self) -> None:
        """An age like '1 year' contains digits but does not end in 'months'."""
        df = self._make_df(age=["1 year"])

        with pytest.raises(ValueError, match="matches_regex"):
            self._preprocess(df)

    def test_empty_model_raises_value_error(self) -> None:
        """An empty model value fails the not_empty rule."""
        df = self._make_df(model=[""])

        with pytest.raises(ValueError, match="not_empty"):
            self._preprocess(df)

    def test_valid_data_passes_column_rules(self) -> None:
        """Well-formed data passes column rules without raising an error."""
        df = self._make_df()

        # Should not raise
        self._preprocess(df)
