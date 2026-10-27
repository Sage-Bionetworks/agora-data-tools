"""
Test suite for the shared Model AD expression transform utilities.

Covers model_ad_expression_utils, which rna_de_individual, protein_de_individual,
and rna_de_aggregate call into. The module is not individual-only: nest_individual_records
is, but the file, tissue, genotype-label, and gene-metadata helpers are used by
aggregate as well.
"""

import math

import pandas as pd
import pytest
import logging
from typing import Any

from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    build_biodomain_lookup,
    build_model_to_model_group_lookup,
    build_model_type_lookup,
    create_age_entries_from_group,
    determine_result_order,
    filter_to_mouse_genes,
    label_genotypes,
    nest_individual_records,
    normalize_tissue,
    prepare_genotype_label_map,
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

    def test_all_null_column_remains_null(self) -> None:
        """pandas reads an entirely empty column as float. normalize_tissue should handle this case
        by leaving the column with all null values but the column should have a "string" type.
        """
        result = normalize_tissue(pd.Series([None, None]))
        assert result.isna().all()
        assert result.dtype == "object"


class TestPrepareGenotypeLabelMap:
    """Tests for the shared label map preparation."""

    def _label_map(self) -> pd.DataFrame:
        """Creates a basic label map data frame with a single model and two genotypes"""
        return pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": ["Group_A", "Group_A"],
                "display_label": ["Case", "Control"],
                "genotype": ["Tg", "WT"],
                "result_order": ["2", "1"],
            }
        )

    def test_casts_result_order_without_mutating_the_input(self) -> None:
        """Test that prepare_genotype_label_map casts result_order to integers without mutating the input."""
        label_map = self._label_map()
        prepared = prepare_genotype_label_map(label_map)

        assert prepared["result_order"].tolist() == [2, 1]
        assert label_map["result_order"].tolist() == ["2", "1"]

    def test_inconsistent_model_group_raises(self) -> None:
        """Test that an inconsistent model_group (where one model has more than one model_group) raises a ValueError."""
        label_map = self._label_map().assign(model_group=["Group_A", "Group_B"])

        with pytest.raises(ValueError, match="multiple model_group"):
            prepare_genotype_label_map(label_map)

    def test_duplicate_model_genotype_in_label_map_raises(self) -> None:
        """Test that a label map with duplicate model-genotype combinations raises a ValueError."""
        label_map = pd.concat([self._label_map(), self._label_map().iloc[[0]]])

        with pytest.raises(ValueError, match=r"duplicate \(model, genotype\)"):
            prepare_genotype_label_map(label_map)


class TestLabelGenotypes:
    """Tests for the shared genotype labeling merge."""

    def _label_map(self) -> pd.DataFrame:
        """Creates a basic label map data frame with a single model and two genotypes"""
        return pd.DataFrame(
            {
                "model": ["Model_A", "Model_A"],
                "model_group": ["Group_A", "Group_A"],
                "display_label": ["Case", "Control"],
                "genotype": ["Tg", "WT"],
                "result_order": [2, 1],
            }
        )

    def test_labels_matched_rows_and_drops_the_rest(self) -> None:
        """Test that label_genotypes correctly drops rows from data_file which have no matching genotype
        in the label map."""
        data_file = pd.DataFrame(
            {
                "model": ["Model_A"] * 3,
                "genotype": ["Tg", "WT", "Het"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result = label_genotypes(data_file, self._label_map())

        assert result["display_label"].tolist() == ["Case", "Control"]
        # display_label is not renamed here; determine_result_order still reads it.
        assert "genotype" in result.columns

    def test_no_matching_genotype_raises_with_context(self) -> None:
        """Test that a data frame with no matching genotypes in the label map raises a ValueError,
        and the ValueError message includes the context string."""
        label_map = self._label_map()
        data_file = pd.DataFrame({"model": ["Model_A"], "genotype": ["Het"]})

        with pytest.raises(
            ValueError, match="No rows remained for model_group 'Group_A'"
        ):
            label_genotypes(data_file, label_map, "model_group 'Group_A'")


class TestNestIndividualRecords:
    """Tests for the shared per-model_group nesting function.

    Expects a frame that has already been processed through label_genotypes, so it should already
    have valid, non-empty display labels.
    """

    _GROUP_COLUMNS = ["ensembl_gene_id", "tissue", "model_group", "age"]
    _UNITS = "test units"

    @staticmethod
    def _labeled_frame(**overrides: Any) -> pd.DataFrame:
        data = {
            "model": ["Model_A", "Model_A"],
            "genotype": ["Tg", "WT"],
            "display_label": ["Case", "Control"],
            "result_order": [2, 1],
            "model_group": ["Group_A", "Group_A"],
            "ensembl_gene_id": ["ENSMUSG1", "ENSMUSG1"],
            "tissue": ["Cortex", "Cortex"],
            "age": ["6 months", "6 months"],
            "sex": ["Male", "Female"],
            "individualid": ["i1", "i2"],
            "value": [1.0, 2.0],
        }
        data.update(overrides)
        return pd.DataFrame(data)

    def test_name_defaults_to_model_group_when_no_name_from_model(self) -> None:
        result = nest_individual_records(
            self._labeled_frame(),
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
        )

        assert result["name"].tolist() == ["Group_A"]

    def test_name_is_model_when_requested_for_single_model_in_group(self) -> None:
        result = nest_individual_records(
            self._labeled_frame(),
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
            name_from_model=True,
        )

        assert result["name"].tolist() == ["Model_A"]

    def test_name_is_model_group_when_requested(self) -> None:
        result = nest_individual_records(
            self._labeled_frame(),
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
            name_from_model=False,
        )

        assert result["name"].tolist() == ["Group_A"]

    def test_name_falls_back_to_model_group_when_multiple_models_in_group(self) -> None:
        df = self._labeled_frame(
            model=["Model_A", "Model_B"],
            genotype=["Tg", "Tg"],
            display_label=["Case_A", "Case_B"],
            result_order=[2, 3],
        )

        result = nest_individual_records(
            df,
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
            name_from_model=True,
        )

        assert result["name"].tolist() == ["Group_A"]

    def test_matched_control_is_the_lowest_result_order(self) -> None:
        result = nest_individual_records(
            self._labeled_frame(),
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
            name_from_model=True,
        )

        assert result["matched_control"].tolist() == ["Control"]
        assert result["result_order"].iloc[0] == ["Control", "Case"]

    def test_nests_the_four_per_animal_columns(self) -> None:
        result = nest_individual_records(
            self._labeled_frame(),
            group_columns=self._GROUP_COLUMNS,
            units=self._UNITS,
            name_from_model=True,
        )

        expected_data = [
            {"genotype": "Case", "sex": "Male", "individual_id": "i1", "value": 1.0},
            {
                "genotype": "Control",
                "sex": "Female",
                "individual_id": "i2",
                "value": 2.0,
            },
        ]

        expected_output = pd.DataFrame(
            {
                "ensembl_gene_id": "ENSMUSG1",
                "tissue": "Cortex",
                "model_group": "Group_A",
                "age": "6 months",
                "data": [expected_data],
                "units": self._UNITS,
                "name": "Model_A",
                "matched_control": "Control",
                "result_order": [["Control", "Case"]],
            }
        )

        pd.testing.assert_frame_equal(result, expected_output)


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


class TestBuildModelToModelGroupLookup:
    """Tests for build_model_to_model_group_lookup function."""

    def test_creates_correct_mapping(self) -> None:
        """Test that each model maps to its model_group."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_B"],
                "model_group": ["Group1", "Group2"],
            }
        )

        result = build_model_to_model_group_lookup(df)

        assert result == {"Model_A": "Group1", "Model_B": "Group2"}

    def test_repeated_model_rows_collapse(self) -> None:
        """Test that a model with one row per genotype still yields a single entry."""
        df = pd.DataFrame(
            {
                "model": ["Model_A", "Model_A", "Model_B"],
                "model_group": ["Group1", "Group1", "Group2"],
            }
        )

        result = build_model_to_model_group_lookup(df)

        assert result == {"Model_A": "Group1", "Model_B": "Group2"}

    def test_several_models_can_share_a_group(self) -> None:
        """Test the UCI shape, where two models are displayed as one group."""
        df = pd.DataFrame(
            {
                "model": ["Bin1-K358R", "Bin1-K358R.5xFAD"],
                "model_group": ["Bin1K358R", "Bin1K358R"],
            }
        )

        result = build_model_to_model_group_lookup(df)

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

    def test_drops_missing_gene_symbols(self) -> None:
        df = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                "gene_symbol": ["Gene1", None],
            }
        )

        result = create_gene_metadata_dict(df)

        # "ENSMUSG00000000002" should not be present
        assert result == {"ENSMUSG00000000001": "Gene1"}


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


class TestBuildModelTypeLookup:
    def test_one_row_per_model(self) -> None:
        label_map = pd.DataFrame(
            {
                "model": ["LOAD2", "LOAD2"],
                "model_type": ["Late Onset AD", "Late Onset AD"],
            }
        )

        assert build_model_type_lookup(label_map) == {"LOAD2": "Late Onset AD"}


class TestBuildBiodomainLookup:
    def test_groups_biodomains_and_drops_a_blank_ensembl_id(self) -> None:
        biodomains = pd.DataFrame(
            {
                "ensembl_id": ["ENSMUSG00000000001", "ENSMUSG00000000001", None],
                "biodomain": ["Apoptosis", "Synapse", "Lipid Metabolism"],
            }
        )

        assert build_biodomain_lookup(biodomains) == {
            "ENSMUSG00000000001": ["Apoptosis", "Synapse"]
        }

    def test_biodomain_lookup_drops_duplicates(self) -> None:
        """Test that duplicate biodomain entries for the same gene are dropped from the output list."""
        biodomains = pd.DataFrame(
            {
                "ensembl_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000001",
                ],
                "biodomain": ["Apoptosis", "Apoptosis", "Metabolic"],
            }
        )

        assert build_biodomain_lookup(biodomains) == {
            "ENSMUSG00000000001": ["Apoptosis", "Metabolic"]
        }


class TestCreateAgeEntriesFromGroup:
    """
    Unit tests for the create_age_entries_from_group helper function.

    This class contains focused unit tests for age entry creation logic,
    testing the function in isolation from the full transformation pipeline.

    Test Methods:
        - test_create_age_entries_single_row: Tests creating age entries from a single row.
        - test_create_age_entries_multiple_rows: Tests creating age entries from multiple rows.
        - test_create_age_entries_with_nan_padj: Tests handling of NaN adjusted p-values.
        - test_create_age_entries_with_negative_zero: Tests handling of negative zero log2foldchange.
        - test_create_age_entries_negative_padj_raises_error: Tests error handling for negative p-values.
    """

    def test_create_age_entries_single_row(self) -> None:
        """Test creating age entries from a single row DataFrame."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [0.01],
            }
        )

        result = create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        assert result == {"6 months": {"log2_fc": 1.5, "adj_p_val": 0.01}}

    def test_create_age_entries_are_in_numeric_age_order(self) -> None:
        """Ages come back ordered by their integer, not by input order or string order."""
        group = pd.DataFrame(
            {
                "age": ["12 months", "3 months", "6 months"],
                "log2foldchange": [0.8, 0.3, 0.5],
                "padj": [0.005, 0.02, 0.01],
            }
        )

        result = create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000001",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        assert list(result) == ["3 months", "6 months", "12 months"]

    def test_create_age_entries_multiple_rows(self) -> None:
        """Test creating age entries from multiple rows with different ages."""
        group = pd.DataFrame(
            {
                "age": ["3 months", "6 months", "12 months"],
                "log2foldchange": [0.5, 1.0, 1.5],
                "padj": [0.05, 0.01, 0.001],
            }
        )

        result = create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000002",
            model="TestModel",
            tissue="Hippocampus",
            sex="Female",
        )

        assert len(result) == 3
        assert "3 months" in result
        assert "6 months" in result
        assert "12 months" in result
        assert result["3 months"] == {"log2_fc": 0.5, "adj_p_val": 0.05}
        assert result["6 months"] == {"log2_fc": 1.0, "adj_p_val": 0.01}
        assert result["12 months"] == {"log2_fc": 1.5, "adj_p_val": 0.001}

    def test_create_age_entries_with_nan_padj(self) -> None:
        """Test that NaN adjusted p-values are converted to 1.0.

        This verifies that:
        - NA padj values are converted to 1.0
        - NA values don't trigger negative p-value validation check
        - The NA check happens before float conversion, preventing TypeError
          when pd.NA is passed to float()
        - log2_fc values are still processed correctly when padj is NA
        """
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [pd.NA],
            }
        )

        # Should not raise TypeError or ValueError, should handle NA gracefully
        result = create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000003",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # NA should be converted to 1.0 without errors
        assert result["6 months"]["adj_p_val"] == pytest.approx(1.0)
        assert result["6 months"]["log2_fc"] == pytest.approx(1.5)

    def test_create_age_entries_mixed_na_and_valid_padj(self) -> None:
        """Test that groups with both NA and valid padj values are handled correctly.

        This verifies that NA values are skipped in negative check but valid
        negative values still raise errors.
        """
        # Group with NA and valid positive values
        group_valid = pd.DataFrame(
            {
                "age": ["3 months", "6 months"],
                "log2foldchange": [0.5, 1.5],
                "padj": [pd.NA, 0.01],
            }
        )

        result = create_age_entries_from_group(
            group=group_valid,
            ensembl_gene_id="ENSMUSG00000000010",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # Both entries should be created without errors
        assert len(result) == 2
        assert result["3 months"]["adj_p_val"] == pytest.approx(1.0)
        assert result["6 months"]["adj_p_val"] == pytest.approx(0.01)

    def test_create_age_entries_with_negative_zero(self) -> None:
        """Test that negative zero log2foldchange is normalized to positive zero."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [-0.0],
                "padj": [0.01],
            }
        )

        result = create_age_entries_from_group(
            group=group,
            ensembl_gene_id="ENSMUSG00000000004",
            model="TestModel",
            tissue="Cortex",
            sex="Male",
        )

        # normalize_zero should convert -0.0 to 0.0
        assert math.isclose(result["6 months"]["log2_fc"], 0.0, abs_tol=1e-12)
        assert math.copysign(1.0, result["6 months"]["log2_fc"]) > 0

    def test_create_age_entries_negative_padj_raises_error(self) -> None:
        """Test that negative adjusted p-values raise ValueError with informative message."""
        group = pd.DataFrame(
            {
                "age": ["6 months"],
                "log2foldchange": [1.5],
                "padj": [-0.01],
            }
        )

        with pytest.raises(ValueError) as exc_info:
            create_age_entries_from_group(
                group=group,
                ensembl_gene_id="ENSMUSG00000000005",
                model="TestModel",
                tissue="Cortex",
                sex="Female",
            )

        error_message = str(exc_info.value)
        assert "Negative adjusted p-value found in data" in error_message
        assert "ENSMUSG00000000005" in error_message
        assert "TestModel" in error_message
        assert "Cortex" in error_message
        assert "Female" in error_message

    def test_create_age_entries_mixed_na_and_invalid_padj_raises(self) -> None:
        """Group with NA and negative value should still raise error for negative adjusted p-values."""
        group_negative = pd.DataFrame(
            {
                "age": ["3 months", "6 months"],
                "log2foldchange": [0.5, 1.5],
                "padj": [pd.NA, -0.01],
            }
        )

        with pytest.raises(ValueError) as exc_info:
            create_age_entries_from_group(
                group=group_negative,
                ensembl_gene_id="ENSMUSG00000000011",
                model="TestModel",
                tissue="Cortex",
                sex="Male",
            )

        # Should still catch negative p-value even when NA is present
        error_message = str(exc_info.value)
        assert "Negative adjusted p-value found in data" in error_message
        assert "ENSMUSG00000000011" in error_message
