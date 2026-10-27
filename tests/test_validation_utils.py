from collections.abc import Sequence
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from agoradatatools.etl import validation_utils

RuleClassWithValueArg = (
    type[validation_utils.MatchesRegexRule]
    | type[validation_utils.ContainsSubstringRule]
    | type[validation_utils.OneOfRule]
)


class TestCheckRequiredDatasetsAndColumns:
    required_input = {
        "foo": ["a", "b"],
        "bar": ["x", "y"],
    }

    def test_check_required_datasets_and_columns_all_present(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1], "b": [2]}),
            "bar": pd.DataFrame({"x": [3], "y": [4]}),
        }
        # Should not raise
        validation_utils.check_required_datasets_and_columns(
            datasets, self.required_input
        )

    def test_check_required_datasets_and_columns_missing_dataset(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1], "b": [2]}),
        }
        with pytest.raises(ValueError, match="Missing required datasets: bar"):
            validation_utils.check_required_datasets_and_columns(
                datasets, self.required_input
            )

    def test_check_required_datasets_and_columns_missing_column(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1]}),
            "bar": pd.DataFrame({"x": [3], "y": [4]}),
        }
        with pytest.raises(
            ValueError, match="Missing required columns in foo dataset: b"
        ):
            validation_utils.check_required_datasets_and_columns(
                datasets, self.required_input
            )


class TestColumnRuleContract:
    """Verify that every concrete ColumnRule subclass honours the base class contract."""

    @pytest.mark.parametrize(
        "rule",
        [
            validation_utils.NotEmptyRule(),
            validation_utils.MatchesRegexRule(value="^ENSMUSG"),
            validation_utils.ContainsSubstringRule(value="world"),
            validation_utils.OneOfRule(value={"a"}),
        ],
    )
    def test_count_violations_returns_int(
        self, rule: validation_utils.ColumnRule
    ) -> None:
        assert isinstance(rule.count_violations(pd.Series(["a", None])), int)

    @pytest.mark.parametrize(
        "rule",
        [
            validation_utils.NotEmptyRule(),
            validation_utils.MatchesRegexRule(value="^ENSMUSG"),
            validation_utils.ContainsSubstringRule(value="world"),
            validation_utils.OneOfRule(value={"a"}),
        ],
    )
    def test_count_violations_is_non_negative(
        self, rule: validation_utils.ColumnRule
    ) -> None:
        assert rule.count_violations(pd.Series(["a", None])) >= 0

    def test_column_rule_cannot_be_instantiated_directly(self) -> None:
        with pytest.raises(TypeError):
            validation_utils.ColumnRule()


class TestNotEmptyRule:
    """Unit tests for NotEmptyRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    def test_no_violations_for_all_valid(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", "b", "c"])
            )
            == 0
        )

    def test_counts_none_as_violation(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", None, "c"])
            )
            == 1
        )

    def test_counts_nan_as_violation(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", np.nan, "c"])
            )
            == 1
        )

    def test_counts_empty_string_as_violation(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", "", "c"])
            )
            == 1
        )

    def test_counts_whitespace_only_as_violation(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", "   ", "c"])
            )
            == 1
        )

    def test_counts_multiple_violations(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series(["a", None, "", "   ", "b"])
            )
            == 3
        )

    def test_all_violations(self) -> None:
        assert (
            validation_utils.NotEmptyRule().count_violations(
                self._series([None, "", "   "])
            )
            == 3
        )

    def test_empty_series(self) -> None:
        assert validation_utils.NotEmptyRule().count_violations(self._series([])) == 0

    def test_value_detail_is_empty_string(self) -> None:
        assert validation_utils.NotEmptyRule().value_detail == ""


class TestMatchesRegexRule:
    """Unit tests for MatchesRegexRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, "", 123, np.nan])
    def test_raises_when_value_is_invalid(
        self, bad_value: int | str | float | None
    ) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            validation_utils.MatchesRegexRule(value=bad_value)

    def test_raises_when_value_is_invalid_regex(self) -> None:
        with pytest.raises(ValueError, match="valid regex"):
            validation_utils.MatchesRegexRule(value="[invalid")

    def test_no_violations_when_all_match(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", "ENSMUSG002"])) == 0

    def test_counts_non_matching_value(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", "ENSG002"])) == 1

    def test_counts_all_non_matching(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSG001", "ENSG002"])) == 2

    def test_skips_none(self) -> None:
        # Nulls are skipped so the rule only validates the format of present values.
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", None])) == 0

    def test_skips_nan(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", np.nan])) == 0

    def test_counts_empty_string_as_violation(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", ""])) == 1

    def test_partial_match_is_violation(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["prefix_ENSMUSG001"])) == 1

    def test_empty_series(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_pattern(self) -> None:
        rule = validation_utils.MatchesRegexRule(value="^ENSMUSG")
        assert "^ENSMUSG" in rule.value_detail


class TestContainsSubstringRule:
    """Unit tests for ContainsSubstringRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, np.nan, ""])
    def test_raises_when_value_is_invalid(self, bad_value: str | float | None) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            validation_utils.ContainsSubstringRule(value=bad_value)

    def test_no_violations_when_all_contain_substring(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", "world cup"])) == 0

    def test_counts_missing_substring(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", "goodbye"])) == 1

    def test_counts_all_missing(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["foo", "bar"])) == 2

    def test_counts_none_as_violation(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", None])) == 1

    def test_counts_nan_as_violation(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", np.nan])) == 1

    def test_counts_empty_string_as_violation(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", ""])) == 1

    def test_counts_non_string_data_as_violations(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert (
            rule.count_violations(self._series(["hello world", "goodbye", 2, 5.555555]))
            == 3
        )

    def test_value_is_treated_as_literal_not_regex(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="-")
        assert rule.count_violations(self._series(["hello-world", "goodbye"])) == 1

    def test_empty_series(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_substring(self) -> None:
        rule = validation_utils.ContainsSubstringRule(value="world")
        assert "world" in rule.value_detail


class TestOneOfRule:
    """Unit tests for OneOfRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, set(), [], {}])
    def test_raises_when_value_is_invalid(self, bad_value: object) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            validation_utils.OneOfRule(value=bad_value)

    def test_no_violations_when_all_in_set(self) -> None:
        rule = validation_utils.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", "female", "male"])) == 0

    def test_counts_value_not_in_set(self) -> None:
        rule = validation_utils.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", "unknown"])) == 1

    def test_counts_all_invalid(self) -> None:
        rule = validation_utils.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["unknown", "other"])) == 2

    def test_counts_none_as_violation(self) -> None:
        rule = validation_utils.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", None])) == 1

    def test_works_with_list_as_allowed_values(self) -> None:
        rule = validation_utils.OneOfRule(value=["male", "female"])
        assert rule.count_violations(self._series(["male", "unknown"])) == 1

    def test_empty_series(self) -> None:
        rule = validation_utils.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_allowed_values(self) -> None:
        rule = validation_utils.OneOfRule(value={"male"})
        assert "male" in rule.value_detail

    def test_no_violations_numeric_allowed_values(self) -> None:
        rule = validation_utils.OneOfRule(value=[1, 2, 3])
        assert rule.count_violations(self._series([1, 1, 2])) == 0

    def test_no_violations_bool_allowed_values(self) -> None:
        rule = validation_utils.OneOfRule(value=[True, False])
        assert rule.count_violations(self._series([True, False, False])) == 0

    def test_no_violations_sentinel_allowed_values(self) -> None:
        # Series.isin matches np.nan to np.nan in the allowed collection (pandas semantics).
        rule = validation_utils.OneOfRule(value=["", None, [], np.nan])
        s = self._series(["", "", None, [], np.nan])
        assert rule.count_violations(s) == 0

    def test_no_violations_mixed_int_and_string_allowed(self) -> None:
        rule = validation_utils.OneOfRule(value=[2, "2"])
        assert rule.count_violations(self._series([2, 2, "2"])) == 0

    def test_violations_when_numeric_allowed_but_string_in_series(self) -> None:
        rule = validation_utils.OneOfRule(value=[1, 2])
        assert rule.count_violations(self._series([1, 1, "2"])) == 1

    def test_violations_all_strings_when_numeric_allowed(self) -> None:
        rule = validation_utils.OneOfRule(value=[1, 2])
        assert rule.count_violations(self._series(["1", "1", "2"])) == 3

    def test_bool_and_int_equivalence_with_bool_allowed(self) -> None:
        rule = validation_utils.OneOfRule(value=[True, False])
        s = self._series([True, 1, False, 0])
        assert rule.count_violations(s) == 0

    def test_bool_and_int_equivalence_with_int_allowed(self) -> None:
        rule = validation_utils.OneOfRule(value=[0, 1])
        s = self._series([True, 1, False, 0])
        assert rule.count_violations(s) == 0


class TestNumericRule:
    """Unit tests for NumericRule.count_violations()."""

    def test_no_violations_for_all_numeric(self) -> None:
        assert (
            validation_utils.NumericRule().count_violations(pd.Series([1, 2.5, 3])) == 0
        )

    def test_no_violations_for_numeric_strings(self) -> None:
        assert (
            validation_utils.NumericRule().count_violations(pd.Series(["6", "9.9"]))
            == 0
        )

    def test_counts_non_numeric_string(self) -> None:
        assert (
            validation_utils.NumericRule().count_violations(pd.Series([1, "abc", 3]))
            == 1
        )

    def test_counts_all_non_numeric(self) -> None:
        assert (
            validation_utils.NumericRule().count_violations(pd.Series(["a", "b"])) == 2
        )

    def test_skips_none(self) -> None:
        # Nulls are skipped so the rule only validates the type of present values.
        assert (
            validation_utils.NumericRule().count_violations(pd.Series([1, None, 3]))
            == 0
        )

    def test_skips_nan(self) -> None:
        assert (
            validation_utils.NumericRule().count_violations(pd.Series([1, np.nan, 3]))
            == 0
        )

    def test_empty_series(self) -> None:
        assert validation_utils.NumericRule().count_violations(pd.Series([])) == 0

    def test_value_detail_is_empty_string(self) -> None:
        assert validation_utils.NumericRule().value_detail == ""


class TestNonNegativeRule:
    """Unit tests for NonNegativeRule.count_violations()."""

    def test_no_violations_for_all_positive(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(pd.Series([1, 2.5, 3]))
            == 0
        )

    def test_no_violations_for_zero(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(pd.Series([0, 0.0]))
            == 0
        )

    def test_counts_negative(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(pd.Series([1, -2.5, 3]))
            == 1
        )

    def test_counts_all_negative(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(pd.Series([-1, -2]))
            == 2
        )

    def test_counts_negative_numeric_string(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(
                pd.Series(["6", "-9.9"])
            )
            == 1
        )

    def test_skips_non_numeric(self) -> None:
        # Unparseable values are NumericRule's job, so a single bad cell isn't
        # counted as two violations.
        assert (
            validation_utils.NonNegativeRule().count_violations(
                pd.Series([1, "abc", -3])
            )
            == 1
        )

    def test_skips_none(self) -> None:
        # Nulls are skipped so the rule only validates the sign of present values.
        assert (
            validation_utils.NonNegativeRule().count_violations(pd.Series([1, None, 3]))
            == 0
        )

    def test_skips_nan(self) -> None:
        assert (
            validation_utils.NonNegativeRule().count_violations(
                pd.Series([1, np.nan, 3])
            )
            == 0
        )

    def test_empty_series(self) -> None:
        assert validation_utils.NonNegativeRule().count_violations(pd.Series([])) == 0

    def test_value_detail_is_empty_string(self) -> None:
        assert validation_utils.NonNegativeRule().value_detail == ""


class TestUniqueRule:
    """Unit tests for UniqueRule.count_violations()."""

    def test_no_violations_for_distinct_values(self) -> None:
        assert (
            validation_utils.UniqueRule().count_violations(pd.Series(["a", "b", "c"]))
            == 0
        )

    def test_counts_every_row_of_a_duplicate_group(self) -> None:
        assert (
            validation_utils.UniqueRule().count_violations(pd.Series(["a", "a", "b"]))
            == 2
        )

    def test_counts_each_duplicate_group_separately(self) -> None:
        s = pd.Series(["a", "a", "b", "b", "b", "c"])
        assert validation_utils.UniqueRule().count_violations(s) == 5

    def test_skips_repeated_nulls(self) -> None:
        # pandas treats null as equal to null, so skipping nulls is what keeps a second
        # blank row from being reported as a duplicate of the first. Both nulls must be nan
        # rather than one None and one nan: in an object column those are distinct objects,
        # so a mixed pair is not a duplicate even without the null filter and would pass
        # against a broken implementation. read_csv yields the matching-nan shape.
        s = pd.Series(["a", np.nan, np.nan, "b"])
        assert validation_utils.UniqueRule().count_violations(s) == 0

    def test_counts_duplicates_alongside_nulls(self) -> None:
        # Only the real duplicate counts; the repeated nulls do not add to it.
        s = pd.Series(["a", "a", np.nan, np.nan])
        assert validation_utils.UniqueRule().count_violations(s) == 2

    def test_does_not_skip_repeated_empty_strings(self) -> None:
        # Blanks are NotEmptyRule's job; this rule only exempts true nulls.
        assert (
            validation_utils.UniqueRule().count_violations(pd.Series(["", "", "a"]))
            == 2
        )

    def test_counts_duplicate_numeric_values(self) -> None:
        assert validation_utils.UniqueRule().count_violations(pd.Series([1, 2, 1])) == 2

    def test_empty_series(self) -> None:
        assert validation_utils.UniqueRule().count_violations(pd.Series([])) == 0

    def test_value_detail_is_empty_string(self) -> None:
        assert validation_utils.UniqueRule().value_detail == ""


class TestCheckColumnRules:
    """Tests for check_column_rules() and its supporting _check_single_rule() helper."""

    def _make_datasets(self, col_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        return {"ds": pd.DataFrame(col_data)}

    @pytest.mark.parametrize(
        "col_data, rule",
        [
            (["a", "b", "c"], validation_utils.NotEmptyRule()),
            (
                ["ENSMUSG001", "ENSMUSG002"],
                validation_utils.MatchesRegexRule(value="^ENSMUSG"),
            ),
            (
                ["hello world", "world cup"],
                validation_utils.ContainsSubstringRule(value="world"),
            ),
            (
                ["male", "female", "male"],
                validation_utils.OneOfRule(value={"male", "female"}),
            ),
        ],
    )
    def test_rule_passes_for_all_valid_values(
        self, col_data: list[Any], rule: validation_utils.ColumnRule
    ) -> None:
        datasets = self._make_datasets({"col": col_data})
        validation_utils.check_column_rules(datasets, {"ds": {"col": [rule]}})

    @pytest.mark.parametrize("bad_value", [None, np.nan, "", "   "])
    def test_not_empty_raises_on_invalid_value(
        self, bad_value: str | float | None
    ) -> None:
        datasets = self._make_datasets({"col": ["a", bad_value, "c"]})
        with pytest.raises(ValueError, match="col.*not_empty"):
            validation_utils.check_column_rules(
                datasets, {"ds": {"col": [validation_utils.NotEmptyRule()]}}
            )

    @pytest.mark.parametrize(
        "col_data, rule, match_pattern",
        [
            (
                ["ENSMUSG001", "ENSG002", "ENSG003"],
                validation_utils.MatchesRegexRule(value="^ENSMUSG"),
                r"2 row\(s\).*matches_regex.*\^ENSMUSG",
            ),
            (
                ["hello world", "goodbye"],
                validation_utils.ContainsSubstringRule(value="world"),
                r"1 row\(s\).*contains_substring.*world",
            ),
            (
                ["hello world", "goodbye", "adieu", "farewell"],
                validation_utils.ContainsSubstringRule(value="world"),
                r"3 row\(s\).*contains_substring.*world",
            ),
            (
                ["male", "female", "unknown", "other"],
                validation_utils.OneOfRule(value={"male", "female"}),
                r"2 row\(s\).*one_of",
            ),
            (
                ["valid", None, ""],
                validation_utils.NotEmptyRule(),
                r"2 row\(s\).*not_empty",
            ),
            (
                ["valid", None, "", "   ", "also valid"],
                validation_utils.NotEmptyRule(),
                r"3 row\(s\).*not_empty",
            ),
        ],
    )
    def test_rule_raises_with_correct_count(
        self, col_data: list[Any], rule: validation_utils.ColumnRule, match_pattern: str
    ) -> None:
        datasets = self._make_datasets({"col": col_data})
        with pytest.raises(ValueError, match=match_pattern):
            validation_utils.check_column_rules(datasets, {"ds": {"col": [rule]}})

    @pytest.mark.parametrize(
        "good_value, rule",
        [
            ("hello world", validation_utils.ContainsSubstringRule(value="world")),
        ],
    )
    def test_rule_treats_null_as_violation(
        self, good_value: str, rule: validation_utils.ColumnRule
    ) -> None:
        datasets = self._make_datasets({"col": [good_value, None]})
        with pytest.raises(ValueError, match=rule.rule):
            validation_utils.check_column_rules(datasets, {"ds": {"col": [rule]}})

    def test_matches_regex_rule_ignores_null(self) -> None:
        # MatchesRegexRule only validates present values; nulls are not violations.
        datasets = self._make_datasets({"col": ["ENSMUSG001", None]})
        validation_utils.check_column_rules(
            datasets,
            {"ds": {"col": [validation_utils.MatchesRegexRule(value="^ENSMUSG")]}},
        )

    def test_all_violations_collected_in_single_error(self) -> None:
        datasets = {
            "ds1": pd.DataFrame({"col_a": ["a", None]}),
            "ds2": pd.DataFrame({"col_b": ["ENSG001", "ENSG002"]}),
        }
        column_rules = {
            "ds1": {"col_a": [validation_utils.NotEmptyRule()]},
            "ds2": {"col_b": [validation_utils.MatchesRegexRule(value="^ENSMUSG")]},
        }
        with pytest.raises(ValueError) as exc_info:
            validation_utils.check_column_rules(datasets, column_rules)
        message = str(exc_info.value)
        assert "ds1" in message
        assert "ds2" in message

    def test_missing_dataset_in_rules_is_skipped(self) -> None:
        datasets = {"ds": pd.DataFrame({"col": ["a"]})}
        validation_utils.check_column_rules(
            datasets,
            {"nonexistent_ds": {"col": [validation_utils.NotEmptyRule()]}},
        )

    def test_missing_column_in_rules_reports_violation(self) -> None:
        datasets = {"ds": pd.DataFrame({"other_col": ["a"]})}
        with pytest.raises(ValueError, match="does not exist"):
            validation_utils.check_column_rules(
                datasets,
                {"ds": {"missing_col": [validation_utils.NotEmptyRule()]}},
            )

    @pytest.mark.parametrize(
        "rule_class",
        [
            validation_utils.MatchesRegexRule,
            validation_utils.ContainsSubstringRule,
            validation_utils.OneOfRule,
        ],
    )
    def test_value_required_rule_raises_when_value_is_none(
        self, rule_class: RuleClassWithValueArg
    ) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            rule_class(value=None)


class TestValidateOneToOneMapping:
    """Tests for validate_one_to_one_mapping()."""

    def test_passes_one_to_one_mapping(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL1", "CHEMBL2"],
            }
        )
        validation_utils.validate_one_to_one_mapping(df, "common_name", "chembl_id")

    def test_raises_when_left_maps_to_multiple_right(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugA"],
                "chembl_id": ["CHEMBL1", "CHEMBL2"],
            }
        )
        with pytest.raises(ValueError, match="common_name"):
            validation_utils.validate_one_to_one_mapping(df, "common_name", "chembl_id")

    def test_default_does_not_check_reverse_direction(self) -> None:
        # Two names share an ID; without bidirectional this passes because each
        # name still maps to a single ID.
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL1"],
            }
        )
        validation_utils.validate_one_to_one_mapping(df, "common_name", "chembl_id")

    def test_bidirectional_catches_reverse_violation(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugB"],
                "chembl_id": ["CHEMBL1", "CHEMBL1"],
            }
        )
        with pytest.raises(ValueError, match="chembl_id"):
            validation_utils.validate_one_to_one_mapping(
                df, "common_name", "chembl_id", bidirectional=True
            )

    def test_ignores_missing_keys(self) -> None:
        df = pd.DataFrame(
            {
                "common_name": [None, "DrugA"],
                "chembl_id": [None, "CHEMBL1"],
            }
        )
        validation_utils.validate_one_to_one_mapping(
            df, "common_name", "chembl_id", bidirectional=True
        )

    def test_raises_when_value_is_null_for_one_of_duplicate_keys(self) -> None:
        # NA keys are ignored, but a null value counts as a distinct value: DrugA
        # maps to both None and CHEMBL1, which is not a one-to-one mapping.
        df = pd.DataFrame(
            {
                "common_name": ["DrugA", "DrugA"],
                "chembl_id": [None, "CHEMBL1"],
            }
        )
        with pytest.raises(ValueError, match="common_name.*multiple chembl_id values"):
            validation_utils.validate_one_to_one_mapping(df, "common_name", "chembl_id")


class TestRaiseOnDuplicateKeys:
    """Tests for raise_on_duplicate_keys()."""

    def test_raises_when_keys_repeat(self) -> None:
        """Repeated key tuples are reported, and rows that differ only outside the keys still conflict."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"], "c": [0, 1, 2]})

        with pytest.raises(
            ValueError, match=r"shared key \(first 10\): \[\(1, 'x'\)\]"
        ):
            validation_utils.raise_on_duplicate_keys(df, ["a", "b"], "shared key")

    def test_does_not_raise_when_keys_are_unique(self) -> None:
        """Distinct keys are not an error, even when another column repeats."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "x"]})

        validation_utils.raise_on_duplicate_keys(df, ["a", "b"], "shared key")

    def test_raises_when_keys_are_empty(self) -> None:
        """Empty keys are not allowed, regardless of the data in the DataFrame."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        with pytest.raises(ValueError, match="not enough values to unpack"):
            validation_utils.raise_on_duplicate_keys(df, [], "shared key")

    def test_does_not_raise_when_dataframe_is_empty(self) -> None:
        """An empty DataFrame shouldn't raise an error as long as it has the required columns."""
        df = pd.DataFrame(columns=["a", "b"])

        validation_utils.raise_on_duplicate_keys(df, ["a", "b"], "shared key")

    def test_raises_when_dataframe_is_missing_columns_in_keys(self) -> None:
        """A DataFrame missing columns specified in keys should raise an error."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "x"]})

        with pytest.raises(KeyError, match="\\['c'\\]"):
            validation_utils.raise_on_duplicate_keys(df, ["a", "b", "c"], "shared key")


class TestRejectEmptyDataframe:
    def test_raises_when_the_frame_has_no_rows(self) -> None:
        df = pd.DataFrame({"a": pd.Series(dtype=str)})
        with pytest.raises(ValueError, match="^is empty$"):
            validation_utils.reject_empty_dataframe(df)

    def test_passes_when_the_frame_has_rows(self) -> None:
        validation_utils.reject_empty_dataframe(pd.DataFrame({"a": ["x"]}))


class TestValidateReferencesExist:
    """Tests for validate_references_exist()."""

    def test_passes_when_every_reference_is_present(self) -> None:
        validation_utils.validate_references_exist(
            ["Presenilin1", "WT"],
            ["WT", "Presenilin1", "APP"],
            source_name="marmo_genotype_label_map",
            target_name="marmo_model_metadata",
            item_name="models",
        )

    def test_raises_when_a_reference_is_missing(self) -> None:
        with pytest.raises(
            ValueError,
            match=(
                "marmo_genotype_label_map references models that are not present in "
                r"marmo_model_metadata: \['APP'\]"
            ),
        ):
            validation_utils.validate_references_exist(
                ["Presenilin1", "APP"],
                ["Presenilin1"],
                source_name="marmo_genotype_label_map",
                target_name="marmo_model_metadata",
                item_name="models",
            )

    def test_passes_when_both_sides_are_empty(self) -> None:
        validation_utils.validate_references_exist(
            [],
            [],
            source_name="source",
            target_name="target",
            item_name="values",
        )
