from collections.abc import Sequence
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from agoradatatools.etl.utils import column_rules as cr

RuleClassWithValueArg = (
    type[cr.MatchesRegexRule] | type[cr.ContainsSubstringRule] | type[cr.OneOfRule]
)


class TestColumnRuleContract:
    """Verify that every concrete ColumnRule subclass honours the base class contract."""

    @pytest.mark.parametrize(
        "rule",
        [
            cr.NotEmptyRule(),
            cr.MatchesRegexRule(value="^ENSMUSG"),
            cr.ContainsSubstringRule(value="world"),
            cr.OneOfRule(value={"a"}),
        ],
    )
    def test_count_violations_returns_int(self, rule: cr.ColumnRule) -> None:
        assert isinstance(rule.count_violations(pd.Series(["a", None])), int)

    @pytest.mark.parametrize(
        "rule",
        [
            cr.NotEmptyRule(),
            cr.MatchesRegexRule(value="^ENSMUSG"),
            cr.ContainsSubstringRule(value="world"),
            cr.OneOfRule(value={"a"}),
        ],
    )
    def test_count_violations_is_non_negative(self, rule: cr.ColumnRule) -> None:
        assert rule.count_violations(pd.Series(["a", None])) >= 0

    def test_column_rule_cannot_be_instantiated_directly(self) -> None:
        with pytest.raises(TypeError):
            cr.ColumnRule()


class TestNotEmptyRule:
    """Unit tests for NotEmptyRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    def test_no_violations_for_all_valid(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series(["a", "b", "c"])) == 0

    def test_counts_none_as_violation(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series(["a", None, "c"])) == 1

    def test_counts_nan_as_violation(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series(["a", np.nan, "c"])) == 1

    def test_counts_empty_string_as_violation(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series(["a", "", "c"])) == 1

    def test_counts_whitespace_only_as_violation(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series(["a", "   ", "c"])) == 1

    def test_counts_multiple_violations(self) -> None:
        assert (
            cr.NotEmptyRule().count_violations(
                self._series(["a", None, "", "   ", "b"])
            )
            == 3
        )

    def test_all_violations(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series([None, "", "   "])) == 3

    def test_empty_series(self) -> None:
        assert cr.NotEmptyRule().count_violations(self._series([])) == 0

    def test_value_detail_is_empty_string(self) -> None:
        assert cr.NotEmptyRule().value_detail == ""


class TestMatchesRegexRule:
    """Unit tests for MatchesRegexRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, "", 123, np.nan])
    def test_raises_when_value_is_invalid(
        self, bad_value: int | str | float | None
    ) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            cr.MatchesRegexRule(value=bad_value)

    def test_raises_when_value_is_invalid_regex(self) -> None:
        with pytest.raises(ValueError, match="valid regex"):
            cr.MatchesRegexRule(value="[invalid")

    def test_no_violations_when_all_match(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", "ENSMUSG002"])) == 0

    def test_counts_non_matching_value(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", "ENSG002"])) == 1

    def test_counts_all_non_matching(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSG001", "ENSG002"])) == 2

    def test_skips_none(self) -> None:
        # Nulls are skipped so the rule only validates the format of present values.
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", None])) == 0

    def test_skips_nan(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", np.nan])) == 0

    def test_counts_empty_string_as_violation(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["ENSMUSG001", ""])) == 1

    def test_partial_match_is_violation(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series(["prefix_ENSMUSG001"])) == 1

    def test_empty_series(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_pattern(self) -> None:
        rule = cr.MatchesRegexRule(value="^ENSMUSG")
        assert "^ENSMUSG" in rule.value_detail


class TestContainsSubstringRule:
    """Unit tests for ContainsSubstringRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, np.nan, ""])
    def test_raises_when_value_is_invalid(self, bad_value: str | float | None) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            cr.ContainsSubstringRule(value=bad_value)

    def test_no_violations_when_all_contain_substring(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", "world cup"])) == 0

    def test_counts_missing_substring(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", "goodbye"])) == 1

    def test_counts_all_missing(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["foo", "bar"])) == 2

    def test_counts_none_as_violation(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", None])) == 1

    def test_counts_nan_as_violation(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", np.nan])) == 1

    def test_counts_empty_string_as_violation(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series(["hello world", ""])) == 1

    def test_counts_non_string_data_as_violations(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert (
            rule.count_violations(self._series(["hello world", "goodbye", 2, 5.555555]))
            == 3
        )

    def test_value_is_treated_as_literal_not_regex(self) -> None:
        rule = cr.ContainsSubstringRule(value="-")
        assert rule.count_violations(self._series(["hello-world", "goodbye"])) == 1

    def test_empty_series(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_substring(self) -> None:
        rule = cr.ContainsSubstringRule(value="world")
        assert "world" in rule.value_detail


class TestOneOfRule:
    """Unit tests for OneOfRule.count_violations()."""

    def _series(self, data: Sequence[object]) -> pd.Series:
        return pd.Series(data)

    @pytest.mark.parametrize("bad_value", [None, set(), [], {}])
    def test_raises_when_value_is_invalid(self, bad_value: object) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            cr.OneOfRule(value=bad_value)

    def test_no_violations_when_all_in_set(self) -> None:
        rule = cr.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", "female", "male"])) == 0

    def test_counts_value_not_in_set(self) -> None:
        rule = cr.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", "unknown"])) == 1

    def test_counts_all_invalid(self) -> None:
        rule = cr.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["unknown", "other"])) == 2

    def test_counts_none_as_violation(self) -> None:
        rule = cr.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series(["male", None])) == 1

    def test_works_with_list_as_allowed_values(self) -> None:
        rule = cr.OneOfRule(value=["male", "female"])
        assert rule.count_violations(self._series(["male", "unknown"])) == 1

    def test_empty_series(self) -> None:
        rule = cr.OneOfRule(value={"male", "female"})
        assert rule.count_violations(self._series([])) == 0

    def test_value_detail_includes_allowed_values(self) -> None:
        rule = cr.OneOfRule(value={"male"})
        assert "male" in rule.value_detail

    def test_no_violations_numeric_allowed_values(self) -> None:
        rule = cr.OneOfRule(value=[1, 2, 3])
        assert rule.count_violations(self._series([1, 1, 2])) == 0

    def test_no_violations_bool_allowed_values(self) -> None:
        rule = cr.OneOfRule(value=[True, False])
        assert rule.count_violations(self._series([True, False, False])) == 0

    def test_no_violations_sentinel_allowed_values(self) -> None:
        # Series.isin matches np.nan to np.nan in the allowed collection (pandas semantics).
        rule = cr.OneOfRule(value=["", None, [], np.nan])
        s = self._series(["", "", None, [], np.nan])
        assert rule.count_violations(s) == 0

    def test_no_violations_mixed_int_and_string_allowed(self) -> None:
        rule = cr.OneOfRule(value=[2, "2"])
        assert rule.count_violations(self._series([2, 2, "2"])) == 0

    def test_violations_when_numeric_allowed_but_string_in_series(self) -> None:
        rule = cr.OneOfRule(value=[1, 2])
        assert rule.count_violations(self._series([1, 1, "2"])) == 1

    def test_violations_all_strings_when_numeric_allowed(self) -> None:
        rule = cr.OneOfRule(value=[1, 2])
        assert rule.count_violations(self._series(["1", "1", "2"])) == 3

    def test_bool_and_int_equivalence_with_bool_allowed(self) -> None:
        rule = cr.OneOfRule(value=[True, False])
        s = self._series([True, 1, False, 0])
        assert rule.count_violations(s) == 0

    def test_bool_and_int_equivalence_with_int_allowed(self) -> None:
        rule = cr.OneOfRule(value=[0, 1])
        s = self._series([True, 1, False, 0])
        assert rule.count_violations(s) == 0


class TestCheckColumnRules:
    """Tests for check_column_rules() and its supporting _check_single_rule() helper."""

    def _make_datasets(self, col_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        return {"ds": pd.DataFrame(col_data)}

    @pytest.mark.parametrize(
        "col_data, rule",
        [
            (["a", "b", "c"], cr.NotEmptyRule()),
            (
                ["ENSMUSG001", "ENSMUSG002"],
                cr.MatchesRegexRule(value="^ENSMUSG"),
            ),
            (
                ["hello world", "world cup"],
                cr.ContainsSubstringRule(value="world"),
            ),
            (
                ["male", "female", "male"],
                cr.OneOfRule(value={"male", "female"}),
            ),
        ],
    )
    def test_rule_passes_for_all_valid_values(
        self, col_data: list[Any], rule: cr.ColumnRule
    ) -> None:
        datasets = self._make_datasets({"col": col_data})
        cr.check_column_rules(datasets, {"ds": {"col": [rule]}})

    @pytest.mark.parametrize("bad_value", [None, np.nan, "", "   "])
    def test_not_empty_raises_on_invalid_value(
        self, bad_value: str | float | None
    ) -> None:
        datasets = self._make_datasets({"col": ["a", bad_value, "c"]})
        with pytest.raises(ValueError, match="col.*not_empty"):
            cr.check_column_rules(datasets, {"ds": {"col": [cr.NotEmptyRule()]}})

    @pytest.mark.parametrize(
        "col_data, rule, match_pattern",
        [
            (
                ["ENSMUSG001", "ENSG002", "ENSG003"],
                cr.MatchesRegexRule(value="^ENSMUSG"),
                r"2 row\(s\).*matches_regex.*\^ENSMUSG",
            ),
            (
                ["hello world", "goodbye"],
                cr.ContainsSubstringRule(value="world"),
                r"1 row\(s\).*contains_substring.*world",
            ),
            (
                ["hello world", "goodbye", "adieu", "farewell"],
                cr.ContainsSubstringRule(value="world"),
                r"3 row\(s\).*contains_substring.*world",
            ),
            (
                ["male", "female", "unknown", "other"],
                cr.OneOfRule(value={"male", "female"}),
                r"2 row\(s\).*one_of",
            ),
            (
                ["valid", None, ""],
                cr.NotEmptyRule(),
                r"2 row\(s\).*not_empty",
            ),
            (
                ["valid", None, "", "   ", "also valid"],
                cr.NotEmptyRule(),
                r"3 row\(s\).*not_empty",
            ),
        ],
    )
    def test_rule_raises_with_correct_count(
        self, col_data: list[Any], rule: cr.ColumnRule, match_pattern: str
    ) -> None:
        datasets = self._make_datasets({"col": col_data})
        with pytest.raises(ValueError, match=match_pattern):
            cr.check_column_rules(datasets, {"ds": {"col": [rule]}})

    @pytest.mark.parametrize(
        "good_value, rule",
        [
            ("hello world", cr.ContainsSubstringRule(value="world")),
        ],
    )
    def test_rule_treats_null_as_violation(
        self, good_value: str, rule: cr.ColumnRule
    ) -> None:
        datasets = self._make_datasets({"col": [good_value, None]})
        with pytest.raises(ValueError, match=rule.rule):
            cr.check_column_rules(datasets, {"ds": {"col": [rule]}})

    def test_matches_regex_rule_ignores_null(self) -> None:
        # MatchesRegexRule only validates present values; nulls are not violations.
        datasets = self._make_datasets({"col": ["ENSMUSG001", None]})
        cr.check_column_rules(
            datasets, {"ds": {"col": [cr.MatchesRegexRule(value="^ENSMUSG")]}}
        )

    def test_all_violations_collected_in_single_error(self) -> None:
        datasets = {
            "ds1": pd.DataFrame({"col_a": ["a", None]}),
            "ds2": pd.DataFrame({"col_b": ["ENSG001", "ENSG002"]}),
        }
        column_rules_map = {
            "ds1": {"col_a": [cr.NotEmptyRule()]},
            "ds2": {"col_b": [cr.MatchesRegexRule(value="^ENSMUSG")]},
        }
        with pytest.raises(ValueError) as exc_info:
            cr.check_column_rules(datasets, column_rules_map)
        message = str(exc_info.value)
        assert "ds1" in message
        assert "ds2" in message

    def test_missing_dataset_in_rules_is_skipped(self) -> None:
        datasets = {"ds": pd.DataFrame({"col": ["a"]})}
        cr.check_column_rules(
            datasets,
            {"nonexistent_ds": {"col": [cr.NotEmptyRule()]}},
        )

    def test_missing_column_in_rules_reports_violation(self) -> None:
        datasets = {"ds": pd.DataFrame({"other_col": ["a"]})}
        with pytest.raises(ValueError, match="does not exist"):
            cr.check_column_rules(
                datasets,
                {"ds": {"missing_col": [cr.NotEmptyRule()]}},
            )

    @pytest.mark.parametrize(
        "rule_class",
        [
            cr.MatchesRegexRule,
            cr.ContainsSubstringRule,
            cr.OneOfRule,
        ],
    )
    def test_value_required_rule_raises_when_value_is_none(
        self, rule_class: RuleClassWithValueArg
    ) -> None:
        with pytest.raises(ValueError, match="requires a non-None"):
            rule_class(value=None)
