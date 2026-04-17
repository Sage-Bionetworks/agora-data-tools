import pytest
import pandas as pd
from agoradatatools.etl import utils
from synapseclient import Synapse

file_object = {
    "id": "syn25838546",
    "format": "table",
    "final_filename": "teams",
    "provenance": [],
    "destination": "syn25871921"
}


def test_login():
    assert type(utils._login_to_synapse()) is Synapse


def test_yaml():
    # tests if a valid file renders a list
    assert type(utils._get_config()) is list

    # tests if a bad file will
    with pytest.raises(SystemExit) as err:
        utils._get_config(config_path="./tests/test_assets/bad_config.yaml")
    assert err.type == SystemExit
    assert err.value.code == 9

    with pytest.raises(SystemExit) as err:
        utils._get_config(config_path="./tests/test_assets/bad_config.yam")
    assert err.type == SystemExit
    assert err.value.code == 2


class TestCheckColumnRules:
    def _make_datasets(self, col_data: dict) -> dict:
        return {"ds": pd.DataFrame(col_data)}

    # ── not_empty ────────────────────────────────────────────────────────────

    def test_not_empty_passes_for_all_non_empty_values(self):
        datasets = self._make_datasets({"col": ["a", "b", "c"]})
        utils.check_column_rules(
            datasets, {"ds": {"col": [utils.ColumnRule(rule="not_empty")]}}
        )

    def test_not_empty_raises_on_null(self):
        datasets = self._make_datasets({"col": ["a", None, "c"]})
        with pytest.raises(ValueError, match="col.*not_empty"):
            utils.check_column_rules(
                datasets, {"ds": {"col": [utils.ColumnRule(rule="not_empty")]}}
            )

    def test_not_empty_raises_on_empty_string(self):
        datasets = self._make_datasets({"col": ["a", "", "c"]})
        with pytest.raises(ValueError, match="col.*not_empty"):
            utils.check_column_rules(
                datasets, {"ds": {"col": [utils.ColumnRule(rule="not_empty")]}}
            )

    def test_not_empty_raises_on_whitespace_only_string(self):
        datasets = self._make_datasets({"col": ["a", "   ", "c"]})
        with pytest.raises(ValueError, match="col.*not_empty"):
            utils.check_column_rules(
                datasets, {"ds": {"col": [utils.ColumnRule(rule="not_empty")]}}
            )

    # ── matches_regex ─────────────────────────────────────────────────────────

    def test_matches_regex_passes_for_all_matching_values(self):
        datasets = self._make_datasets({"col": ["ENSMUSG001", "ENSMUSG002"]})
        utils.check_column_rules(
            datasets,
            {"ds": {"col": [utils.ColumnRule(rule="matches_regex", value="^ENSMUSG")]}},
        )

    def test_matches_regex_raises_with_correct_count(self):
        datasets = self._make_datasets({"col": ["ENSMUSG001", "ENSG002", "ENSG003"]})
        with pytest.raises(ValueError, match="2 row\\(s\\).*matches_regex.*\\^ENSMUSG"):
            utils.check_column_rules(
                datasets,
                {
                    "ds": {
                        "col": [
                            utils.ColumnRule(rule="matches_regex", value="^ENSMUSG")
                        ]
                    }
                },
            )

    def test_matches_regex_treats_null_as_violation(self):
        datasets = self._make_datasets({"col": ["ENSMUSG001", None]})
        with pytest.raises(ValueError, match="matches_regex"):
            utils.check_column_rules(
                datasets,
                {
                    "ds": {
                        "col": [
                            utils.ColumnRule(rule="matches_regex", value="^ENSMUSG")
                        ]
                    }
                },
            )

    # ── contains ─────────────────────────────────────────────────────────────

    def test_contains_passes_for_all_matching_values(self):
        datasets = self._make_datasets({"col": ["hello world", "world cup"]})
        utils.check_column_rules(
            datasets,
            {"ds": {"col": [utils.ColumnRule(rule="contains", value="world")]}},
        )

    def test_contains_raises_with_correct_count(self):
        datasets = self._make_datasets({"col": ["hello world", "goodbye", "adieu"]})
        with pytest.raises(ValueError, match="2 row\\(s\\).*contains.*world"):
            utils.check_column_rules(
                datasets,
                {"ds": {"col": [utils.ColumnRule(rule="contains", value="world")]}},
            )

    def test_contains_treats_null_as_violation(self):
        datasets = self._make_datasets({"col": ["hello world", None]})
        with pytest.raises(ValueError, match="contains"):
            utils.check_column_rules(
                datasets,
                {"ds": {"col": [utils.ColumnRule(rule="contains", value="world")]}},
            )

    # ── one_of ───────────────────────────────────────────────────────────────

    def test_one_of_passes_for_all_allowed_values(self):
        datasets = self._make_datasets({"col": ["male", "female", "male"]})
        utils.check_column_rules(
            datasets,
            {
                "ds": {
                    "col": [utils.ColumnRule(rule="one_of", value={"male", "female"})]
                }
            },
        )

    def test_one_of_raises_with_correct_count(self):
        datasets = self._make_datasets({"col": ["male", "unknown", "other"]})
        with pytest.raises(ValueError, match="2 row\\(s\\).*one_of"):
            utils.check_column_rules(
                datasets,
                {
                    "ds": {
                        "col": [
                            utils.ColumnRule(rule="one_of", value={"male", "female"})
                        ]
                    }
                },
            )

    # ── multi-violation collection ────────────────────────────────────────────

    def test_all_violations_collected_in_single_error(self):
        datasets = {
            "ds1": pd.DataFrame({"col_a": ["a", None]}),
            "ds2": pd.DataFrame({"col_b": ["ENSG001", "ENSG002"]}),
        }
        column_rules = {
            "ds1": {"col_a": [utils.ColumnRule(rule="not_empty")]},
            "ds2": {
                "col_b": [utils.ColumnRule(rule="matches_regex", value="^ENSMUSG")]
            },
        }
        with pytest.raises(ValueError) as exc_info:
            utils.check_column_rules(datasets, column_rules)
        message = str(exc_info.value)
        assert "ds1" in message
        assert "ds2" in message

    # ── missing dataset / column graceful skip ────────────────────────────────

    def test_missing_dataset_in_rules_is_skipped(self):
        datasets = {"ds": pd.DataFrame({"col": ["a"]})}
        utils.check_column_rules(
            datasets,
            {"nonexistent_ds": {"col": [utils.ColumnRule(rule="not_empty")]}},
        )

    def test_missing_column_in_rules_reports_violation(self):
        datasets = {"ds": pd.DataFrame({"other_col": ["a"]})}
        with pytest.raises(ValueError, match="does not exist"):
            utils.check_column_rules(
                datasets,
                {"ds": {"missing_col": [utils.ColumnRule(rule="not_empty")]}},
            )


if __name__ == "__main__":
    pytest.main()
