"""
Custom Expectation to validate that the proportion of string values for a specified field,
across all dictionaries in each list, match a user-provided regular expression pattern,
and that this proportion meets or exceeds a specified valid string threshold.
"""

from typing import Dict, Optional, List, Union

import re
import pandas as pd
from agoradatatools.great_expectations.gx.plugins.expectations.utils.utils import (
    safe_parse,
)

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)

NotDict = Union[str, int, float, bool, None, tuple, set]
DictOrNestedList = Union[Dict[str, NotDict], List["DictOrNestedList"]]
METRIC_NAME = "column.nested_object_regex_rule"

# This class defines a Metric to support your Expectation.
class ColumnMostlyStringLength(ColumnAggregateMetricProvider):
    """Metric provider for calculating the ratio of dictionaries in a list
    that have a specified field matching a regex pattern."""

    metric_name = METRIC_NAME
    value_keys = ("regex_pattern", "target_field", "valid_threshold")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> float:
        """
        Calculate the ratio of dictionaries in a list that have a specified field
        matching a regex pattern.

        Arguments:
            column (pd.Series): The column to analyze.
            **kwargs:
                - target_field (str): The field in the dictionary to check the regex.
                - regex_pattern (str): The regex pattern to match against the string.
        Raises:
            ValueError: if no json object to be evaluated

        Returns:
            float: The ratio of valid dictionaries to total dictionaries.
        """
        target_field = kwargs.get("target_field")
        regex_pattern = kwargs.get("regex_pattern")

        # parse json in the column
        series_parsed = column.apply(safe_parse)

        counts = cls._flatten_nested_object_regex_match(
            list_object=list(series_parsed),
            target_field=target_field,
            regex_pattern=regex_pattern,
        )

        if counts["total_dict"]:
            return round((counts["total_valid"]) / counts["total_dict"], 2)
        else:
            raise ValueError("There are no JSON objects to validate.")

    @staticmethod
    def _flatten_nested_object_regex_match(
        list_object: list[DictOrNestedList],
        target_field: str,
        regex_pattern: str,
    ) -> dict[str, int]:
        """
        Recursively flattens a nested list of dictionaries and counts how many
        dictionaries have a target field that matches the regex pattern.

        Arguments:
            list_object: a list of nested dictionaries or a single dictionary.
            target_field: the field in the dictionary to check the regex.
            regex_pattern: the regex pattern to match against the string.
        Returns:
            A dictionary with counts of total valid and total dictionaries.
        Note:
            A value is considered valid if it is a string and matches the regex pattern. If the target field does not exist, it is considered invalid.
        """
        counts = {"total_valid": 0, "total_dict": 0}
        pattern = re.compile(regex_pattern)

        def _traverse_and_count(obj: DictOrNestedList) -> dict[str, int]:
            """
            Recursively traverses a nested list of dictionaries and counts how many
            dictionaries have a target field whose value is a string matching the specified regex pattern.
            Arguments:
                obj: a nested dictionary or list of nested dictionaries to traverse.
            """
            if isinstance(obj, list):
                for item in obj:
                    _traverse_and_count(item)
            elif isinstance(obj, dict):
                counts["total_dict"] += 1
                value = obj.get(target_field)
                if isinstance(value, str) and pattern.match(value):
                    counts["total_valid"] += 1

        _traverse_and_count(list_object)
        return counts


class ExpectColumnNestedObjectRegexRule(ColumnAggregateExpectation):
    """Expect the proportion of string values for the specified field
    across all dictionaries in each list to match a user-provided regex pattern"""

    examples = [
        {
            "data": {
                "a": [
                    # "targeted" only contains numbers - 4/4 invalid
                    '[{"targeted": "456", "other_key": "m"}, {"targeted": "100", "other_key": "m"}]',
                    '[{"targeted": "340", "another_key2": null}]',
                    '[{"targeted": "123", "another key1": null, "another_key2": null}]',
                ],
                "b": [
                    # "targeted" only contains alphabetic strings - 4/4 valid
                    '[{"targeted": "apple", "other_key": "m"}, {"targeted": "banana", "other_key": "m"}]',
                    '[{"targeted": "kiwi", "another_key2": null}]',
                    '[{"targeted": "mango", "another key1": null, "another_key2": null}]',
                ],
                "c": [
                    # "targeted" contains mix of valid alphabetic strings, numbers and empty strings - 2/4 valid
                    '[{"targeted": "apple", "other_key": "m"}, {"targeted": "100", "other_key": "m"}]',
                    '[{"targeted": "kiwi", "another_key2": null}]',
                    '[{"targeted": "", "another key1": null, "another_key2": null}]',
                ],
                "d": [
                    # "targeted" contains mix of valid and invalid strings - 1/4 valid
                    '[{"targeted": "734-000-000", "other_key": "m"}, {"targeted": "", "other_key": "m"}]',
                    '[{"another_key2": null}]',
                    '[{"targeted": "kiwi", "another key1": null, "another_key2": null}]',
                ],
            },
            "tests": [
                # Fails: 0 of 4 values match the regex, not satisfying the 0.5 valid threshold.
                {
                    "title": "target_not_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "target_field": "targeted",
                        "regex_pattern": r"^[a-zA-Z]+$",
                        "valid_threshold": 0.5,
                    },
                    "out": {"success": False},
                },
                # Passes: 4 of 4 values match the regex, satisfying the 0.99 valid threshold.
                {
                    "title": "target_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "target_field": "targeted",
                        "regex_pattern": r"^[a-zA-Z]+$",
                        "valid_threshold": 0.99,
                    },
                    "out": {"success": True},
                },
                # Passes: 2 of 4 values match the regex, satisfying the 0.5 valid threshold.
                {
                    "title": "target_mix_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "c",
                        "target_field": "targeted",
                        "regex_pattern": r"^[a-zA-Z]+$",
                        "valid_threshold": 0.5,
                    },
                    "out": {"success": True},
                },
                # Fails: 1 of 4 values match the regex, not satisfying the 0.5 valid threshold.
                {
                    "title": "target_mix_not_meet_telephone_regex",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "target_field": "targeted",
                        "regex_pattern": r"^\d{3}-\d{3}-\d{3}$",
                        "valid_threshold": 0.3,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    metric_dependencies = (METRIC_NAME,)
    success_keys = ("regex_pattern", "target_field", "valid_threshold")
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        kwargs = configuration.kwargs
        valid_threshold = kwargs.get("valid_threshold")
        target_field = kwargs.get("target_field")
        regex_pattern = kwargs.get("regex_pattern")

        if target_field is None:
            raise InvalidExpectationConfigurationError("`target_field` is required.")

        if valid_threshold is None:
            raise InvalidExpectationConfigurationError("`valid_threshold` is required.")

        if not isinstance(valid_threshold, float):
            raise InvalidExpectationConfigurationError(
                "`valid_threshold` must be a float."
            )

        if valid_threshold <= 0 or valid_threshold >= 1:
            raise InvalidExpectationConfigurationError(
                "``valid_threshold` must be strictly between 0 and 1 (0 < valid_threshold < 1)"
            )

        if regex_pattern is None:
            raise InvalidExpectationConfigurationError("`regex_pattern` is required.")

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict[str, int],
        runtime_configuration: Optional[dict] = None,  # required by gx api
        execution_engine: ExecutionEngine = None,  # required by gx api
    ) -> dict[str, dict[str, float] | bool]:
        """
        This method performs a validation of your metrics against your success keys,
        returning a dict indicating the success or failure of the Expectation.
        """
        _ = runtime_configuration
        _ = execution_engine
        valid_threshold = configuration["kwargs"]["valid_threshold"]
        valid_ratio = metrics[METRIC_NAME]
        # if the null ratio is less than the allowed null ratio, return True; else return False
        return {
            "success": valid_ratio >= valid_threshold,
            "result": {"observed_valid_ratio": valid_ratio},
        }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@linglp",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnNestedObjectRegexRule().print_diagnostic_checklist()
