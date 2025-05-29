from typing import Optional

import pandas as pd
import operator
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnNestedObjectStrLength(ColumnMapMetricProvider):
    """Class definition for list member type checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.string_length_check"
    condition_value_keys = (
        "mostly_threshold",
        "target_field",
        "operator",
        "length_threshold",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.core.series.Series, **kwargs) -> pd.Series:
        """Core logic for list member checking metric on a
        pandas execution engine.
        Returns: pd.Sereis
        """
        target_field = kwargs.get("target_field")
        threshold = kwargs.get("mostly_threshold", 1)
        operator_name = kwargs.get("operator")
        length_threshold = kwargs.get("length_threshold", 0)

        op_func_map = {
            ">=": operator.ge,
            ">": operator.gt,
            "<=": operator.le,
            "<": operator.lt,
            "==": operator.eq,
        }
        op_func = op_func_map.get(operator_name)

        if target_field is None:
            raise ValueError("Missing required parameter: target_field")

        if op_func is None:
            raise ValueError(
                f"{operator} is not a valid operator. The available options are: >=, >, <=, <, =="
            )

        counts = cls._flatten_nested_object_count_invalid_string(
            list_object=list(column),
            target_field=target_field,
            op_func=op_func,
            length_threshold=length_threshold,
        )

        if counts["total_dict"] > 0:
            invalid_percentage = round(
                counts["total_invalid"] / counts["total_dict"], 1
            )
        else:
            invalid_percentage = 0
        if invalid_percentage > (1 - threshold):
            return pd.Series([False] * len(column))
        else:
            return pd.Series([True] * len(column))

    def _flatten_nested_object_count_invalid_string(
        list_object: list[list[dict[str, str | int | bool | None]]],
        target_field: str,
        op_func: str,
        length_threshold: int,
    ) -> dict[str, int]:
        """
        Recursively flattens a nested list of dictionaries and counts how many
        values for the specified target field fail a string length check using
        the given comparison operator and threshold.

        Args:
            list_object: A nested list containing dictionaries.
            target_field: The key to check within each dictionary.
            op_func: A string representing the comparison operator
                    (e.g., '>', '<=', '==').
            length_threshold: The integer threshold to compare string lengths against.

        Returns:
            A dictionary with:
                - "total_invalid": Number of string values that fail the check.
                - "total_checked": Number of valid string values evaluated.
        Example:
            list_object = [
                [{"targeted": "abc"}, {"targeted": ""}],   # one invalid
                [], # ignored
                [{"other_key": "xyz"}, {"targeted": None}]  # two invalid (missing + None)
                [{"other_key": "xyz"}, {"targeted": "abc"}]  # validity depends on string threshold
                [{"other_key": "xyz"}, {"targeted": True}]  # invalid: value is not a string
            ]

        Note:
        A value is counted as invalid if:
            - The target field is missing from the dictionary
            - The value is None or an empty string
            - The target field does not contain a string (i.e. bool)
            - The string length does not satisfy the comparison with the threshold

        Empty lists are ignored entirely.
        """
        counts = {"total_invalid": 0, "total_dict": 0}

        def _flatten(list_object):
            for item in list_object:
                if isinstance(item, list):
                    _flatten(item)
                elif isinstance(item, dict):
                    target_field_value = item.get(target_field)
                    if (
                        not target_field_value
                        or not isinstance(target_field_value, str)
                        or not op_func(len(target_field_value), length_threshold)
                    ):
                        counts["total_invalid"] += 1
                    counts["total_dict"] += 1

        _flatten(list_object)
        return counts


# This class defines the Expectation itself
class ExpectColumnNestedObjectStrLength(ColumnMapExpectation):
    """Expect the proportion of non-null values for the specified field
    across all dictionaries in each list to meet or exceed the `nonnull_threshold`."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                # "targeted" is an empty string in one row  — 1/2 invalid
                "a": [
                    [{"targeted": "a", "other_key": "not empty"}],
                    [{"targeted": "", "other_key": "not empty"}],
                ],
                # "targeted" is None in one row — 1/2 null
                "b": [
                    [{"targeted": "b", "other_key": "not empty"}],
                    [{"targeted": None, "other_key": ""}],
                ],
                # "targeted" key is missing in both rows — 2/2 null
                "c": [[{"other_key": "not empty"}], [{"other_key": ""}]],
                # "targeted" has a string with length = 3 in both rows
                "d": [
                    [{"targeted": "aaa", "other_key": "not empty"}],
                    [{"targeted": "aaa", "other_key": "not empty"}],
                ],
                # both rows are emtpy
                "e": [[], []],
                # "targeted" has a wrong data type
                "f": [
                    [{"targeted": True, "other_key": "not empty"}],
                    [{"targeted": "aaa", "other_key": "not empty"}],
                ],
            },
            "tests": [
                {
                    # Passes: 50% of values are empty strings (length = 0), which is acceptable under 0.4 threshold
                    "title": "targeted_field_contain_empty_string_pass",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "mostly_threshold": 0.4,
                        "target_field": "targeted",
                        "operator": ">",
                        "length_threshold": 0,
                    },
                    "out": {"success": True},
                },
                {
                    # Passes: 50% of values are None (treated as invalid), meets mostly threshold of 0.4
                    "title": "targeted_field_contain_null_pass",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "mostly_threshold": 0.4,
                        "target_field": "targeted",
                        "operator": ">",
                        "length_threshold": 0,
                    },
                    "out": {"success": True},
                },
                {
                    # Fails: 100% of dictionaries are missing the 'targeted' key, which is above the 0.1 tolerance
                    "title": "targeted_field_missing_fail",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "c",
                        "mostly_threshold": 0.1,
                        "target_field": "targeted",
                        "operator": ">",
                        "length_threshold": 0,
                    },
                    "out": {"success": False},
                },
                {
                    # Fails: All values are too short (length < 4), violating the mostly threshold
                    "title": "targeted_field_too_short_fail",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "mostly_threshold": 0.9,
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 4,
                    },
                    "out": {"success": False},
                },
                {
                    # Passes: All values meet the <= 3 length requirement
                    "title": "targeted_field_correct_length_se_pass",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "mostly_threshold": 0.9,
                        "target_field": "targeted",
                        "operator": "<=",
                        "length_threshold": 3,
                    },
                    "out": {"success": True},
                },
                {
                    # Passes: All values have exact length of 3, satisfying the equality check
                    "title": "targeted_field_correct_length_e_pass",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "mostly_threshold": 0.9,
                        "target_field": "targeted",
                        "operator": "==",
                        "length_threshold": 3,
                    },
                    "out": {"success": True},
                },
                {
                    # Passes: Empty rows are ignored and don't affect validation outcome
                    "title": "targeted_field_empty_rows",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "e",
                        "mostly_threshold": 1,
                        "target_field": "targeted",
                        "operator": ">",
                        "length_threshold": 0,
                    },
                    "out": {"success": True},
                },
                {
                    # Fails: Contains non-string values (e.g., bool), treated as invalid
                    "title": "targeted_field_wrong_type_fail",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "f",
                        "mostly_threshold": 1,
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 1,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.string_length_check"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly_threshold", "target_field", "operator", "length_threshold")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@linglp",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnNestedObjectStrLength().print_diagnostic_checklist()
