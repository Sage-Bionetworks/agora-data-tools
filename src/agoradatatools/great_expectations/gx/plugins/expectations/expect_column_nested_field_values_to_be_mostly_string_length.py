from typing import Dict, Optional
import pandas as pd
import operator

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine, ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)


# This method implements the core logic for the PandasExecutionEngine
class ColumnNestedObjectStrLength(ColumnAggregateMetricProvider):
    metric_name = "column_values.string_length_check"
    value_keys = ("mostly_threshold", "target_field", "operator", "length_threshold")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> float | int:
        """
        Compute the proportion of invalid string values for a specified `target_field` within
        a nested list of dictionaries in a column.

        The column is expected to contain rows of nested lists of dictionaries. This method
        recursively flattens these lists and applies the validation rules per dictionary.

        Parameters (from kwargs):
            target_field (str): The dictionary key to inspect.
            operator (str): The comparison operator to apply (e.g., '>=', '==').
            length_threshold (int): The integer value to compare the string length against.

        Returns:
            float: The proportion of valid entries. If no valid dictionaries are found (i.e., empty input), returns 1.

        Notes:
            - Empty lists are ignored and do not affect counts.
            - If `total_dict` is 0 (i.e., no relevant dictionaries were found), the function returns 1 (i.e., 100% valid).
        """
        target_field = kwargs.get("target_field")
        operator_name = kwargs.get("operator")
        length_threshold = kwargs.get("length_threshold")

        op_func_map = {
            ">=": operator.ge,
            ">": operator.gt,
            "<=": operator.le,
            "<": operator.lt,
            "==": operator.eq,
        }
        op_func = op_func_map.get(operator_name)

        counts = cls._flatten_nested_object_count_invalid_string(
            list_object=list(column),
            target_field=target_field,
            op_func=op_func,
            length_threshold=length_threshold,
        )

        valid_counts = counts["total_dict"] - counts["total_invalid"]
        return (
            round(valid_counts / counts["total_dict"], 1) if counts["total_dict"] else 1
        )

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
class ExpectColumnNestedObjectStrLength(ColumnAggregateExpectation):
    """
    Expect the proportion of string values in nested list-of-dict structures
    to satisfy the specified string length condition for a given field.
    """

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

    metric_dependencies = ("column_values.string_length_check",)
    success_keys = ("mostly_threshold", "target_field", "operator", "length_threshold")
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
        mostly_threshold = kwargs.get("mostly_threshold")
        operator_name = kwargs.get("operator")

        required_params = [
            "mostly_threshold",
            "target_field",
            "operator",
            "length_threshold",
        ]
        missing_params = [
            param
            for param in required_params
            if configuration.kwargs.get(param) is None
        ]

        if missing_params:
            raise InvalidExpectationConfigurationError(
                f"Missing required parameter(s): {', '.join(missing_params)}"
            )

        if not isinstance(mostly_threshold, (float, int)) or not (
            0 <= mostly_threshold <= 1
        ):
            raise InvalidExpectationConfigurationError(
                "mostly_threshold parameter needs to be set between 0 and 1"
            )
        if operator_name not in [">=", ">", "<=", "<", "=="]:
            raise InvalidExpectationConfigurationError(
                f"{operator_name} is not a valid operator. The available options are: >=, >, <=, <, =="
            )

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,  # required by gx api
        execution_engine: ExecutionEngine = None,  # required by gx api
    ):
        _ = runtime_configuration
        _ = execution_engine
        valid_threshold = configuration["kwargs"]["mostly_threshold"]
        valid_ratio = metrics["column_values.string_length_check"]
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
    ExpectColumnNestedObjectStrLength().print_diagnostic_checklist()
