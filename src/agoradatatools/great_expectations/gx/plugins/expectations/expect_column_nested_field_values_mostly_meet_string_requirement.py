"""
Custom Expectation to validate that the proportion of string values
for a specified field across all dictionaries in each list meets or exceeds a given threshold.
"""

from typing import Dict, Optional, List, Any, Union
import json
import operator
import pandas as pd

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
METRIC_NAME = "column.nested_object_meet_string_length_ratio"
OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}

# This class defines a Metric to support your Expectation.
class ColumnMostlyStringLength(ColumnAggregateMetricProvider):
    """Metric provider for calculating the ratio of dictionaries in a list
    that have a specified field meeting a string length requirement."""

    metric_name = METRIC_NAME
    value_keys = (
        "length_threshold",
        "target_field",
        "operator",
        "valid_string_threshold",
    )

    @staticmethod
    def safe_parse(value: str) -> List[Dict[str, Any]]:
        """
        Load a JSON string and return a list of dictionaries.
        If the input is not a valid JSON, return an empty list.

        Parameters:
            value[str]: the json string to be parsed. If input is not a valid json string, return an empty list

        Returns:
            Parsed json object or fall back to an empty list
        """
        # Fallback if it's "null"
        if value == "null":
            return []
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON string: {value}") from e

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> float:
        target_field = kwargs.get("target_field")
        selected_operator = kwargs.get("operator")
        length_threshold = kwargs.get("length_threshold")

        # parse json in the column
        series_parsed = column.apply(cls.safe_parse)

        counts = cls._flatten_nested_object_string_length(
            list_object=list(series_parsed),
            target_field=target_field,
            selected_operator=selected_operator,
            length_threshold=length_threshold,
        )

        if counts["total_dict"]:
            return round((counts["total_valid"]) / counts["total_dict"], 2)
        else:
            raise ValueError("There are no JSON objects to validate.")

    @staticmethod
    def _flatten_nested_object_string_length(
        list_object: list[DictOrNestedList],
        target_field: str,
        selected_operator: str,
        length_threshold: int,
    ) -> dict[str, int]:
        counts = {"total_valid": 0, "total_dict": 0}

        def _flatten(object: DictOrNestedList) -> dict[str, int]:
            """
            Recursively flattens a nested list of dictionaries and counts how many
            dictionaries have a target field that have the right string length.

            Arguments:
                list_object: a list of nested dictionaries or a single dictionary.
                target_field: the field in the dictionary to check the string length.
                selected_operator: the operator to use for comparison (e.g., ">", "<", ">=", "<=", "==", "!=").
                length_threshold: the length threshold to compare against.
            Returns:
                A dictionary with counts of total invalid and total dictionaries.
            """
            # if this only contains empty list
            for item in object:
                if isinstance(item, list):
                    _flatten(item)
                elif isinstance(item, dict):
                    if isinstance(item.get(target_field), str) and OPS[
                        selected_operator
                    ](len(item.get(target_field)), length_threshold):
                        counts["total_valid"] += 1
                    counts["total_dict"] += 1

        _flatten(list_object)
        return counts


class ExpectColumnMostlyStringLength(ColumnAggregateExpectation):
    """Expect the proportion of string values for the specified field
    across all dictionaries in each list to meet or exceed the `string_threshold`."""

    examples = [
        {
            "data": {
                "a": [
                    # "targeted" has string that meets the length threshold - 4/4 valid
                    '[{"targeted": "a very very long string", "other_key": "m"}, {"targeted": "a very very long string", "other_key": "m"}]',
                    '[{"targeted": "a very very long string", "another_key2": null}]',
                    '[{"targeted": "a very very long string", "another key1": null, "another_key2": null}]',
                ],
                "b": [
                    # "targeted" contain wrong types or string that does not meet the length threshold - 0/4 valid
                    '[{"targeted": "True", "other_key": "m"}, {"targeted": "b", "other_key": "m"}]',
                    '[{"targeted": 5, "another_key2": null}]',
                    '[{"targeted": "a", "another key1": null, "another_key2": null}]',
                ],
                "c": [
                    # "targeted" field does not exist or contain null/empty string - 0/4 valid
                    '[{"targeted": null, "other_key": "m"}, {"targeted": "", "other_key": "m"}]',
                    '[{"another_key2": null}]',
                    '[{"another key1": null, "another_key2": null}]',
                ],
                "d": [
                    # "targeted" field contains a mix of valid and invalid string - 2/4 valid
                    '[{"targeted": "", "other_key": "m"}, {"targeted": "a very long and valid string", "other_key": "m"}]',
                    '[{"another_key2": null}]',
                    '[{"targeted": "a very long and valid string", "another key1": null, "another_key2": null}]',
                ],
                "e": [
                    # "targeted" field contains a mix of valid and invalid string - 2/5 valid
                    '[{"targeted": "a valid string", "other_key": "m"}, {"targeted": "a valid string", "other_key": "m"}]',
                    '[{"another_key2": null, "targeted": "m"}, {"another_key2": null, "targeted": "va"}]',
                    '[{"targeted": "a", "another key1": null, "another_key2": null}]',
                ],
                "f": [
                    # "targeted" field contains a mix of valid and invalid string - 1/5 valid
                    '[{"targeted": "apple", "other_key": "m"}, {"targeted": "kiwi", "other_key": "m"}]',
                    '[{"another_key2": null, "targeted": null}, {"another_key2": null, "targeted": "cd"}]',
                    '[{"targeted": "kiwi", "another key1": null, "another_key2": null}]',
                ],
            },
            "tests": [
                # Passes: 4 of 4 values is valid, satisfying the 0.99 valid string threshold.
                {
                    "title": "target_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 5,
                        "valid_string_threshold": 0.99,
                    },
                    "out": {"success": True},
                },
                # Fails: 0 of 4 values is valid, not satisfying the 0.1 valid string threshold.
                {
                    "title": "target_not_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 5,
                        "valid_string_threshold": 0.1,
                    },
                    "out": {"success": False},
                },
                # Fails: 0 of 4 values are valid, not satisfying the 0.1 valid string threshold.
                {
                    "title": "target_does_not_exist_not_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "c",
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 10,
                        "valid_string_threshold": 0.1,
                    },
                    "out": {"success": False},
                },
                # Passes: 2 of 4 values are valid, satisfying the 0.4 valid string threshold.
                {
                    "title": "target_mix_of_valid_and_invalid_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "target_field": "targeted",
                        "operator": ">=",
                        "length_threshold": 3,
                        "valid_string_threshold": 0.4,
                    },
                    "out": {"success": True},
                },
                # Fails: 2 of 5 values are valid, not satisfying the 0.8 valid string threshold.
                {
                    "title": "target_mix_of_valid_and_invalid_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "e",
                        "target_field": "targeted",
                        "operator": "<=",
                        "length_threshold": 2,
                        "valid_string_threshold": 0.8,
                    },
                    "out": {"success": False},
                },
                # Fails: 1 of 5 values are valid, not satisfying the 0.3 valid string threshold.
                {
                    "title": "target_mix_of_valid_and_invalid_equal_operator",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "f",
                        "target_field": "targeted",
                        "operator": "==",
                        "length_threshold": 5,
                        "valid_string_threshold": 0.3,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    metric_dependencies = (METRIC_NAME,)
    success_keys = (
        "valid_string_threshold",
        "target_field",
        "operator",
        "length_threshold",
    )
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
        valid_string_threshold = kwargs.get("valid_string_threshold")
        target_field = kwargs.get("target_field")
        length_threshold = kwargs.get("length_threshold")
        operator = kwargs.get("operator")

        if target_field is None:
            raise InvalidExpectationConfigurationError("`target_field` is required.")

        if not isinstance(valid_string_threshold, float) or not (
            0 < valid_string_threshold < 1
        ):
            raise InvalidExpectationConfigurationError(
                "`valid_string_threshold` is required and must be a float strictly between 0 and 1."
            )

        if not isinstance(length_threshold, int) or (length_threshold < 0):
            raise InvalidExpectationConfigurationError(
                "`length_threshold` is required and must be a non-negative integer (zero or greater)."
            )
        if not operator or (operator not in OPS):
            raise InvalidExpectationConfigurationError(
                "`operator` is required and must be one of: >, <, >=, <=, ==, !=."
            )

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
        valid_string_threshold = configuration["kwargs"]["valid_string_threshold"]
        valid_ratio = metrics[METRIC_NAME]
        # if the null ratio is less than the allowed null ratio, return True; else return False
        return {
            "success": valid_ratio >= valid_string_threshold,
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
    ExpectColumnMostlyStringLength().print_diagnostic_checklist()
