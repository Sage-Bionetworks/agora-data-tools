"Custom expectation rule that counts the percentage of nulls in a targeted field"

from typing import Dict, Optional, List, Any
import pandas as pd
import json

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

METRIC_NAME = "column.nested_object_not_null_ratio"

# This method implements the core logic for the PandasExecutionEngine
class ColumnNestedObjectNotNull(ColumnAggregateMetricProvider):
    """A custom Great Expectations metric that calculates the proportion of non-null values
    for a specified key across nested lists of dictionaries in a column."""

    metric_name = METRIC_NAME
    value_keys = ("non_null_threshold", "target_field")

    @staticmethod
    def safe_parse(value: str) -> List[Dict[str, Any]]:
        """
        Load a JSON string and return a list of dictionaries.
        If the input is not valid JSON or not a list of dictionaries, return an empty list.

        Parameters:
            value[str]: the json string to be parsed. If input is not a valid json string, return an empty list

        Returns:
            Parsed json object or fall back to an empty list
        """
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        # Fallback if it's not valid JSON like "null"
        except (json.JSONDecodeError, TypeError):
            return []

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> float | int:
        """
        Computes the proportion of non-null values for a specified field within a
        nested list of dictionaries contained in a Pandas column.

        The function flattens each cell's nested structure and checks whether the
        specified `target_field` exists and is not None.

        Parameters:
            column (pd.Series): A column where each row contains a list of dictionaries.
            **kwargs:
                target_field (str):  the field to validate within each JSON object.
                non_null_threshold  (float): non null threshold
        Raises:
            ValueError: if no json object to be evaluated

        Returns:
            float: The proportion of non-null entries for `target_field` across all dictionaries.
                Returns 1.0 if there are no dictionaries to evaluate.
        """
        target_field = kwargs.get("target_field")

        if not target_field:
            raise ValueError("Missing required parameter: target_field")

        # parse json in the column
        series_parsed = column.apply(cls.safe_parse)

        counts = cls._flatten_nested_object_count_nulls(
            list_object=list(series_parsed), target_field=target_field
        )

        if counts["total_dict"]:
            return round(
                (counts["total_dict"] - counts["total_nulls"]) / counts["total_dict"], 1
            )
        else:
            raise ValueError("There are no JSON objects to validate.")

    @staticmethod
    def _flatten_nested_object_count_nulls(
        list_object: list[list[dict[str, str | int | bool | None]]], target_field: str
    ) -> dict[str, int]:
        """
        Recursively flattens a nested list of dictionaries and counts how many
        dictionaries have a null value for the specified target field.

        Example:
        list_object = [
            '[{"target_field": "value1", "other_key": "info"}, {"target_field": "value2", "other_key": "info"}]', //all valid
            '[{"target_field": null}, {"target_field": "value2", "other_key": "info"}]', // one INVALID: contain null
            '[{"target_field": ""}]', // one valid: contain an empty string
            '[{"another_key": ""}]', // one INVALID: target_field is missing
            '[{}]', // one INVALID: target_field is missing
            '[]' //IGNORE
            'null' //IGNORE
        ]

        The code recursively counts nulls for a field in nested dict lists.
        total_nulls = 3
        total_nulls = 7

        Parameters:
            list_object (list of dictionaries): A potentially nested list containing dictionaries.
            target_field (str): The key to check for null values within each dictionary.

        Returns:
            dictionary that contains two keys:
                - total_nulls (int): The number of dictionaries where target_field is None.
                - total_dict (int): The total number of dictionaries encountered that contain the target_field.
        Note:
        A value is counted as invalid if:
            - The target field is missing from the dictionary
            - The value is None
        Empty lists are ignored entirely.
        Empty string is considered as valid
        """
        counts = {"total_nulls": 0, "total_dict": 0}

        def _flatten(list_object: list[str]) -> dict[str, int]:
            """
            Recursively flattens a nested list of dictionaries and counts how many
            dictionaries have a null value for the specified target field.
            """
            # if this only contains empty list
            for item in list_object:
                if isinstance(item, list):
                    _flatten(item)
                elif isinstance(item, dict):
                    if target_field not in item or item.get(target_field) is None:
                        counts["total_nulls"] += 1
                    counts["total_dict"] += 1

        _flatten(list_object)
        return counts


# This class defines the Expectation itself
class ExpectColumnNestedObjectNotNull(ColumnAggregateExpectation):
    """Expect the proportion of non-null values for the specified field
    across all dictionaries in each list to meet or exceed the `non_null_threshold`."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [
                    # "targeted" is null in one row - 1/4 invalid
                    '[{"targeted": "a", "other_key": "m"}, {"targeted": "a", "other_key": "m"}]',
                    '[{"targeted": null, "another_key2": null}]',
                    '[{"targeted": "a", "another key1": null, "another_key2": null}]',
                ],
                "b": [
                    # "targeted" is null in one row - 1/2 invalid
                    '[{"targeted": null, "other_key": "b"}, {"targeted": "", "other_key": "b"}]',
                    "[]",
                    "[]",
                ],
                "c": [
                    # "targeted" is null in three rows - 5/5 invalid
                    '[{"targeted": null}, {"targeted": null, "a_key": "x"}]',
                    '[{"targeted": null}]',
                    '[{"targeted": null}, {"targeted": null, "another_key": "x"}]',
                ],
                "d": [
                    # "targeted" is null in one row, ignore null and empty list - 2/2 invalid
                    "null",
                    "[]",
                    '[{"targeted": null, "key": "x"}, {"targeted": null, "a_key": "y"}]',
                ],
                "e": [
                    # "targeted" is missing in one rows, ignore null - 1/3 invalid
                    "null",
                    '[{"targeted": ""}]',
                    '[{"targeted": null, "other_key": "y"}, {"targeted": "z", "other_key": "y"}]',
                ],
            },
            "tests": [
                # Passes: 1 of 3 values is null, satisfying the 0.7 non-null threshold.
                {
                    "title": "target_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "non_null_threshold": 0.7,
                        "target_field": "targeted",
                    },
                    "out": {"success": True},
                },
                # Fails: 1 of 3 values is null, NOT satisfying the 0.9 non-null threshold.
                {
                    "title": "target_fail_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "non_null_threshold": 0.9,
                        "target_field": "targeted",
                    },
                    "out": {"success": False},
                },
                # Passes: 1 of 2 values is null, satisfying the 0.5 non-null threshold.
                {
                    "title": "target_half_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "non_null_threshold": 0.5,
                        "target_field": "targeted",
                    },
                    "out": {"success": True},
                },
                # Fails: 5 of 5 values are null, NOT satisfying the 0.1 non-null threshold.
                {
                    "title": "target_all_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "c",
                        "non_null_threshold": 0.1,
                        "target_field": "targeted",
                    },
                    "out": {"success": False},
                },
                # Fails: 2 of 2 values are null, NOT satisfying the 0.01 non-null threshold.
                {
                    "title": "null_and_empty_list",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "non_null_threshold": 0.001,
                        "target_field": "targeted",
                    },
                    "out": {"success": False},
                },
                # Success: 1 of 3 values is invalid, satisfying the non-null threshold.
                {
                    "title": "target_mix_with_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "e",
                        "non_null_threshold": 0.6,
                        "target_field": "targeted",
                    },
                    "out": {"success": True},
                },
            ],
        }
    ]

    metric_dependencies = (METRIC_NAME,)
    success_keys = ("non_null_threshold", "target_field")
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
        non_null_threshold = kwargs.get("non_null_threshold")
        target_field = kwargs.get("target_field")

        if not target_field:
            raise InvalidExpectationConfigurationError("`target_field` is required.")

        if not isinstance(non_null_threshold, float) or not (
            0 < non_null_threshold < 1
        ):
            raise InvalidExpectationConfigurationError(
                "`non_null_threshold` is required and must be a float strictly between 0 and 1."
            )


    # This method performs a validation of your metrics against your success keys,
    # returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict[str, int],
        runtime_configuration: Optional[dict] = None,  # required by gx api
        execution_engine: ExecutionEngine = None,  # required by gx api
    ) -> dict[str, dict[str, float] | bool]:
        _ = runtime_configuration
        _ = execution_engine
        not_null_threshold = configuration["kwargs"]["non_null_threshold"]
        not_null_ratio = metrics[METRIC_NAME]
        # if the null ratio is less than the allowed null ratio, return True; else return False
        return {
            "success": not_null_ratio >= not_null_threshold,
            "result": {"observed_not_null_ratio": not_null_ratio},
        }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@linglp",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnNestedObjectNotNull().print_diagnostic_checklist()
