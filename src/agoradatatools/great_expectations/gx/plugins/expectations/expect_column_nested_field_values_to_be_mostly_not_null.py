from typing import Dict, Optional
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


# This method implements the core logic for the PandasExecutionEngine
class ColumnNestedObjectNotNull(ColumnAggregateMetricProvider):
    metric_name = "column.nested_object_not_null_ratio"
    value_keys = ("nonnull_threshold", "target_field")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> float | int:
        """
        Computes the proportion of non-null values for a specified field within a
        nested list of dictionaries contained in a Pandas column.

        The function flattens each cell's nested structure and checks whether the
        specified `target_field` exists and is not missing and not None.

        Parameters:
            column (pd.Series): A column where each row contains a list of dictionaries.
            **kwargs:
                target_field (str): The dictionary key to check for null values.
                nonnull_threshold (float): [Optional] Used for expectation logic,
                                            not directly required in this method.

        Returns:
            float: The proportion of non-null entries for `target_field` across all dictionaries.
                Returns 1.0 if there are no dictionaries to evaluate.
        """
        target_field = kwargs.get("target_field")

        if not target_field:
            raise ValueError("Missing required parameter: target_field")

        counts = cls._flatten_nested_object_count_nulls(
            list_object=list(column), target_field=target_field
        )

        return (
            round(counts["total_none"] / counts["total_dict"], 1)
            if counts["total_dict"]
            else 1
        )

    def _flatten_nested_object_count_nulls(
        list_object: list[list[dict[str, str | int | bool | None]]], target_field: str
    ) -> dict[str, int]:
        """
        Recursively flattens a nested list of dictionaries and counts how many
        dictionaries have a null value for the specified target field.

        Example:
        list_object = [
            [
                {"target_field": "value1", "other_key": "info"},
                {"target_field": "value2", "other_key": "info"}
            ],
            [
                {"target_field": None}, // one invalid: contain null
                {"target_field": "value2", "other_key": "info"}
            ],
            [
                {"target_field": ""}, // one valid: contain an empty string
            ],
            [
                {"another_key": ""}, // one invalid: target_field is missing
            ],
            [] //be ignored because it is empty
        ]

        The code flattens nested lists of dictionaries and counts the total number of null values for the specified field.
        total nulls = 2
        total dictionary = 6

        Parameters:
            list_object (list of dictionaries): A potentially nested list containing dictionaries.
            target_field (str): The key to check for null values within each dictionary.

        Returns:
            dictionary that contains two keys:
                - total_nulls (int): The number of dictionaries where target_field is None.
                - total_none (int): The total number of dictionaries encountered that contain the target_field.
        Note:
        A value is counted as invalid if:
            - The target field is missing from the dictionary
            - The value is None
        Empty lists are ignored entirely.
        Empty string is considered as valid
        """
        counts = {"total_none": 0, "total_dict": 0}

        def _flatten(list_object):
            for item in list_object:
                if isinstance(item, list):
                    _flatten(item)
                elif isinstance(item, dict):
                    if target_field not in item:
                        counts["total_none"] += 1
                    else:
                        target_field_value = item.get(target_field)
                        if target_field_value is None:
                            counts["total_none"] += 1
                    counts["total_dict"] += 1

        _flatten(list_object)
        return counts


# This class defines the Expectation itself
class ExpectColumnNestedObjectNotNull(ColumnAggregateExpectation):
    """Expect the proportion of non-null values for the specified field
    across all dictionaries in each list to meet or exceed the `nonnull_threshold`."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [
                    # "targeted" is null in one row - 1/4 invalid
                    [
                        {"targeted": "a", "other_key": "not empty"},
                        {"targeted": "a", "other_key": "not empty"},
                    ],
                    [{"targeted": None, "another_key2": None}],
                    [{"targeted": "a", "another key1": None, "another_key2": None}],
                ],
                "b": [
                    # "targeted" is null in one row - 1/2 invalid
                    [
                        {"targeted": None, "other_key": "not empty"},
                        {"targeted": "", "other_key": "not empty"},
                    ],
                    [],
                    [],
                ],
                "c": [
                    # "targeted" is null in five rows - 5/5 invalid
                    [{"targeted": None}, {"targeted": None}],
                    [{"targeted": None}],
                    [{"targeted": None}, {"targeted": None}],
                ],
                "d": [
                    # "targeted" is missing in two rows - 4/4 invalid
                    [{"not_targeted": "x"}, {"something_else": "y"}],
                    [{"also_missing": None}],
                    [{}],
                ],
            },
            "tests": [
                # Passes: 1 of 4 values is null, satisfying the 0.7 non-null threshold.
                {
                    "title": "target_meet_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "nonnull_threshold": 0.7,
                        "target_field": "targeted",
                    },
                    "out": {"success": True},
                },
                # Fails: 1 of 4 values is null, NOT satisfying the 0.9 non-null threshold.
                {
                    "title": "target_fail_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "nonnull_threshold": 0.9,
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
                        "nonnull_threshold": 0.5,
                        "target_field": "targeted",
                    },
                    "out": {"success": True},
                },
                # Failes: 5 of 5 values are null, NOT satisfying the 0.1 non-null threshold.
                {
                    "title": "target_all_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "c",
                        "nonnull_threshold": 0.1,
                        "target_field": "targeted",
                    },
                    "out": {"success": False},
                },
                # Failes: 4 of 4 values are missing, NOT satisfying the 0.1 non-null threshold.
                {
                    "title": "target_not_present",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "d",
                        "nonnull_threshold": 0.1,
                        "target_field": "targeted",
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    metric_dependencies = ("column.nested_object_not_null_ratio",)
    success_keys = ("nonnull_threshold", "target_field")
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
        non_null_threshold = kwargs.get("nonnull_threshold")
        target_field = kwargs.get("target_field")

        if not non_null_threshold:
            raise InvalidExpectationConfigurationError(
                "nonnull_threshold parameter is required"
            )

        if not isinstance(non_null_threshold, (float, int)) or not (
            0 <= non_null_threshold <= 1
        ):
            raise InvalidExpectationConfigurationError(
                "nonnull_threshold parameter needs to be set between 0 and 1"
            )

        if not target_field:
            raise InvalidExpectationConfigurationError("target_field is required")

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
    ):
        nonnull_threshold = configuration["kwargs"]["nonnull_threshold"]
        null_ratio = metrics["column.nested_object_not_null_ratio"]
        # if the null ratio is less than the allowed null ratio, return True; else return False
        return {
            "success": null_ratio <= (1 - nonnull_threshold),
            "result": {"observed_value": null_ratio},
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
