from typing import Optional

import pandas as pd
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnNestedObjectNotNull(ColumnMapMetricProvider):
    """Class definition for list member type checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.not_null"
    condition_value_keys = ("nonnull_threshold", "target_field")

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.core.series.Series, **kwargs) -> pd.Series:
        """Core logic for list member checking metric on a
        pandas execution engine.
        Returns: pd.Sereis
        """
        target_field = kwargs.get("target_field")
        nonnull_threshold = kwargs.get("nonnull_threshold", 1)

        if target_field is None:
            raise ValueError("Missing required parameter: target_field")

        counts = cls._flatten_nested_object_count_nulls(
            list_object=list(column), target_field=target_field
        )
        if counts["total_dict"] > 0:
            null_percentage = round(counts["total_none"] / counts["total_dict"], 1)
        else:
            null_percentage = 0
        if null_percentage > (1 - nonnull_threshold):
            return pd.Series([False] * len(column))
        else:
            return pd.Series([True] * len(column))

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
class ExpectColumnNestedObjectNotNull(ColumnMapExpectation):
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
                    "out": {"success": True},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_null"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("nonnull_threshold", "target_field")

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
    ExpectColumnNestedObjectNotNull().print_diagnostic_checklist()
