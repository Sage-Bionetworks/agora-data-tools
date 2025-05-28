from typing import Any, Optional

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
    condition_value_keys = ("nonnull_threshold","target_field")

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
        
        counts = cls._flatten_nested_object_count_nulls(list_object=list(column), target_field=target_field)
        print('counts', counts)
        if counts["total_dict"] > 0: 
            null_percentage = round(counts["total_none"]/counts["total_dict"], 1)
        else: 
            null_percentage = 0
        if null_percentage > (1-nonnull_threshold):
            return pd.Series([False] * len(column))
        else: 
            return pd.Series([True] * len(column))

    def _flatten_nested_object_count_nulls(list_object: list[list[dict[str, str | int | bool | None]]], target_field: str) -> dict[str, int]:
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
                {"target_field": None}, // be counted as null 
                {"another_key": "value"}
            ],
            [
                {"target_field": ""}, // not be counted as null 
                {"another_key": None}
            ]
        ]

        The code flattens nested lists of dictionaries and counts the total number of null values for the specified field.

        Parameters:
            list_object (list of dictionaries): A potentially nested list containing dictionaries.
            target_field (str): The key to check for null values within each dictionary.

        Returns:
            dictionary that contains two keys: 
                - total_nulls (int): The number of dictionaries where target_field is None.
                - total_none (int): The total number of dictionaries encountered that contain the target_field.
        Note: 
         - an empty list with no dictionary will be ignored. 
         - an empty string will not be counted as "null". 
        """
        counts = {"total_none": 0, "total_dict": 0}
        def _flatten(list_object): 
            for item in list_object:
                if isinstance(item, list): 
                    _flatten(item)
                elif isinstance(item, dict):
                    if target_field in item: 
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
                "a": [[{"targeted": "b", "other_key": "not empty"}, {"targeted": "a", "other_key": "not empty"}], [{"targeted": None}, {"another_key2": None}], [{"targeted": "another key"}, {"another_key2": None}]],
                "b": [[{"targeted": None, "other_key": "not empty"}, {"targeted": "", "other_key": "not empty"}], [], []],
                "c": [[{"targeted": None}, {"targeted": None}],[{"targeted": None}],[{"targeted": None}, {"targeted": None}]],
                "d": [[{"not_targeted": "x"}, {"something_else": "y"}],[{"also_missing": None}],[{}]],
            },
            "tests": [
                # should pass because "targeted" is null 1/6 and threshold is 0.7
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "nonnull_threshold": 0.7, "target_field": "targeted"},
                    "out": {"success": True},
                },
                # should fail because "targeted" is null 1/6 but threshold is 0.9
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "nonnull_threshold": 0.9, "target_field": "targeted"},
                    "out": {"success": False},
                },
                # should pass because "targeted is null" 1/2
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "b", "nonnull_threshold": 0.5, "target_field": "targeted"},
                    "out": {"success": True},
                },
                # should pass because "targeted" is null 100% but threshold is 0.1
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "nonnull_threshold": 0.1, "target_field": "targeted"},
                    "out": {"success": False},
                },
                # should pass because "targeted" is not present in the dictionary
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "d", "nonnull_threshold": 0.1, "target_field": "targeted"},
                    "out": {"success": True},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_null"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("nonnull_threshold","target_field")

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
            "@BWMac",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnNestedObjectNotNull().print_diagnostic_checklist()