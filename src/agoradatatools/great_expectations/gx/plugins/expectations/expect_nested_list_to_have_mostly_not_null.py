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
class ColumnListIndividualItemNotNull(ColumnMapMetricProvider):
    """Class definition for list member type checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.not_null"
    condition_value_keys = ("mostly_threshold",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.core.series.Series, mostly_threshold: float | int = 1, **kwargs) -> pd.Series:
        """Core logic for list member checking metric on a
        pandas execution engine.

        Args:
            column (pd.core.series.Series): Pandas column to be evaluated.
            mostly_threshold (float or int): a float or int >= 0 and <=1
        Returns:
            pd.Series: Whether or not the column values have the expected list members.
        """
        return column.apply(lambda x: cls._check_list_item_not_null(x, mostly_threshold))

    @staticmethod
    def _check_list_item_not_null(cell: Any, mostly_threshold: float | int = 1) -> bool:
        """Check if a cell is a list, and if it has members of the expected type.

        Args:
            cell (Any): Individual cell to be evaluated.
            mostly_threshold (float or int): a float or int >= 0 and <=1

        Returns:
            bool: Whether or not the cell is a list with the expected members.
        """
        if not isinstance(cell, list):
            return False
        non_null_count = sum(1 for item in cell if item is not None)
        return non_null_count / len(cell) >= mostly_threshold if cell else False


# This class defines the Expectation itself
class ExpectColumnListIndividualItemNotNull(ColumnMapExpectation):
    """Expect the list in column values to be not null based on mostly parameter"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [["ab", "bc", "cd"], ["de", "ef", "fg"]],
                "b": [[None, None, "ab"], ["ab", "cd", "ef"]],
                "c": [[None, "", "ab"], [None, "", "ab"]],
                "d": [[], ["ab"]]
            },
            "tests": [
                # should pass because column "a" does not contain null 
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "mostly_threshold": 1},
                    "out": {"success": True},
                },
                # should fail because column "b" contains one row that has more than 10% of null
                {
                    "title": "negative_test_with_some_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "b", "mostly_threshold": 0.9},
                    "out": {"success": False},
                },
                # should fail because column "c" contains two rows that have more than 10% of null
                {
                    "title": "negative_test_with_empty_string",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "mostly_threshold": 0.9},
                    "out": {"success": False},
                },
                # should pass because column "c" contains both rows have less than 10% of null
                {
                    "title": "negative_test_with_empty_string",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "mostly_threshold": 0.1},
                    "out": {"success": True},
                },
                # should fail because one row in column "d" is empty
                {
                    "title": "negative_test_with_empty_string",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "d", "mostly_threshold": 0.5},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_null"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly_threshold",)

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
    ExpectColumnListIndividualItemNotNull().print_diagnostic_checklist()