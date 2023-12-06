import pandas as pd
from typing import Optional, Any

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesListLengthLessThan(ColumnMapMetricProvider):
    """Class definition for list length less than checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.list_length_less_than"
    condition_value_keys = ("list_length_less_than",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls, column: pd.core.series.Series, list_length_less_than: int, **kwargs
    ) -> bool:
        """Core logic for list length checking metric on a
        pandas execution engine.

        Args:
            column (pd.core.series.Series): Pandas column to be evaluated.
            list_length_less_than (int): length the list should be less than.
        Returns:
            bool: Whether or not the column values have the expected list length.
        """
        return column.apply(
            lambda x: cls._check_list_length_is_less_than(x, list_length_less_than)
        )

    @staticmethod
    def _check_list_length_is_less_than(cell: Any, list_length_less_than: int) -> bool:
        """Check if a cell is a list, and if it has the expected length.

        Args:
            cell (Any): Individual cell to be evaluated.
            list_length_less_than (int): length the list should be less than.

        Returns:
            bool: Whether or not the cell is a list with the expected length.
        """
        if not isinstance(cell, list):
            return False
        if len(cell) >= list_length_less_than:
            return False
        return True


# This class defines the Expectation itself
class ExpectColumnValuesToHaveListLengthLessThan(ColumnMapExpectation):
    """Expect the list in column values to have a length less than a certain value."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [[1, 2, 3, 4, 5]],
            },
            "tests": [
                {
                    "title": "positive_test_with_list_length_6",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "list_length_less_than": 6},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_list_length_5",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "list_length_less_than": 4},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.list_length_less_than"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("list_length_less_than",)

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
    ExpectColumnValuesToHaveListLengthLessThan().print_diagnostic_checklist()
