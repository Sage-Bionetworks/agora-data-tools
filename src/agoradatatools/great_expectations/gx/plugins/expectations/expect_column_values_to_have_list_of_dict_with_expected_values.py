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
class ColumnValuesListOfDictWithExpectedValues(ColumnMapMetricProvider):
    """Class definition for expecting list of dic with expected vlaues checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.list_dict_values"
    condition_value_keys = ("list_dict_values",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls, column: pd.core.series.Series, list_dict_values: dict, **kwargs
    ) -> bool:
        """Core logic for list length checking metric on a
        pandas execution engine.

        Args:
            column (pd.core.series.Series): Pandas column to be evaluated.
            list_dict_values (dict): Dictionary containing the key to check
            and a list of the values it is allowed to have.
        Returns:
            bool: Whether or not the column values have the expected list length.
        """
        return column.apply(
            lambda x: cls._check_list_of_dict_has_expected_values(x, list_dict_values)
        )

    @staticmethod
    def _check_list_of_dict_has_expected_values(
        cell: Any, list_dict_values: dict[str, list]
    ) -> bool:
        """Check if a cell is a list, and if it has the expected length.

        Args:
            cell (Any): Individual cell to be evaluated.
            list_dict_values (dict): Dictionary containing the key to check
            and a list of the values it is allowed to have.
        Returns:
            bool: Whether or not the cell is a list with the expected length.
        """
        if not isinstance(cell, list):
            return False
        if (
            not isinstance(list_dict_values, dict)
            and "key" not in list_dict_values
            and "values" not in list_dict_values
        ):
            raise ValueError(
                "list_dict_values must be a dict which contains 'key' (string) and 'values' (list)."
            )
        for item in cell:
            if not isinstance(item, dict):
                return False
            if item[list_dict_values["key"]] not in list_dict_values["values"]:
                return False
        return True


# This class defines the Expectation itself
class ExpectColumnValuesToHaveListOfDictWithExpectedValues(ColumnMapExpectation):
    """Expect the list in column values to have a length within a certain range."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [[{"a": "b"}]],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "list_dict_values": {"key": "a", "values": ["b", "c", "d"]},
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a",
                        "list_dict_values": {"key": "a", "values": ["c", "d"]},
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.list_dict_values"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("list_dict_values",)

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
    ExpectColumnValuesToHaveListOfDictWithExpectedValues().print_diagnostic_checklist()
