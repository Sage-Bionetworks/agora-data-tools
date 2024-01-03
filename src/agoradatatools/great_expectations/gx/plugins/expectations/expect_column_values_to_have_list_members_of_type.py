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
class ColumnValuesListMembersOfType(ColumnMapMetricProvider):
    """Class definition for list member type checking metric."""

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.list_members.of_type"
    condition_value_keys = ("member_type",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.core.series.Series, member_type: str, **kwargs) -> bool:
        """Core logic for list member checking metric on a
        pandas execution engine.

        Args:
            column (pd.core.series.Series): Pandas column to be evaluated.
            member_type (str): Expected list member type represented as a string.
        Returns:
            bool: Whether or not the column values have the expected list members.
        """
        return column.apply(lambda x: cls._check_list_member_type(x, member_type))

    @staticmethod
    def _check_list_member_type(cell: Any, member_type: str) -> bool:
        """Check if a cell is a list, and if it has members of the expected type.

        Args:
            cell (Any): Individual cell to be evaluated.
            member_type (str): Expected list member type represented as a string.

        Returns:
            bool: Whether or not the cell is a list with the expected members.
        """
        type_map = {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "list": list,
            "dict": dict,
            "set": set,
        }
        python_type = type_map.get(member_type)
        if not python_type:
            raise ValueError(f"member_type must be one of: {list(type_map.keys())}")

        if not isinstance(cell, list):
            return False
        if not all(isinstance(item, python_type) for item in cell):
            return False
        return True


# This class defines the Expectation itself
class ExpectColumnValuesToHaveListMembersOfType(ColumnMapExpectation):
    """Expect the list in column values to have members of a certain type."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": [["a", "b", "a", "b"]],
            },
            "tests": [
                {
                    "title": "positive_test_with_strings",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "member_type": "str"},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_strings",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "member_type": "int"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.list_members.of_type"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("member_type",)

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
    ExpectColumnValuesToHaveListMembersOfType().print_diagnostic_checklist()
