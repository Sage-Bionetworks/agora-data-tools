"""Tests for the input validation stage."""

from pathlib import Path

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl import input_specs
from agoradatatools.etl.validate import (
    INPUT_SPECS,
    _validate_one,
    load_input_specs,
    validate_inputs,
)
from agoradatatools.etl.validation_utils import (
    ColumnRule,
    InputSpec,
    NotEmptyRule,
    NumericRule,
    validate_one_to_one_mapping,
)


def _require_one_col_b_per_col_a(df: pd.DataFrame) -> None:
    """Reject a frame where one colA value maps to more than one colB."""
    validate_one_to_one_mapping(df, "colA", "colB")


# Injected into unit tests so they do not depend on the real registry.
TEST_SPECS: dict[str, InputSpec] = {
    "data_1": InputSpec(
        required_columns=["colA", "colB"],
        column_rules={
            "colA": [NotEmptyRule()],
            "colB": [NotEmptyRule(), NumericRule()],
        },
        checks=[_require_one_col_b_per_col_a],
    ),
    "data_2": InputSpec(required_columns=["colC", "colD"]),
}


class TestLoadInputSpecs:
    """Tests for load_input_specs and the specs it registers."""

    def test_every_module_is_registered_under_its_own_name(self) -> None:
        """Package .py files (except __init__) match the keys load_input_specs returns."""
        package_dir = Path(input_specs.__file__).parent
        module_names = {
            path.stem for path in package_dir.glob("*.py") if path.name != "__init__.py"
        }
        assert module_names
        assert set(load_input_specs()) == module_names

    def test_every_spec_is_an_input_spec_with_required_columns(self) -> None:
        """Every registered SPEC is an InputSpec whose required_columns is a non-empty list."""
        for name, spec in load_input_specs().items():
            assert isinstance(spec, InputSpec), name
            assert isinstance(spec.required_columns, list), name
            assert spec.required_columns, name

    def test_column_rules_only_cover_required_columns(self) -> None:
        """Rule keys are a subset of required_columns, and every rule is a ColumnRule."""
        for name, spec in load_input_specs().items():
            assert set(spec.column_rules) <= set(spec.required_columns), name
            for column, rules in spec.column_rules.items():
                for rule in rules:
                    assert isinstance(rule, ColumnRule), (name, column)

    def test_checks_are_callable(self) -> None:
        """Every whole-frame check on a registered spec is callable."""
        for name, spec in load_input_specs().items():
            for check_fn in spec.checks:
                assert callable(check_fn), name

    def test_every_spec_has_a_dedicated_test_file(self) -> None:
        """Each registered spec has a matching tests/input_specs/test_<name>_spec.py."""
        spec_test_dir = Path(__file__).parent / "input_specs"
        for name in INPUT_SPECS:
            path = spec_test_dir / f"test_{name}_spec.py"
            assert path.is_file(), path


class TestValidateOne:
    """Tests for validating one frame against one spec."""

    def test_missing_column_is_reported_alone(self) -> None:
        """A missing required column is reported without also running rules or checks."""
        df = pd.DataFrame({"colB": [""]})
        messages = _validate_one("data_1", df, TEST_SPECS["data_1"])
        assert len(messages) == 1
        assert "Missing required columns in data_1" in messages[0]
        assert "colA" in messages[0]
        assert "not_empty" not in messages[0]

    def test_failing_check_function_reports_error(self) -> None:
        """A check that raises ValueError is reported with the dataset name."""

        def failing_check(df: pd.DataFrame) -> None:
            raise ValueError("boom")

        spec = InputSpec(required_columns=["a"], checks=(failing_check,))
        messages = _validate_one("custom", pd.DataFrame({"a": [1]}), spec)
        assert messages == ["In dataset 'custom': boom"]

    def test_rule_and_check_violations_are_collected_together(self) -> None:
        """When columns exist, rule and check failures are both returned."""
        df = pd.DataFrame({"colA": ["x", "x"], "colB": [1, 2]})
        messages = _validate_one("data_1", df, TEST_SPECS["data_1"])
        joined = "\n".join(messages)
        assert "not_empty" not in joined
        assert "multiple colB values" in joined


class TestValidateInputs:
    """Tests for validate_inputs across a dict of frames."""

    def test_empty_datasets_pass(self) -> None:
        """An empty datasets dict is a no-op."""
        validate_inputs({}, TEST_SPECS)

    def test_unregistered_input_passes_through(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A name with no spec is logged and skipped."""
        with caplog.at_level("INFO"):
            validate_inputs({"not_registered": pd.DataFrame({"a": [None]})}, TEST_SPECS)
        assert "No input validation spec registered for not_registered" in caplog.text

    def test_valid_frames_pass(self) -> None:
        """Frames that satisfy TEST_SPECS raise nothing."""
        validate_inputs(
            {
                "data_1": pd.DataFrame({"colA": ["x"], "colB": [1]}),
                "data_2": pd.DataFrame({"colC": ["c"], "colD": ["d"]}),
            },
            TEST_SPECS,
        )

    def test_violations_from_several_inputs_are_aggregated(self) -> None:
        """One raise lists every input's violations."""
        datasets = {
            "data_1": pd.DataFrame({"colA": [""], "colB": [1]}),
            "data_2": pd.DataFrame({"colC": [1]}),
        }
        with pytest.raises(ADTDataValidationError) as excinfo:
            validate_inputs(datasets, TEST_SPECS)
        message = str(excinfo.value)
        assert "'data_1', column 'colA'" in message
        assert "not_empty" in message
        assert "Missing required columns in data_2 dataset: colD" in message

    def test_default_registry_is_input_specs(self) -> None:
        """Omitting specs uses the real INPUT_SPECS registry."""
        name = next(iter(INPUT_SPECS))
        with pytest.raises(ADTDataValidationError, match=name):
            validate_inputs({name: pd.DataFrame()})

    @pytest.mark.parametrize(
        "name, missing_column",
        [
            (name, column)
            for name in sorted(INPUT_SPECS)
            for column in INPUT_SPECS[name].required_columns
        ],
    )
    def test_every_required_column_is_reported_when_missing(
        self, name: str, missing_column: str
    ) -> None:
        """Dropping any required column of any real spec fails with that column named."""
        df = pd.DataFrame(
            {
                column: ["x"]
                for column in INPUT_SPECS[name].required_columns
                if column != missing_column
            }
        )
        with pytest.raises(
            ADTDataValidationError,
            match=f"Missing required columns in {name}.*{missing_column}",
        ):
            validate_inputs({name: df})
