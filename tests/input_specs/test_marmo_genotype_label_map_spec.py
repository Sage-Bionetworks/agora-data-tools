"""Tests for the marmo_genotype_label_map input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.input_specs.marmo_genotype_label_map import COLUMNS
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed marmoset genotype label map row."""
    return pd.DataFrame(
        {
            "model": ["PSEN1-M146V"],
            "genotype": ["PSEN1-M146V_carrier"],
            "display_label": ["PSEN1-M146V"],
            "result_order": [1],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"marmo_genotype_label_map": valid_frame()})


@pytest.mark.parametrize("column", COLUMNS)
def test_blank_value_fails_not_empty(column: str) -> None:
    """A blank in any required column is reported as not_empty."""
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_genotype_label_map', column '{column}'.*not_empty",
    ):
        validate_inputs({"marmo_genotype_label_map": df})


@pytest.mark.parametrize("bad_value,rule", [("first", "numeric"), (-1, "non_negative")])
def test_bad_result_order_fails(bad_value: str | int, rule: str) -> None:
    """A non-numeric or negative result_order fails the matching rule."""
    df = valid_frame()
    df["result_order"] = [bad_value]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_genotype_label_map', column 'result_order'.*{rule}",
    ):
        validate_inputs({"marmo_genotype_label_map": df})


def test_duplicate_model_genotype_fails() -> None:
    """A repeated (model, genotype) pair is rejected."""
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    with pytest.raises(
        ADTDataValidationError,
        match="duplicate \\(model, genotype\\)",
    ):
        validate_inputs({"marmo_genotype_label_map": df})
