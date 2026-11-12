"""Tests for the marmo_individual_metadata input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.input_specs.marmo_individual_metadata import COLUMNS
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed individual metadata row."""
    return pd.DataFrame(
        {
            "individualid": ["IND001"],
            "genotype": ["PSEN1-M146V_carrier"],
            "sex": ["female"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"marmo_individual_metadata": valid_frame()})


@pytest.mark.parametrize("column", COLUMNS)
def test_blank_value_fails_not_empty(column: str) -> None:
    """A blank in any required column is reported as not_empty."""
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_individual_metadata', column '{column}'.*not_empty",
    ):
        validate_inputs({"marmo_individual_metadata": df})
