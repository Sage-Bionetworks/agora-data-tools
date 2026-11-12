"""Tests for the marmo_results input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed results row with a blank QC flag."""
    return pd.DataFrame(
        {
            "biomaterialid": ["BM001"],
            "individualid": ["IND001"],
            "qc_ab": ["pass"],
            "qc_neuro": [""],
        }
    )


def test_valid_frame_passes() -> None:
    """QC flags only have to be present; blank values are interpreted by the transform."""
    validate_inputs({"marmo_results": valid_frame()})


@pytest.mark.parametrize("column", ["biomaterialid", "individualid"])
def test_blank_value_fails_not_empty(column: str) -> None:
    """A blank biomaterialid or individualid is reported as not_empty."""
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_results', column '{column}'.*not_empty",
    ):
        validate_inputs({"marmo_results": df})
