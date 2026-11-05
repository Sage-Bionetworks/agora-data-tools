import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """Wide proteomics file: individualid plus one abundance column per protein."""
    return pd.DataFrame(
        {
            "individualid": ["12345"],
            "Gnai3|P27144": [10.2],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"protein_ne_file": valid_frame()})


def test_empty_file_with_headers_fails():
    with pytest.raises(
        ADTDataValidationError, match="In dataset 'protein_ne_file': is empty"
    ):
        validate_inputs({"protein_ne_file": valid_frame().iloc[:0]})


def test_blank_individualid_fails_not_empty():
    df = valid_frame()
    df["individualid"] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match="'protein_ne_file', column 'individualid'.*not_empty",
    ):
        validate_inputs({"protein_ne_file": df})
