import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "protein_id": ["Gnai3|P27144"],
            "diff": [0.5],
            "padj": [0.01],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"protein_de_file": valid_frame()})


def test_empty_file_with_headers_fails():
    with pytest.raises(
        ADTDataValidationError, match="In dataset 'protein_de_file': is empty"
    ):
        validate_inputs({"protein_de_file": valid_frame().iloc[:0]})


def test_protein_id_without_a_pipe_fails():
    df = valid_frame()
    df["protein_id"] = ["P27144"]
    with pytest.raises(
        ADTDataValidationError,
        match="'protein_de_file', column 'protein_id'.*contains_substring",
    ):
        validate_inputs({"protein_de_file": df})


@pytest.mark.parametrize("column", ["protein_id", "diff"])
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [None]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'protein_de_file', column '{column}'.*not_empty",
    ):
        validate_inputs({"protein_de_file": df})
