import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "individualid": ["12345"],
            "sex": ["female"],
            "agedeath": ["12 months"],
            "genotype": ["LOAD2_carrier"],
            "tissue": ["hippocampus"],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"load2_harmonized_metadata": valid_frame()})


@pytest.mark.parametrize("column", ["individualid", "genotype"])
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'load2_harmonized_metadata', column '{column}'.*not_empty",
    ):
        validate_inputs({"load2_harmonized_metadata": df})
