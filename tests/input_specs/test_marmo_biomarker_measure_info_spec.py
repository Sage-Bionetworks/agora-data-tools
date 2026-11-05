import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "result_column": ["ab42_ab40_ratio"],
            "evidence_type": ["A-beta 42/40 ratio"],
            "units": [""],
            "display_order": [3],
        }
    )


def test_valid_frame_passes():
    """units may be blank, so the valid frame leaves it empty on purpose."""
    validate_inputs({"marmo_biomarker_measure_info": valid_frame()})


@pytest.mark.parametrize("column", ["result_column", "evidence_type", "display_order"])
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_biomarker_measure_info', column '{column}'.*not_empty",
    ):
        validate_inputs({"marmo_biomarker_measure_info": df})


@pytest.mark.parametrize("bad_value,rule", [("third", "numeric"), (-1, "non_negative")])
def test_bad_display_order_fails(bad_value, rule):
    df = valid_frame()
    df["display_order"] = [bad_value]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_biomarker_measure_info', column 'display_order'.*{rule}",
    ):
        validate_inputs({"marmo_biomarker_measure_info": df})
