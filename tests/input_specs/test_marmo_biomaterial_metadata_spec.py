import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "biomaterialid": ["BM001", "BM002"],
            "collectionage": [18, 24],
            "collectionageunits": ["months", "months"],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"marmo_biomaterial_metadata": valid_frame()})


def test_blank_collectionageunits_is_allowed():
    df = valid_frame()
    df["collectionageunits"] = ["months", ""]
    validate_inputs({"marmo_biomaterial_metadata": df})


def test_duplicate_biomaterialid_fails():
    df = valid_frame()
    df["biomaterialid"] = ["BM001", "BM001"]
    with pytest.raises(
        ADTDataValidationError,
        match="'marmo_biomaterial_metadata', column 'biomaterialid'.*unique",
    ):
        validate_inputs({"marmo_biomaterial_metadata": df})


@pytest.mark.parametrize(
    "bad_value,rule", [("eighteen", "numeric"), (-6, "non_negative")]
)
def test_bad_collectionage_fails(bad_value, rule):
    df = valid_frame()
    df["collectionage"] = [bad_value, 24]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_biomaterial_metadata', column 'collectionage'.*{rule}",
    ):
        validate_inputs({"marmo_biomaterial_metadata": df})
