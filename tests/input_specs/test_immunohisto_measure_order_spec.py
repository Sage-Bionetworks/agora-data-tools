"""Tests for the immunohisto_measure_order input spec."""

import pandas as pd

from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed measure order row, as emitted by read_yaml_into_df."""
    return pd.DataFrame(
        {
            "key": ["pathology"],
            "items": [["Plaque Density", "Plaque Size"]],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed measure order row, as emitted by read_yaml_into_df, satisfies the spec."""
    validate_inputs({"immunohisto_measure_order": valid_frame()})
