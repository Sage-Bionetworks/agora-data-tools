"""Tests for the pathology input spec."""

import pandas as pd

from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed pathology row."""
    return pd.DataFrame(
        {
            "model": ["5xFAD"],
            "type": ["Plaque Density"],
            "measurement": [12.3],
            "units": ["percent"],
            "age_death": ["4 months"],
            "tissue": ["Cortex"],
            "sex": ["male"],
            "genotype": ["5xFAD_carrier"],
            "individual_id": ["12345"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed pathology row satisfies the spec."""
    validate_inputs({"pathology": valid_frame()})
