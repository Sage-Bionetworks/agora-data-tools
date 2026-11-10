"""Tests for the biomarkers input spec."""

import pandas as pd

from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed biomarkers row."""
    return pd.DataFrame(
        {
            "model": ["5xFAD"],
            "type": ["Plasma Biomarkers"],
            "measurement": [1.5],
            "units": ["pg/mL"],
            "age_death": ["4 months"],
            "tissue": ["Plasma"],
            "sex": ["female"],
            "genotype": ["5xFAD_carrier"],
            "individual_id": ["12345"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed biomarkers row satisfies the spec."""
    validate_inputs({"biomarkers": valid_frame()})
