"""Tests for the disease_correlation_results input spec."""

import pandas as pd

from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed disease correlation row."""
    return pd.DataFrame(
        {
            "cluster": ["Consensus Cluster A"],
            "module": ["TCXblue"],
            "mouse_model": ["5xFAD"],
            "sex": ["female"],
            "age": ["4 months"],
            "correlation": [0.42],
            "adjusted_p_value": [0.001],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed disease correlation row satisfies the spec."""
    validate_inputs({"disease_correlation_results": valid_frame()})
