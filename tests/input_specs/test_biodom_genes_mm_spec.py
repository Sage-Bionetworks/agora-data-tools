import pandas as pd

from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "biodomain": ["Apoptosis"],
            "abbr": ["Apo"],
            "label": ["Apoptosis"],
            "color": ["#000000"],
            "go_id": ["GO:0006915"],
            "goterm_name": ["apoptotic process"],
            "n_symbol": [1],
            "symbol": ["Gnai3"],
            "ensembl_id": [None],
        }
    )


def test_valid_frame_passes():
    """A well-formed row, including a null ensembl_id, satisfies the spec."""
    validate_inputs({"biodom_genes_mm": valid_frame()})
