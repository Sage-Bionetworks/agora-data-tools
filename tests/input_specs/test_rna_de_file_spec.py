"""Tests for the rna_de_file input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed RNA differential-expression row."""
    return pd.DataFrame(
        {
            "ensembl_gene_id": ["ENSMUSG00000000001"],
            "log2foldchange": [0.5],
            "padj": [0.01],
            "model": ["5xFAD"],
            "case": ["5xFAD_carrier"],
            "control": ["5xFAD_noncarrier"],
            "age": ["4 months"],
            "sex": ["female"],
            "tissue": ["hippocampus"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"rna_de_file": valid_frame()})


def test_empty_file_with_headers_fails() -> None:
    """A header-only file is rejected as empty."""
    with pytest.raises(
        ADTDataValidationError, match="In dataset 'rna_de_file': is empty"
    ):
        validate_inputs({"rna_de_file": valid_frame().iloc[:0]})


@pytest.mark.parametrize("bad_age", ["6months", "1 year", ""])
def test_age_must_match_n_months(bad_age: str) -> None:
    """An age that is not N months fails the age regex."""
    df = valid_frame()
    df["age"] = [bad_age]
    with pytest.raises(ADTDataValidationError, match="'rna_de_file', column 'age'"):
        validate_inputs({"rna_de_file": df})
