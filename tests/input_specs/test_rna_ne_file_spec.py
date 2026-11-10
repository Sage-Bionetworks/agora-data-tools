"""Tests for the rna_ne_file input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed RNA normalized-expression row."""
    return pd.DataFrame(
        {
            "ensembl_gene_id": ["ENSMUSG00000000001"],
            "expression": [12.5],
            "model": ["5xFAD"],
            "genotype": ["5xFAD_carrier"],
            "age": ["4 months"],
            "sex": ["female"],
            "tissue": ["hippocampus"],
            "individualid": ["12345"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"rna_ne_file": valid_frame()})


def test_empty_file_with_headers_fails() -> None:
    """A header-only file is rejected as empty."""
    with pytest.raises(
        ADTDataValidationError, match="In dataset 'rna_ne_file': is empty"
    ):
        validate_inputs({"rna_ne_file": valid_frame().iloc[:0]})


def test_blank_model_fails_not_empty() -> None:
    """A blank model is reported as not_empty."""
    df = valid_frame()
    df["model"] = [""]
    with pytest.raises(
        ADTDataValidationError, match="'rna_ne_file', column 'model'.*not_empty"
    ):
        validate_inputs({"rna_ne_file": df})


@pytest.mark.parametrize("bad_age", ["6months", "1 year", ""])
def test_age_must_match_n_months(bad_age: str) -> None:
    """An age that is not N months fails the age regex."""
    df = valid_frame()
    df["age"] = [bad_age]
    with pytest.raises(ADTDataValidationError, match="'rna_ne_file', column 'age'"):
        validate_inputs({"rna_ne_file": df})
