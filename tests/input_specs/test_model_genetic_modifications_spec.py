"""Tests for the model_genetic_modifications input spec."""

import numpy as np
import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs

NONEMPTY_COLUMNS = [
    "model",
    "mouse_gene_symbol",
    "mouse_ensembl_id",
    "allele",
    "allele_type",
    "mgi_allele_id",
]


def valid_frame() -> pd.DataFrame:
    """One well-formed row with both mouse and human identifiers."""
    return pd.DataFrame(
        {
            "model": ["3xTg-AD"],
            "mouse_gene_symbol": ["App"],
            "mouse_ensembl_id": ["ENSMUSG00000022892"],
            "allele": ["Allele1"],
            "allele_type": ["Transgenic"],
            "mgi_allele_id": ["2672831"],
            "human_gene_symbol": ["APP"],
            "human_ensembl_id": ["ENSG00000142192"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"model_genetic_modifications": valid_frame()})


def test_mouse_only_allele_with_null_human_id_passes() -> None:
    """Human identifiers may be null on a mouse-only allele."""
    df = valid_frame()
    df["human_gene_symbol"] = [None]
    df["human_ensembl_id"] = [None]
    validate_inputs({"model_genetic_modifications": df})


@pytest.mark.parametrize("column", NONEMPTY_COLUMNS)
@pytest.mark.parametrize("blank", ["", None, np.nan])
def test_blank_value_fails_not_empty(column: str, blank: str | float | None) -> None:
    """A blank in any NotEmpty column is reported as not_empty."""
    df = valid_frame()
    df[column] = [blank]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'model_genetic_modifications', column '{column}'.*not_empty",
    ):
        validate_inputs({"model_genetic_modifications": df})


def test_human_ensembl_id_must_match_ensg() -> None:
    """A mouse Ensembl id in human_ensembl_id fails the ENSG regex."""
    df = valid_frame()
    df["human_ensembl_id"] = ["ENSMUSG00000022892"]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_genetic_modifications', column 'human_ensembl_id'.*matches_regex",
    ):
        validate_inputs({"model_genetic_modifications": df})


def test_mouse_ensembl_id_must_match_ensmusg() -> None:
    """A human Ensembl id in mouse_ensembl_id fails the ENSMUSG regex."""
    df = valid_frame()
    df["mouse_ensembl_id"] = ["ENSG00000142192"]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_genetic_modifications', column 'mouse_ensembl_id'.*matches_regex",
    ):
        validate_inputs({"model_genetic_modifications": df})
