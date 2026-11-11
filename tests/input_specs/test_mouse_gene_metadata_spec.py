"""Tests for the mouse_gene_metadata input spec."""

import numpy as np
import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed mouse gene metadata row."""
    return pd.DataFrame(
        {
            "ensembl_gene_id": ["ENSMUSG00000000001"],
            "gene_symbol": ["Gnai3"],
            "alias": ["Gnai-3"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"mouse_gene_metadata": valid_frame()})


@pytest.mark.parametrize("blank", ["", None, np.nan])
def test_blank_ensembl_gene_id_fails_not_empty(blank: str | float | None) -> None:
    """A blank ensembl_gene_id is reported as not_empty."""
    df = valid_frame()
    df["ensembl_gene_id"] = [blank]
    with pytest.raises(
        ADTDataValidationError,
        match="'mouse_gene_metadata', column 'ensembl_gene_id'.*not_empty",
    ):
        validate_inputs({"mouse_gene_metadata": df})


def test_human_ensembl_id_is_allowed() -> None:
    """A human ENSG id is accepted by the ENS(MUS)?G regex.

    A mouse ENSMUSG id is already covered by test_valid_frame_passes.
    """
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSG00000142192"]
    validate_inputs({"mouse_gene_metadata": df})


def test_non_ensembl_id_fails_regex() -> None:
    """A transcript id fails the gene-id regex."""
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSMUST00000000001"]
    with pytest.raises(
        ADTDataValidationError,
        match="'mouse_gene_metadata', column 'ensembl_gene_id'.*matches_regex",
    ):
        validate_inputs({"mouse_gene_metadata": df})
