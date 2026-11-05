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


def test_valid_frame_passes():
    validate_inputs({"model_genetic_modifications": valid_frame()})


def test_mouse_only_allele_with_null_human_id_passes():
    df = valid_frame()
    df["human_gene_symbol"] = [None]
    df["human_ensembl_id"] = [None]
    validate_inputs({"model_genetic_modifications": df})


@pytest.mark.parametrize("column", NONEMPTY_COLUMNS)
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'model_genetic_modifications', column '{column}'.*not_empty",
    ):
        validate_inputs({"model_genetic_modifications": df})


def test_human_ensembl_id_must_match_ensg():
    df = valid_frame()
    df["human_ensembl_id"] = ["ENSMUSG00000022892"]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_genetic_modifications', column 'human_ensembl_id'.*matches_regex",
    ):
        validate_inputs({"model_genetic_modifications": df})


def test_mouse_ensembl_id_must_match_ensmusg():
    df = valid_frame()
    df["mouse_ensembl_id"] = ["ENSG00000142192"]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_genetic_modifications', column 'mouse_ensembl_id'.*matches_regex",
    ):
        validate_inputs({"model_genetic_modifications": df})
