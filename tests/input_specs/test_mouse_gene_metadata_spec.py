import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ensembl_gene_id": ["ENSMUSG00000000001"],
            "gene_symbol": ["Gnai3"],
            "alias": ["Gnai-3"],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"mouse_gene_metadata": valid_frame()})


def test_blank_ensembl_gene_id_fails_not_empty():
    df = valid_frame()
    df["ensembl_gene_id"] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match="'mouse_gene_metadata', column 'ensembl_gene_id'.*not_empty",
    ):
        validate_inputs({"mouse_gene_metadata": df})


def test_human_ensembl_id_is_allowed():
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSG00000142192"]
    validate_inputs({"mouse_gene_metadata": df})


def test_non_ensembl_id_fails_regex():
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSMUST00000000001"]
    with pytest.raises(
        ADTDataValidationError,
        match="'mouse_gene_metadata', column 'ensembl_gene_id'.*matches_regex",
    ):
        validate_inputs({"mouse_gene_metadata": df})
