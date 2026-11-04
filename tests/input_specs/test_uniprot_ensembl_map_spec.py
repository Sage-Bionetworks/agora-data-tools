import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uniprot_id": ["P00001"],
            "ensembl_gene_id": ["ENSMUSG00000000001"],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"uniprot_ensembl_map": valid_frame()})


@pytest.mark.parametrize("column", ["uniprot_id", "ensembl_gene_id"])
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'uniprot_ensembl_map', column '{column}'.*not_empty",
    ):
        validate_inputs({"uniprot_ensembl_map": df})


def test_human_ensembl_id_is_allowed():
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSG00000142192"]
    validate_inputs({"uniprot_ensembl_map": df})


def test_non_ensembl_id_fails_regex():
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSMUST00000000001"]
    with pytest.raises(
        ADTDataValidationError,
        match="'uniprot_ensembl_map', column 'ensembl_gene_id'.*matches_regex",
    ):
        validate_inputs({"uniprot_ensembl_map": df})
