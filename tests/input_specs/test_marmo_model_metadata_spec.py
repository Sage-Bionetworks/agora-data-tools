"""Tests for the marmo_model_metadata input spec."""

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed marmoset model metadata row."""
    return pd.DataFrame(
        {
            "model": ["PSEN1-M146V"],
            "model_type": ["Familial AD"],
            "study_synid": ["syn12345678"],
            "modified_gene": ["PSEN1"],
            "ensembl_gene_id": ["ENSCJAG00000000001"],
            "allele_type": ["Point mutation"],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"marmo_model_metadata": valid_frame()})


def test_blank_model_type_and_study_synid_are_allowed() -> None:
    """model_type and study_synid may be blank."""
    df = valid_frame()
    df["model_type"] = [""]
    df["study_synid"] = [""]
    validate_inputs({"marmo_model_metadata": df})


@pytest.mark.parametrize("column", ["model", "ensembl_gene_id"])
def test_blank_value_fails_not_empty(column: str) -> None:
    """A blank model or ensembl_gene_id is reported as not_empty."""
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'marmo_model_metadata', column '{column}'.*not_empty",
    ):
        validate_inputs({"marmo_model_metadata": df})


def test_non_marmoset_ensembl_gene_id_fails() -> None:
    """A human ENSG id fails the marmoset ENSCJAG regex."""
    df = valid_frame()
    df["ensembl_gene_id"] = ["ENSG00000000001"]
    with pytest.raises(
        ADTDataValidationError,
        match="'marmo_model_metadata', column 'ensembl_gene_id'.*matches_regex",
    ):
        validate_inputs({"marmo_model_metadata": df})


@pytest.mark.parametrize("column", ["model_type", "study_synid"])
def test_model_must_map_to_one_value(column: str) -> None:
    """One model mapping to two values of model_type or study_synid fails."""
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    df["modified_gene"] = ["PSEN1", "APP"]
    df[column] = ["a", "b"]
    with pytest.raises(
        ADTDataValidationError,
        match=f"In dataset 'marmo_model_metadata'.*multiple {column} values",
    ):
        validate_inputs({"marmo_model_metadata": df})
