"""Tests for the model_metadata input spec."""

import numpy as np
import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    """One well-formed model metadata row."""
    return pd.DataFrame(
        {
            "model": ["3xTg-AD"],
            "matched_controls": ["B6129"],
            "model_type": ["Familial AD"],
            "contributing_group": ["UCI"],
            "study_synid": ["syn22964685"],
            "rrid": ["MMRRC_034830-JAX"],
            "jax_id": ["004807"],
            "alzforum_id": ["3xtg"],
            "genotype": ["B6;129-Tg"],
            "aliases": ["3xTg"],
            "transcriptomics": [True],
            "transcriptomics_url_categories_value": ["RNA"],
            "transcriptomics_url_models_value": [""],
            "disease_correlation": [False],
            "pathology": [True],
            "biomarkers": [True],
            "proteomics": [False],
            "proteomics_url_categories_value": [""],
            "proteomics_url_models_value": [""],
        }
    )


def test_valid_frame_passes() -> None:
    """A well-formed row satisfies the spec."""
    validate_inputs({"model_metadata": valid_frame()})


@pytest.mark.parametrize("blank", ["", None, np.nan])
def test_blank_model_fails_not_empty(blank: str | float | None) -> None:
    """A blank model is reported as not_empty."""
    df = valid_frame()
    df["model"] = [blank]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_metadata', column 'model'.*not_empty",
    ):
        validate_inputs({"model_metadata": df})


def test_duplicate_model_fails_unique() -> None:
    """A repeated model name is reported as unique."""
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    with pytest.raises(
        ADTDataValidationError,
        match="'model_metadata', column 'model'.*unique",
    ):
        validate_inputs({"model_metadata": df})
