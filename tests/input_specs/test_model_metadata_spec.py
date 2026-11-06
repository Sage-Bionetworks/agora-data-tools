import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
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


def test_valid_frame_passes():
    validate_inputs({"model_metadata": valid_frame()})


def test_blank_model_fails_not_empty():
    df = valid_frame()
    df["model"] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match="'model_metadata', column 'model'.*not_empty",
    ):
        validate_inputs({"model_metadata": df})


def test_duplicate_model_fails_unique():
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    with pytest.raises(
        ADTDataValidationError,
        match="'model_metadata', column 'model'.*unique",
    ):
        validate_inputs({"model_metadata": df})
