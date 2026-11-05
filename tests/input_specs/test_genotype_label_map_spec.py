import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl.input_specs.genotype_label_map import COLUMNS
from agoradatatools.etl.validate import validate_inputs


def valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "model": ["3xTg-AD"],
            "model_group": ["3xTg-AD"],
            "display_label": ["3xTg-AD"],
            "genotype": ["3xTg-AD_carrier"],
            "result_order": [2],
            "model_type": ["Familial AD"],
        }
    )


def test_valid_frame_passes():
    validate_inputs({"genotype_label_map": valid_frame()})


@pytest.mark.parametrize("column", COLUMNS)
def test_blank_value_fails_not_empty(column):
    df = valid_frame()
    df[column] = [""]
    with pytest.raises(
        ADTDataValidationError,
        match=f"'genotype_label_map', column '{column}'.*not_empty",
    ):
        validate_inputs({"genotype_label_map": df})


def test_non_numeric_result_order_fails():
    df = valid_frame()
    df["result_order"] = ["second"]
    with pytest.raises(
        ADTDataValidationError,
        match="'genotype_label_map', column 'result_order'.*numeric",
    ):
        validate_inputs({"genotype_label_map": df})


@pytest.mark.parametrize("column", ["model_group", "model_type"])
def test_model_must_map_to_one_value(column):
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    df["genotype"] = ["g1", "g2"]
    df[column] = ["a", "b"]
    with pytest.raises(
        ADTDataValidationError,
        match=f"In dataset 'genotype_label_map'.*multiple {column} values",
    ):
        validate_inputs({"genotype_label_map": df})


def test_duplicate_model_genotype_fails():
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    with pytest.raises(
        ADTDataValidationError,
        match="duplicate \\(model, genotype\\)",
    ):
        validate_inputs({"genotype_label_map": df})


def test_shared_control_genotype_across_models_is_allowed():
    df = pd.concat([valid_frame()] * 2, ignore_index=True)
    df["model"] = ["m1", "m2"]
    df["model_group"] = ["g1", "g2"]
    validate_inputs({"genotype_label_map": df})
