import pkgutil

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl import validate, validation_configs
from agoradatatools.etl.validate import INPUT_SPECS, load_input_specs, validate_inputs
from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

# Cells that cannot be the generic placeholder "x" because a spec rule requires a
# shaped value (Ensembl prefix, numeric). Unlisted cells stay "x".
VALID_CELL_OVERRIDES = {
    "marmo_model_metadata": {"ensembl_gene_id": "ENSCJAG00000000001"},
    "marmo_genotype_label_map": {"result_order": 0},
    "marmo_biomarker_measure_info": {"display_order": 0},
    "marmo_biomaterial_metadata": {"collectionage": 0},
}


def valid_frame(name: str) -> pd.DataFrame:
    """One well-formed row for the named spec, populating every required column."""
    row = {column: ["x"] for column in INPUT_SPECS[name].required_columns}
    for column, value in VALID_CELL_OVERRIDES.get(name, {}).items():
        row[column] = [value]
    return pd.DataFrame(row)


class TestLoadInputSpecs:
    def test_every_config_module_is_registered_under_its_own_name(self):
        module_names = {
            info.name for info in pkgutil.iter_modules(validation_configs.__path__)
        }
        assert module_names
        assert set(load_input_specs()) == module_names

    def test_every_spec_is_an_input_spec_with_required_columns(self):
        for name, spec in load_input_specs().items():
            assert isinstance(spec, InputSpec), name
            assert spec.required_columns, name
            assert set(spec.column_rules) <= set(spec.required_columns), name


class TestValidateInputs:
    def test_empty_datasets_pass(self):
        validate_inputs({})

    def test_unregistered_input_passes_through(self, caplog):
        with caplog.at_level("INFO"):
            validate_inputs({"not_registered": pd.DataFrame({"a": [None]})})
        assert "No input validation spec registered for not_registered" in caplog.text

    @pytest.mark.parametrize("name", sorted(INPUT_SPECS))
    def test_every_registered_spec_passes_a_valid_frame(self, name):
        validate_inputs({name: valid_frame(name)})

    @pytest.mark.parametrize("name", sorted(INPUT_SPECS))
    def test_every_registered_spec_fails_a_missing_column(self, name):
        missing = INPUT_SPECS[name].required_columns[0]
        df = valid_frame(name).drop(columns=[missing])
        with pytest.raises(
            ADTDataValidationError,
            match=f"Missing required columns in {name}.*{missing}",
        ):
            validate_inputs({name: df})

    @pytest.mark.parametrize(
        "name,column",
        [
            (name, column)
            for name in sorted(INPUT_SPECS)
            for column, rules in INPUT_SPECS[name].column_rules.items()
            if any(isinstance(rule, NotEmptyRule) for rule in rules)
        ],
    )
    def test_every_column_rule_fails_a_blank_value(self, name, column):
        df = valid_frame(name)
        df[column] = [""]
        with pytest.raises(
            ADTDataValidationError, match=f"'{name}', column '{column}'.*not_empty"
        ):
            validate_inputs({name: df})

    def test_missing_column_is_reported_alone(self):
        # Rules and checks would KeyError on the missing column, so only the
        # missing-column message is reported for that input.
        df = valid_frame("genotype_label_map").drop(columns=["model_group"])
        with pytest.raises(ADTDataValidationError) as excinfo:
            validate_inputs({"genotype_label_map": df})
        assert str(excinfo.value).count("\n") == 1

    def test_violations_from_several_inputs_are_aggregated(self):
        label_map = valid_frame("genotype_label_map")
        label_map["genotype"] = [""]
        gene_metadata = valid_frame("mouse_gene_metadata").drop(columns=["alias"])
        datasets = {
            "genotype_label_map": label_map,
            "mouse_gene_metadata": gene_metadata,
            "biodom_genes_mm": valid_frame("biodom_genes_mm"),
        }
        with pytest.raises(ADTDataValidationError) as excinfo:
            validate_inputs(datasets)
        message = str(excinfo.value)
        assert "'genotype_label_map', column 'genotype'" in message
        assert (
            "Missing required columns in mouse_gene_metadata dataset: alias" in message
        )

    def test_check_failure_names_the_input(self, monkeypatch):
        def failing_check(df):
            raise ValueError("boom")

        monkeypatch.setitem(
            validate.INPUT_SPECS,
            "custom",
            InputSpec(required_columns=["a"], checks=(failing_check,)),
        )
        datasets = {"custom": pd.DataFrame({"a": [1]})}
        with pytest.raises(ADTDataValidationError, match="In dataset 'custom': boom"):
            validate_inputs(datasets)


class TestGenotypeLabelMapSpec:
    @pytest.mark.parametrize("column", ["model_group", "model_type"])
    def test_model_must_map_to_one_value(self, column):
        df = pd.concat([valid_frame("genotype_label_map")] * 2, ignore_index=True)
        df[column] = ["a", "b"]
        with pytest.raises(
            ADTDataValidationError,
            match=f"In dataset 'genotype_label_map'.*multiple {column} values",
        ):
            validate_inputs({"genotype_label_map": df})

    def test_shared_control_genotype_across_models_is_allowed(self):
        df = pd.concat([valid_frame("genotype_label_map")] * 2, ignore_index=True)
        df["model"] = ["m1", "m2"]
        df["model_group"] = ["g1", "g2"]
        validate_inputs({"genotype_label_map": df})


class TestMarmoModelMetadataSpec:
    @pytest.mark.parametrize("column", ["model_type", "study_synid"])
    def test_model_must_map_to_one_value(self, column):
        df = pd.concat([valid_frame("marmo_model_metadata")] * 2, ignore_index=True)
        df[column] = ["a", "b"]
        with pytest.raises(
            ADTDataValidationError,
            match=f"In dataset 'marmo_model_metadata'.*multiple {column} values",
        ):
            validate_inputs({"marmo_model_metadata": df})

    def test_ensembl_gene_id_must_match_regex(self):
        df = valid_frame("marmo_model_metadata")
        df["ensembl_gene_id"] = ["ENSG00000000001"]
        with pytest.raises(
            ADTDataValidationError,
            match=r"'marmo_model_metadata', column 'ensembl_gene_id'.*matches_regex",
        ):
            validate_inputs({"marmo_model_metadata": df})


class TestMarmoGenotypeLabelMapSpec:
    def test_duplicate_model_genotype_raises(self):
        df = pd.concat([valid_frame("marmo_genotype_label_map")] * 2, ignore_index=True)
        with pytest.raises(
            ADTDataValidationError,
            match=r"duplicate \(model, genotype\)",
        ):
            validate_inputs({"marmo_genotype_label_map": df})


class TestMarmoBiomaterialMetadataSpec:
    def test_duplicate_biomaterialid_raises(self):
        df = pd.concat(
            [valid_frame("marmo_biomaterial_metadata")] * 2, ignore_index=True
        )
        with pytest.raises(
            ADTDataValidationError,
            match=r"'marmo_biomaterial_metadata', column 'biomaterialid'.*unique",
        ):
            validate_inputs({"marmo_biomaterial_metadata": df})


class TestMarmoNumericRules:
    @pytest.mark.parametrize(
        "name,column,bad_value,rule",
        [
            ("marmo_biomaterial_metadata", "collectionage", "eighteen", "numeric"),
            ("marmo_biomaterial_metadata", "collectionage", -6, "non_negative"),
            ("marmo_biomarker_measure_info", "display_order", "third", "numeric"),
            ("marmo_biomarker_measure_info", "display_order", -1, "non_negative"),
            ("marmo_genotype_label_map", "result_order", "first", "numeric"),
            ("marmo_genotype_label_map", "result_order", -1, "non_negative"),
        ],
        ids=[
            "non-numeric collection age",
            "negative collection age",
            "non-numeric display order",
            "negative display order",
            "non-numeric result order",
            "negative result order",
        ],
    )
    def test_numeric_rules_reject_bad_values(self, name, column, bad_value, rule):
        df = valid_frame(name)
        df[column] = [bad_value]
        with pytest.raises(
            ADTDataValidationError, match=f"column '{column}'.*rule '{rule}'"
        ):
            validate_inputs({name: df})
