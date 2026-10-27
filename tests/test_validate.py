import pkgutil

import pandas as pd
import pytest

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl import validate, validation_configs
from agoradatatools.etl.validate import INPUT_SPECS, load_input_specs, validate_inputs
from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

# Columns whose rules need a value other than x so a valid_frame passes.
_VALID_FRAME_OVERRIDES = {
    "age": "0 months",
    "protein_id": "x|y",
}


def valid_frame(name: str) -> pd.DataFrame:
    """One well-formed row for the named spec, populating every required column."""
    return pd.DataFrame(
        {
            column: [_VALID_FRAME_OVERRIDES.get(column, "x")]
            for column in INPUT_SPECS[name].required_columns
        }
    )


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

    def test_unregistered_input_fails(self):
        with pytest.raises(
            ADTDataValidationError,
            match="No input validation spec registered for not_registered",
        ):
            validate_inputs({"not_registered": pd.DataFrame({"a": [None]})})

    def test_unknown_schema_fails(self):
        with pytest.raises(
            ADTDataValidationError,
            match="No input validation spec registered for jax_5xFAD_de "
            r"\(schema not_a_spec\)",
        ):
            validate_inputs(
                {"jax_5xFAD_de": pd.DataFrame({"a": [1]})},
                schemas={"jax_5xFAD_de": "not_a_spec"},
            )

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
    def test_every_not_empty_rule_fails_a_blank_value(self, name, column):
        df = valid_frame(name)
        df[column] = [""]
        with pytest.raises(
            ADTDataValidationError, match=f"'{name}', column '{column}'.*not_empty"
        ):
            validate_inputs({name: df})

    def test_schema_lookup_uses_shared_spec(self):
        df = valid_frame("rna_de_file").drop(columns=["ensembl_gene_id"])
        with pytest.raises(
            ADTDataValidationError,
            match="Missing required columns in jax_5xFAD_de.*ensembl_gene_id",
        ):
            validate_inputs(
                {"jax_5xFAD_de": df},
                schemas={"jax_5xFAD_de": "rna_de_file"},
            )

    def test_name_only_lookup_still_works_for_unique_files(self):
        validate_inputs({"genotype_label_map": valid_frame("genotype_label_map")})

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


class TestRnaDeFileSpec:
    def test_empty_file_with_headers_fails(self):
        df = valid_frame("rna_de_file").iloc[:0]
        with pytest.raises(
            ADTDataValidationError, match="In dataset 'rna_de_file': is empty"
        ):
            validate_inputs({"rna_de_file": df})

    def test_age_must_match_n_months(self):
        df = valid_frame("rna_de_file")
        df["age"] = ["6months"]
        with pytest.raises(
            ADTDataValidationError, match="'rna_de_file', column 'age'.*matches_regex"
        ):
            validate_inputs({"rna_de_file": df})


class TestRnaNeFileSpec:
    def test_empty_file_with_headers_fails(self):
        df = valid_frame("rna_ne_file").iloc[:0]
        with pytest.raises(
            ADTDataValidationError, match="In dataset 'rna_ne_file': is empty"
        ):
            validate_inputs({"rna_ne_file": df})

    def test_age_must_match_n_months(self):
        df = valid_frame("rna_ne_file")
        df["age"] = ["1 year"]
        with pytest.raises(
            ADTDataValidationError, match="'rna_ne_file', column 'age'.*matches_regex"
        ):
            validate_inputs({"rna_ne_file": df})


class TestProteinDeFileSpec:
    def test_empty_file_with_headers_fails(self):
        df = valid_frame("protein_de_file").iloc[:0]
        with pytest.raises(
            ADTDataValidationError, match="In dataset 'protein_de_file': is empty"
        ):
            validate_inputs({"protein_de_file": df})

    def test_protein_id_without_a_pipe_fails(self):
        df = valid_frame("protein_de_file")
        df["protein_id"] = ["P27144"]
        with pytest.raises(
            ADTDataValidationError,
            match="'protein_de_file', column 'protein_id'.*contains_substring",
        ):
            validate_inputs({"protein_de_file": df})

    def test_blank_log2foldchange_fails(self):
        df = valid_frame("protein_de_file")
        df["log2foldchange"] = [None]
        with pytest.raises(
            ADTDataValidationError,
            match="'protein_de_file', column 'log2foldchange'.*not_empty",
        ):
            validate_inputs({"protein_de_file": df})


class TestProteinNeFileSpec:
    def test_empty_file_with_headers_fails(self):
        df = valid_frame("protein_ne_file").iloc[:0]
        with pytest.raises(
            ADTDataValidationError, match="In dataset 'protein_ne_file': is empty"
        ):
            validate_inputs({"protein_ne_file": df})

    def test_missing_individualid_fails(self):
        df = valid_frame("protein_ne_file").drop(columns=["individualid"])
        with pytest.raises(
            ADTDataValidationError,
            match="Missing required columns in protein_ne_file.*individualid",
        ):
            validate_inputs({"protein_ne_file": df})
