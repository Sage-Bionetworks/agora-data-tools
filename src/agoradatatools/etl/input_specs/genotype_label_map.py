"""Input spec for genotype_label_map.

Consumers: rna_de_aggregate, rna_de_individual, protein_de_aggregate, and
protein_de_individual.

The spec requires every column that any consumer needs. Only the two aggregate
transforms use model_type, but it is required for all four, so the individual
transforms also reject a label map where model_type is missing or blank.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NotEmptyRule,
    NumericRule,
    raise_on_duplicate_keys,
    validate_one_to_one_mapping,
)

COLUMNS = [
    "model",
    "model_group",
    "display_label",
    "genotype",
    "result_order",
    "model_type",
]

RULES = {column: [NotEmptyRule()] for column in COLUMNS}
RULES["result_order"] = [NotEmptyRule(), NumericRule()]

CHECKS = [
    lambda df: validate_one_to_one_mapping(df, "model", "model_group"),
    lambda df: validate_one_to_one_mapping(df, "model", "model_type"),
    lambda df: raise_on_duplicate_keys(
        df,
        ["model", "genotype"],
        "genotype label map has duplicate (model, genotype) rows",
    ),
]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
