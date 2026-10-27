"""Input spec for genotype_label_map.

Read by rna_de_aggregate, rna_de_individual, protein_de_individual, and
protein_de_aggregate. model_type is used only by the aggregate transforms but is
required for every consumer because the spec is the union.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NotEmptyRule,
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

CHECKS = [
    lambda df: validate_one_to_one_mapping(df, "model", "model_group"),
    lambda df: validate_one_to_one_mapping(df, "model", "model_type"),
]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
