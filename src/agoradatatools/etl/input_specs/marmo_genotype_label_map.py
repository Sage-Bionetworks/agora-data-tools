"""Input spec for marmo_genotype_label_map.

Read by transform_marmo_details. A repeated (model, genotype) pair would multiply
measurements within a model.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NonNegativeRule,
    NotEmptyRule,
    NumericRule,
    raise_on_duplicate_keys,
)

COLUMNS = [
    "model",
    "genotype",
    "display_label",
    "result_order",
]

RULES = {
    "model": [NotEmptyRule()],
    "genotype": [NotEmptyRule()],
    "display_label": [NotEmptyRule()],
    "result_order": [NotEmptyRule(), NumericRule(), NonNegativeRule()],
}


def _unique_model_genotype(df):
    raise_on_duplicate_keys(
        df,
        ["model", "genotype"],
        "marmo_genotype_label_map has duplicate (model, genotype) rows, which would "
        "multiply measurements within a model",
    )


CHECKS = [_unique_model_genotype]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
