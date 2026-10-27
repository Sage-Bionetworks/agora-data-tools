"""Input spec for rna_ne_file.

Shared by the fifteen RNA normalized-expression files read by
transform_rna_de_individual.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    MatchesRegexRule,
    NotEmptyRule,
    reject_empty_dataframe,
)

COLUMNS = [
    "ensembl_gene_id",
    "expression",
    "model",
    "genotype",
    "age",
    "sex",
    "tissue",
    "individualid",
]

# MatchesRegexRule skips nulls, so NotEmptyRule is needed to reject a missing age.
RULES = {
    "model": [NotEmptyRule()],
    "age": [NotEmptyRule(), MatchesRegexRule(value=r"\d+ months$")],
}

CHECKS = [reject_empty_dataframe]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
