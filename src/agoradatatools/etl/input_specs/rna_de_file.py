"""Input spec for rna_de_file.

Shared by the nine RNA differential-expression files read by
transform_rna_de_aggregate. Age is validated here so create_age_entries_from_group
can sort on the leading integer.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    MatchesRegexRule,
    NotEmptyRule,
    reject_empty_dataframe,
)

COLUMNS = [
    "ensembl_gene_id",
    "log2foldchange",
    "padj",
    "model",
    "case",
    "control",
    "age",
    "sex",
    "tissue",
]

# MatchesRegexRule skips nulls, so NotEmptyRule is needed to reject a missing age.
RULES = {
    "age": [NotEmptyRule(), MatchesRegexRule(value=r"\d+ months$")],
}

CHECKS = [reject_empty_dataframe]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
