"""Input spec for model_genetic_modifications.

Read by model_details, model_overview, and disease_correlation. The spec uses the
original column names in the file rather than any renames used by the transforms.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    MatchesRegexRule,
    NotEmptyRule,
)

COLUMNS = [
    "model",
    "mouse_gene_symbol",
    "mouse_ensembl_id",
    "allele",
    "allele_type",
    "mgi_allele_id",
    "human_gene_symbol",
    "human_ensembl_id",
]

RULES = {
    "model": [NotEmptyRule()],
    "mouse_gene_symbol": [NotEmptyRule()],
    "mouse_ensembl_id": [NotEmptyRule(), MatchesRegexRule(value=r"^ENSMUSG[0-9]+$")],
    "allele": [NotEmptyRule()],
    "allele_type": [NotEmptyRule()],
    "mgi_allele_id": [NotEmptyRule()],
    "human_ensembl_id": [MatchesRegexRule(value=r"^ENSG[0-9]+$")],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
