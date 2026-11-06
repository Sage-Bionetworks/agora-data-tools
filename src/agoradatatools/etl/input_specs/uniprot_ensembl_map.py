"""Input spec for uniprot_ensembl_map. Read by protein_de_individual and protein_de_aggregate."""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    MatchesRegexRule,
    NotEmptyRule,
)

COLUMNS = [
    "uniprot_id",
    "ensembl_gene_id",
]

RULES = {
    "uniprot_id": [NotEmptyRule()],
    "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(value=r"^ENS(MUS)?G[0-9]+$")],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
