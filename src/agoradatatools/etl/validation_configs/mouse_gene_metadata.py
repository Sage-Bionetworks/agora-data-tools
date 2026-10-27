"""Input spec for mouse_gene_metadata.

Read by rna_de_aggregate, rna_de_individual, protein_de_individual, and
protein_de_aggregate. alias is used only by rna_de_aggregate.
"""

from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

COLUMNS = [
    "ensembl_gene_id",
    "gene_symbol",
    "alias",
]

RULES = {
    "ensembl_gene_id": [NotEmptyRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
