"""Input spec for biodom_genes_mm. Read by rna_de_aggregate and protein_de_aggregate."""

from agoradatatools.etl.validation_utils import InputSpec

COLUMNS = [
    "biodomain",
    "abbr",
    "label",
    "color",
    "go_id",
    "goterm_name",
    "n_symbol",
    "symbol",
    "ensembl_id",
]

SPEC = InputSpec(
    required_columns=COLUMNS,
)
