"""Input spec for model_genetic_modifications.

Read by model_details, model_overview, and disease_correlation. Two of those rename
model to name and mouse_gene_symbol to modified_gene in their config; the spec uses
the source names.
"""

from agoradatatools.etl.validation_utils import InputSpec

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

SPEC = InputSpec(
    required_columns=COLUMNS,
)
