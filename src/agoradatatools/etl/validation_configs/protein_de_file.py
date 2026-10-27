"""Input spec for protein_de_file.

Shared by the eight LOAD2 protein differential-expression files read by
transform_protein_de_aggregate. protein_id is gene_symbol|uniprotid; the pipe
is what canonical_accession splits on.
"""

from agoradatatools.etl.validation_utils import (
    ContainsSubstringRule,
    InputSpec,
    NotEmptyRule,
    reject_empty_dataframe,
)

COLUMNS = [
    "protein_id",
    "log2foldchange",
    "padj",
]

RULES = {
    "protein_id": [NotEmptyRule(), ContainsSubstringRule("|")],
    "log2foldchange": [NotEmptyRule()],
}

CHECKS = [reject_empty_dataframe]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
