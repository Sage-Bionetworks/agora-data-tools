"""Input spec for protein_de_file.

Shared by the eight LOAD2 protein differential-expression files read by
transform_protein_de_aggregate. protein_id is gene_symbol|uniprotid; the pipe
is what canonical_accession splits on. Source files call the fold-change
column diff; the dataset column_rename maps that to log2foldchange after
this stage.
"""

from agoradatatools.etl.validation_utils import (
    ContainsSubstringRule,
    InputSpec,
    NotEmptyRule,
    reject_empty_dataframe,
)

COLUMNS = [
    "protein_id",
    "diff",
    "padj",
]

RULES = {
    "protein_id": [NotEmptyRule(), ContainsSubstringRule("|")],
    "diff": [NotEmptyRule()],
}

CHECKS = [reject_empty_dataframe]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
