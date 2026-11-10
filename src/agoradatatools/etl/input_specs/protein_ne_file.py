"""Input spec for protein_ne_file.

Shared by the two wide LOAD2 proteomics files read by
transform_protein_de_individual. Pipe-delimited protein columns and numeric
abundances are checked later in the melt, not here.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NotEmptyRule,
    reject_empty_dataframe,
)

COLUMNS = [
    "individualid",
]

RULES = {
    "individualid": [NotEmptyRule()],
}

CHECKS = [reject_empty_dataframe]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
