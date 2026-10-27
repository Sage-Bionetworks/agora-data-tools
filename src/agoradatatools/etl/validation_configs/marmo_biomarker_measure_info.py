"""Input spec for marmo_biomarker_measure_info.

Read by transform_marmo_details. units may be blank (the A-beta ratio has none).
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NonNegativeRule,
    NotEmptyRule,
    NumericRule,
)

COLUMNS = [
    "result_column",
    "evidence_type",
    "units",
    "display_order",
]

RULES = {
    "result_column": [NotEmptyRule()],
    "evidence_type": [NotEmptyRule()],
    "display_order": [NotEmptyRule(), NumericRule(), NonNegativeRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
