"""Input spec for marmo_results.

Read by transform_marmo_details. qc_ab and qc_neuro must be present so a missing
flag cannot silently disable the QC filter; their values are interpreted later.
"""

from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

COLUMNS = [
    "biomaterialid",
    "individualid",
    "qc_ab",
    "qc_neuro",
]

RULES = {
    "biomaterialid": [NotEmptyRule()],
    "individualid": [NotEmptyRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
