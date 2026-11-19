"""Input spec for marmo_individual_metadata.

Read by transform_marmo_details.
"""

from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

COLUMNS = [
    "individualid",
    "genotype",
    "sex",
]

RULES = {
    "individualid": [NotEmptyRule()],
    "genotype": [NotEmptyRule()],
    "sex": [NotEmptyRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
