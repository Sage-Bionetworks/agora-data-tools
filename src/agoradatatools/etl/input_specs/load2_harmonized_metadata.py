"""Input spec for load2_harmonized_metadata.

Read by transform_protein_de_individual. A single named file, so the spec key
is the file name and the YAML anchor needs no schema field.
"""

from agoradatatools.etl.validation_utils import InputSpec, NotEmptyRule

COLUMNS = [
    "individualid",
    "sex",
    "agedeath",
    "genotype",
    "tissue",
]

RULES = {
    "individualid": [NotEmptyRule()],
    "genotype": [NotEmptyRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
