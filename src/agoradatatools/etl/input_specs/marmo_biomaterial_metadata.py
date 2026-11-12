"""Input spec for marmo_biomaterial_metadata.

Read by transform_marmo_details. collectionageunits is not checked here: unused
rows may have a blank unit, and the transform applies OneOfRule only to the
consumed subset after joins.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    NonNegativeRule,
    NumericRule,
    UniqueRule,
)

COLUMNS = [
    "biomaterialid",
    "collectionage",
    "collectionageunits",
]

RULES = {
    "biomaterialid": [UniqueRule()],
    "collectionage": [NumericRule(), NonNegativeRule()],
}

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
)
