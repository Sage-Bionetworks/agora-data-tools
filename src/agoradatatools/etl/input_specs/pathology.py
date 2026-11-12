"""Input spec for pathology.

Read by model_details and immunohisto_transform. Those rename model to name,
type to evidence_type, measurement to value, and age_death to age; the spec
uses the source names.
"""

from agoradatatools.etl.validation_utils import InputSpec

COLUMNS = [
    "model",
    "type",
    "measurement",
    "units",
    "age_death",
    "tissue",
    "sex",
    "genotype",
    "individual_id",
]

SPEC = InputSpec(
    required_columns=COLUMNS,
)
