"""Input spec for immunohisto_measure_order.

Read by model_details and immunohisto_transform. read_yaml_into_df emits key
and items; those are renamed to dataset_name and evidence_type later.
"""

from agoradatatools.etl.validation_utils import InputSpec

COLUMNS = [
    "key",
    "items",
]

SPEC = InputSpec(
    required_columns=COLUMNS,
)
