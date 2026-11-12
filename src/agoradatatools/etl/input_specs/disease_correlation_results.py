"""Input spec for disease_correlation_results. Read by disease_correlation."""

from agoradatatools.etl.validation_utils import InputSpec

COLUMNS = [
    "cluster",
    "module",
    "mouse_model",
    "sex",
    "age",
    "correlation",
    "adjusted_p_value",
]

SPEC = InputSpec(
    required_columns=COLUMNS,
)
