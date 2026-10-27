"""Input spec for model_metadata.

Read by model_details, model_overview, and disease_correlation. Two of those rename
model to name in their config; the spec uses the source name, model.
"""

from agoradatatools.etl.validation_utils import InputSpec

COLUMNS = [
    "model",
    "matched_controls",
    "model_type",
    "contributing_group",
    "study_synid",
    "rrid",
    "jax_id",
    "alzforum_id",
    "genotype",
    "aliases",
    "transcriptomics",
    "transcriptomics_url_categories_value",
    "transcriptomics_url_models_value",
    "disease_correlation",
    "pathology",
    "biomarkers",
    "proteomics",
    "proteomics_url_categories_value",
    "proteomics_url_models_value",
]

SPEC = InputSpec(
    required_columns=COLUMNS,
)
