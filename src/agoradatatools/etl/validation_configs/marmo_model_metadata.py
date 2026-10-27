"""Input spec for marmo_model_metadata.

Read by transform_marmo_details. model_type and study_synid may be blank; they
become None in the JSON. ensembl_gene_id must be a Callithrix jacchus Ensembl id.
"""

from agoradatatools.etl.validation_utils import (
    InputSpec,
    MatchesRegexRule,
    NotEmptyRule,
    validate_one_to_one_mapping,
)

COLUMNS = [
    "model",
    "model_type",
    "study_synid",
    "modified_gene",
    "ensembl_gene_id",
    "allele_type",
]

RULES = {
    "model": [NotEmptyRule()],
    "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(r"^ENSCJAG\d+$")],
}

CHECKS = [
    lambda df: validate_one_to_one_mapping(df, "model", "model_type"),
    lambda df: validate_one_to_one_mapping(df, "model", "study_synid"),
]

SPEC = InputSpec(
    required_columns=COLUMNS,
    column_rules=RULES,
    checks=CHECKS,
)
