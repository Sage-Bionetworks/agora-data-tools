from typing import Dict, List

from agoradatatools.etl import transform, utils
from agoradatatools.etl.utils import (
    ColumnRule,
    check_column_rules,
    check_required_datasets_and_columns,
)

REQUIRED_INPUT: Dict[str, List[str]] = {
    "diff_exp_data": [
        "ensembl_gene_id",
        "hgnc_symbol",
        "logfc",
        "ci_l",
        "ci_r",
        "adj_p_val",
        "tissue",
        "study",
        "model",
        "sex",
    ],
}

COLUMN_RULES: Dict[str, Dict[str, List[ColumnRule]]] = {
    "diff_exp_data": {
        "ensembl_gene_id": [ColumnRule(rule="not_empty")],
    },
}


def transform_rna_distribution_data(datasets: dict):
    check_required_datasets_and_columns(datasets, REQUIRED_INPUT)
    check_column_rules(datasets, COLUMN_RULES)

    # "datasets" contains the unprocessed RNA-seq data, which needs to go
    # through the same processing as before in order to use it here.
    rna_df = transform.transform_rnaseq_differential_expression(datasets)
    rna_df = rna_df[["tissue", "model", "logfc"]]

    rna_df = utils.calculate_distribution(
        df=rna_df, grouping=["tissue", "model"], distribution_column="logfc"
    )

    # Columns must be in this order
    rna_df = rna_df[
        ["model", "tissue", "min", "max", "first_quartile", "median", "third_quartile"]
    ]

    return rna_df
