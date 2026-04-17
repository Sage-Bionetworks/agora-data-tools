from typing import Dict, List

import numpy as np
import pandas as pd

from agoradatatools.etl.utils import (
    ColumnRule,
    check_column_rules,
    check_required_datasets_and_columns,
)

REQUIRED_INPUT: Dict[str, List[str]] = {
    "overall_scores": [
        "ensg",
        "hgnc_gene_id",
        "overall",
        "geneticsscore",
        "omicsscore",
        "isscored_genetics",
        "isscored_omics",
    ],
}

COLUMN_RULES: Dict[str, Dict[str, List[ColumnRule]]] = {
    "overall_scores": {
        "ensg": [ColumnRule(rule="not_empty")],
    },
}


def transform_overall_scores(df: pd.DataFrame) -> pd.DataFrame:
    check_required_datasets_and_columns({"overall_scores": df}, REQUIRED_INPUT)
    check_column_rules({"overall_scores": df}, COLUMN_RULES)

    interesting_columns = [
        "ensg",
        "hgnc_gene_id",
        "overall",
        "geneticsscore",
        "omicsscore",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics"]
    mapping = dict(zip(interesting_columns[3:], scored))

    for field, is_scored in mapping.items():
        df.loc[lambda row: row[is_scored] == "N", field] = np.nan

    # Remove identical rows (see AG-826)
    return df[interesting_columns].drop_duplicates()
