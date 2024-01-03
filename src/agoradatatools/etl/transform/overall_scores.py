import numpy as np
import pandas as pd


def transform_overall_scores(df: pd.DataFrame) -> pd.DataFrame:
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
