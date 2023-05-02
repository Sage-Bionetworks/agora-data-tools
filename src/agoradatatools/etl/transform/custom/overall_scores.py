import pandas as pd
import numpy as np


def transform_overall_scores(df: pd.DataFrame) -> pd.DataFrame:
    interesting_columns = [
        "ensg",
        "hgnc_gene_id",
        "overall",
        "geneticsscore",
        "omicsscore",
        "literaturescore",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics", "isscored_lit"]
    mapping = dict(zip(interesting_columns[3:], scored))

    for field, is_scored in mapping.items():
        df.loc[lambda row: row[is_scored] == "N", field] = np.nan

    # LiteratureScore is a string in the source file, so convert to numeric
    df["literaturescore"] = pd.to_numeric(df["literaturescore"])

    # Remove identical rows (see AG-826)
    return df[interesting_columns].drop_duplicates()
