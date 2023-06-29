import numpy as np
from agoradatatools.etl import transform

def transform_rna_distribution_data(datasets: dict):
    # "datasets" contains the unprocessed RNA-seq data, which needs to go
    # through the same processing as before in order to use it here.
    rna_df = transform.transform_rnaseq_differential_expression(datasets)
    rna_df = rna_df[["tissue", "model", "logfc"]]

    rna_df = (
        rna_df.groupby(["tissue", "model"])
        .agg("describe")["logfc"]
        .reset_index()[["model", "tissue", "min", "max", "25%", "50%", "75%"]]
    )
    rna_df.rename(
        columns={"25%": "first_quartile", "50%": "median", "75%": "third_quartile"},
        inplace=True,
    )

    rna_df["IQR"] = rna_df["third_quartile"] - rna_df["first_quartile"]
    rna_df["min"] = rna_df["first_quartile"] - (1.5 * rna_df["IQR"])
    rna_df["max"] = rna_df["third_quartile"] + (1.5 * rna_df["IQR"])

    for col in ["min", "max", "median", "first_quartile", "third_quartile"]:
        rna_df[col] = np.around(rna_df[col], 4)

    rna_df.drop("IQR", axis=1, inplace=True)

    return rna_df
