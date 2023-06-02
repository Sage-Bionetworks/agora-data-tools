import numpy as np


def transform_rna_seq_data(datasets: dict):
    diff_exp_data = datasets["diff_exp_data"]

    diff_exp_data["study"].replace(
        to_replace={"MAYO": "MayoRNAseq", "MSSM": "MSBB"}, regex=True, inplace=True
    )
    diff_exp_data["sex"].replace(
        to_replace={
            "ALL": "males and females",
            "FEMALE": "females only",
            "MALE": "males only",
        },
        regex=True,
        inplace=True,
    )
    diff_exp_data["model"].replace(
        to_replace="\\.", value=" x ", regex=True, inplace=True
    )
    diff_exp_data["model"].replace(
        to_replace={"Diagnosis": "AD Diagnosis"}, regex=True, inplace=True
    )
    diff_exp_data["fc"] = 2 ** diff_exp_data["logfc"]
    diff_exp_data["model"] = diff_exp_data["model"] + " (" + diff_exp_data["sex"] + ")"

    diff_exp_data = diff_exp_data[
        [
            "ensembl_gene_id",
            "hgnc_symbol",
            "logfc",
            "fc",
            "ci_l",
            "ci_r",
            "adj_p_val",
            "tissue",
            "study",
            "model",
        ]
    ]

    return diff_exp_data


def transform_rna_distribution_data(datasets: dict):
    # "datasets" contains the unprocessed RNA-seq data, which needs to go
    # through the same processing as before in order to use it here.
    rna_df = transform_rna_seq_data(datasets)
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
