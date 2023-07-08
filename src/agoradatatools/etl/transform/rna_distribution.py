import numpy as np
from agoradatatools.etl import utils

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
    
    rna_df = utils.calculate_distribution(
        df=rna_df,
        grouping=["tissue", "model"],
        distribution_column="logfc")
    
    # Columns must be in this order
    rna_df = rna_df[["model", "tissue", "min", "max", "first_quartile", "median", "third_quartile"]]

    return rna_df
