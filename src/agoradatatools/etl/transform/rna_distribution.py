from agoradatatools.etl import transform, utils


def transform_rna_distribution_data(datasets: dict):
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
