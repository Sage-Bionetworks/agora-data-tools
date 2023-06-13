import pandas as pd
import numpy as np

from agoradatatools.etl.utils import nest_fields


def transform_gene_info(
    datasets: dict, adjusted_p_value_threshold, protein_level_threshold
):
    """
    This function will perform transformations and incrementally create a dataset called gene_info.
    Each dataset will be left_joined onto gene_info, starting with gene_metadata.
    """
    gene_metadata = datasets["gene_metadata"]
    igap = datasets["igap"]
    eqtl = datasets["eqtl"]
    proteomics = datasets["proteomics"]
    rna_change = datasets["rna_expression_change"]
    proteomics_tmt = datasets["agora_proteomics_tmt"]
    target_list = datasets["target_list"]
    median_expression = datasets["median_expression"]
    druggability = datasets["druggability"]
    biodomains = datasets["genes_biodomains"]

    # Modify the data before merging

    # All genes in this list should have 'is_igap' = True when added to gene_info.
    # Creating the column here automatically adds the column in to gene_info
    # during merge, with True values correctly populated.
    igap["is_igap"] = True

    # Get the smallest adj_p_val for each gene, to determine significance
    rna_change = (
        rna_change.groupby("ensembl_gene_id")["adj_p_val"].agg("min").reset_index()
    )

    # Get the smallest cor_pval for each protein, to determine significance
    proteomics_concat = pd.concat([proteomics, proteomics_tmt])
    proteomics_concat = proteomics_concat.dropna(
        subset=["log2_fc", "cor_pval", "ci_lwr", "ci_upr"]
    )
    proteomics_concat = (
        proteomics_concat.groupby("ensembl_gene_id")["cor_pval"]
        .agg("min")
        .reset_index()
    )

    # these are the interesting columns of the druggability dataset
    useful_columns = [
        "geneid",
        "sm_druggability_bucket",
        "safety_bucket",
        "abability_bucket",
        "pharos_class",
        "classification",
        "safety_bucket_definition",
        "abability_bucket_definition",
    ]
    druggability = druggability[useful_columns]

    target_list = nest_fields(
        df=target_list, grouping="ensembl_gene_id", new_column="nominated_target"
    )

    median_expression = nest_fields(
        df=median_expression, grouping="ensembl_gene_id", new_column="median_expression"
    )

    druggability = nest_fields(
        df=druggability, grouping="geneid", new_column="druggability"
    )
    druggability.rename(columns={"geneid": "ensembl_gene_id"}, inplace=True)
    
    biodomains = (
        biodomains.groupby("ensembl_gene_id")["biodomain"]
        .apply(set) # ensure unique biodomain names
        .apply(list)
        .reset_index()
        .rename(columns={"biodomain": "biodomains"})
    )

    # Merge all the datasets

    gene_info = gene_metadata

    for dataset in [
        igap,
        eqtl,
        rna_change,
        proteomics_concat,
        target_list,
        median_expression,
        druggability,
        biodomains
    ]:
        gene_info = pd.merge(
            left=gene_info,
            right=dataset,
            on="ensembl_gene_id",
            how="outer",
            validate="one_to_one",
        )

    # Populate values for rows that didn't exist in the individual datasets

    gene_info.fillna(
        {"is_igap": False, "has_eqtl": False, "adj_p_val": -1, "cor_pval": -1},
        inplace=True,
    )

    # fillna doesn't work for creating an empty array, need this function instead
    gene_info["alias"] = gene_info.apply(
        lambda row: row["alias"]
        if isinstance(row["alias"], np.ndarray)
        else np.ndarray(0, dtype=object),
        axis=1,
    )

    gene_info["rna_brain_change_studied"] = gene_info["adj_p_val"] != -1
    gene_info["rna_in_ad_brain_change"] = (
        gene_info["adj_p_val"] <= adjusted_p_value_threshold
    ) & gene_info["rna_brain_change_studied"]

    gene_info["protein_brain_change_studied"] = gene_info["cor_pval"] != -1
    gene_info["protein_in_ad_brain_change"] = (
        gene_info["cor_pval"] <= protein_level_threshold
    ) & gene_info["protein_brain_change_studied"]

    # create 'nominations' field
    gene_info["nominations"] = gene_info.apply(
        lambda row: len(row["nominated_target"])
        if isinstance(row["nominated_target"], list)
        else np.NaN,
        axis=1,
    )

    # Remove some extra columns that got added during merges
    gene_info = gene_info[
        [
            "ensembl_gene_id",
            "name",
            "summary",
            "symbol",
            "alias",
            "is_igap",
            "has_eqtl",
            "rna_in_ad_brain_change",
            "rna_brain_change_studied",
            "protein_in_ad_brain_change",
            "protein_brain_change_studied",
            "nominated_target",
            "median_expression",
            "druggability",
            "nominations",
            "biodomains"
        ]
    ]

    # Make sure there are no N/A Ensembl IDs
    gene_info = gene_info.dropna(subset=["ensembl_gene_id"])

    return gene_info
