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
    tep_info = datasets["tep_adi_info"]

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
        "ensembl_gene_id",
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
        df=target_list, grouping="ensembl_gene_id", new_column="target_nominations", drop_columns=["ensembl_gene_id"]
    )

    median_expression = nest_fields(
        df=median_expression, grouping="ensembl_gene_id", new_column="median_expression", drop_columns=["ensembl_gene_id"]
    )

    druggability = nest_fields(
        df=druggability, grouping="ensembl_gene_id", new_column="druggability", drop_columns=["ensembl_gene_id"]
    )

    biodomains = (
        biodomains.groupby("ensembl_gene_id")["biodomain"]
        .apply(set) # ensure unique biodomain names
        .apply(list)
        .reset_index()
        .rename(columns={"biodomain": "biodomains"})
    )

    # sort biodomains list alphabetically
    biodomains['biodomains'] = biodomains['biodomains'].apply(sorted)

    # For genes with either is_adi or is_tep set to True, create a resource URL that opens
    # the portal page to the specific gene. This must be done using the hgnc_symbol from the
    # tep_info file and not the symbol in gene_info, because there are some mismatches
    # between the two and the hgnc_symbol from tep_info is the correct one to use here.
    # resource_url should be NA if both is_adi and is_tep are false. 
    resource_url_prefix = "https://adknowledgeportal.synapse.org/Explore/Target%20Enabling%20Resources?QueryWrapper0=%7B%22sql%22%3A%22select%20*%20from%20syn26146692%20WHERE%20%60isPublic%60%20%3D%20true%22%2C%22limit%22%3A25%2C%22offset%22%3A0%2C%22selectedFacets%22%3A%5B%7B%22concreteType%22%3A%22org.sagebionetworks.repo.model.table.FacetColumnValuesRequest%22%2C%22columnName%22%3A%22target%22%2C%22facetValues%22%3A%5B%22"
    resource_url_suffix = "%22%5D%7D%5D%7D"
    tep_info["resource_url"] = tep_info.apply(
        lambda row: resource_url_prefix + row["hgnc_symbol"] + resource_url_suffix
        if (row["is_adi"] == True) | (row["is_tep"] == True)
        else np.NaN,
        axis=1,
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
        biodomains,
        tep_info,
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
        {
            "is_igap": False,
            "is_eqtl": False,
            "adj_p_val": -1,
            "cor_pval": -1,
            "is_adi": False,
            "is_tep": False,
        },
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
    gene_info["is_any_rna_changed_in_ad_brain"] = (
        gene_info["adj_p_val"] <= adjusted_p_value_threshold
    ) & gene_info["rna_brain_change_studied"]

    gene_info["protein_brain_change_studied"] = gene_info["cor_pval"] != -1
    gene_info["is_any_protein_changed_in_ad_brain"] = (
        gene_info["cor_pval"] <= protein_level_threshold
    ) & gene_info["protein_brain_change_studied"]

    # create 'total_nominations' field
    gene_info["total_nominations"] = gene_info.apply(
        lambda row: len(row["target_nominations"])
        if isinstance(row["target_nominations"], list)
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
            "is_eqtl",
            "is_any_rna_changed_in_ad_brain",
            "rna_brain_change_studied",
            "is_any_protein_changed_in_ad_brain",
            "protein_brain_change_studied",
            "target_nominations",
            "median_expression",
            "druggability",
            "total_nominations",
            "biodomains",
            "is_adi",
            "is_tep",
            "resource_url",
        ]
    ]

    # Make sure there are no N/A Ensembl IDs
    gene_info = gene_info.dropna(subset=["ensembl_gene_id"])

    return gene_info
