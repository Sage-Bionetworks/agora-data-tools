from typing import Dict, List

import numpy as np
import pandas as pd

from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    nest_fields,
    normalize_null_values,
)
from agoradatatools.etl import transform


REQUIRED_INPUT = {
    "gene_metadata": ["ensembl_gene_id"],
    "igap": ["ensembl_gene_id"],
    "eqtl": ["ensembl_gene_id"],
    "proteomics": [
        "ensembl_gene_id",
        "uniqid",
        "log2_fc",
        "cor_pval",
        "ci_lwr",
        "ci_upr",
    ],
    "diff_exp_data": ["ensembl_gene_id", "adj_p_val"],
    "proteomics_tmt": [
        "ensembl_gene_id",
        "uniqid",
        "log2_fc",
        "cor_pval",
        "ci_lwr",
        "ci_upr",
    ],
    "proteomics_srm": [
        "ensembl_gene_id",
        "uniqid",
        "log2_fc",
        "cor_pval",
        "ci_lwr",
        "ci_upr",
    ],
    "target_list": ["ensembl_gene_id"],
    "median_expression": ["ensembl_gene_id"],
    "pharos_classes": ["ensembl_gene_id", "pharos_class"],
    "genes_biodomains": ["ensembl_gene_id", "biodomain"],
    "tep_adi_info": ["hgnc_symbol", "is_adi", "is_tep"],
    "ensg_to_uniprot_mapping": ["ensembl_gene_id", "uniprotkb_accessions"],
}


def transform_gene_info(
    datasets: dict,
    adjusted_p_value_threshold: float,
    protein_level_threshold: float,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
    """
    This function will perform transformations and incrementally create a dataset called gene_info.
    Each dataset will be left_joined onto gene_info, starting with gene_metadata.
    """
    check_required_datasets_and_columns(datasets, required_input)

    gene_metadata = datasets["gene_metadata"]
    igap = datasets["igap"]
    eqtl = datasets["eqtl"]
    proteomics = transform.transform_proteomics(df=datasets["proteomics"])
    rna_change = datasets["diff_exp_data"]
    proteomics_tmt = transform.transform_proteomics(df=datasets["proteomics_tmt"])
    proteomics_srm = transform.transform_proteomics(df=datasets["proteomics_srm"])
    target_list = datasets["target_list"]
    median_expression = datasets["median_expression"]
    pharos_classes = datasets["pharos_classes"]
    biodomains = datasets["genes_biodomains"]
    tep_info = datasets["tep_adi_info"]
    uniprot = datasets["ensg_to_uniprot_mapping"]

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
    proteomics_concat = pd.concat([proteomics, proteomics_tmt, proteomics_srm])
    proteomics_concat = proteomics_concat.dropna(
        subset=["log2_fc", "cor_pval", "ci_lwr", "ci_upr"]
    )
    proteomics_concat = (
        proteomics_concat.groupby("ensembl_gene_id")["cor_pval"]
        .agg("min")
        .reset_index()
    )

    target_list = nest_fields(
        df=target_list,
        grouping="ensembl_gene_id",
        new_column="target_nominations",
        drop_columns=["ensembl_gene_id"],
    )

    median_expression = nest_fields(
        df=median_expression,
        grouping="ensembl_gene_id",
        new_column="median_expression",
        drop_columns=["ensembl_gene_id"],
    )

    druggability = nest_fields(
        df=(
            pharos_classes.groupby("ensembl_gene_id")["pharos_class"]
            .apply(list)
            .reset_index()
        ),
        grouping="ensembl_gene_id",
        new_column="druggability",
        drop_columns=["ensembl_gene_id"],
        nested_field_is_list=False,
    )

    biodomains = biodomains.dropna(subset=["biodomain", "ensembl_gene_id"])
    biodomains = (
        biodomains.groupby("ensembl_gene_id")["biodomain"]
        .apply(set)  # ensure unique biodomain names
        .apply(list)
        .reset_index()
        .rename(columns={"biodomain": "biodomains"})
    )

    # sort biodomains list alphabetically
    biodomains["biodomains"] = biodomains["biodomains"].apply(sorted)

    # For genes with either is_adi or is_tep set to True, create a resource URL that opens
    # the portal page to the specific gene. This must be done using the hgnc_symbol from the
    # tep_info file and not the symbol in gene_info, because there are some mismatches
    # between the two and the hgnc_symbol from tep_info is the correct one to use here.
    # resource_url should be None if both is_adi and is_tep are false.
    RESOURCE_URL_PREFIX = (
        "https://adknowledgeportal.synapse.org/Explore/Target%20Enabling%20Resources?QueryWrapper0=%7B%22sql%22%3A%22"
        + "select%20*%20from%20syn26146692%20WHERE%20%60isPublic%60%20%3D%20true%22%2C%22limit%22%3A25%2C%22offset%22"
        + "%3A0%2C%22selectedFacets%22%3A%5B%7B%22concreteType%22%3A%22org.sagebionetworks.repo.model.table."
        + "FacetColumnValuesRequest%22%2C%22columnName%22%3A%22target%22%2C%22facetValues%22%3A%5B%22"
    )
    RESOURCE_URL_SUFFIX = "%22%5D%7D%5D%7D"

    tep_info["resource_url"] = tep_info.apply(
        lambda row: (
            RESOURCE_URL_PREFIX + row["hgnc_symbol"] + RESOURCE_URL_SUFFIX
            if row["is_adi"] is True or row["is_tep"] is True
            else None
        ),
        axis=1,
    )

    # Collapse uniprot IDs into a list for each ensembl_gene_id
    collapsed_uniprot = (
        uniprot.groupby("ensembl_gene_id")["uniprotkb_accessions"]
        .apply(list)
        .reset_index()
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
        collapsed_uniprot,
    ]:
        gene_info = pd.merge(
            left=gene_info,
            right=dataset,
            on="ensembl_gene_id",
            how="outer",
            validate="one_to_one",
        )

    # Populate values for rows that didn't exist in the individual datasets with normalize_null_values. Fill special
    # values (-1 for adj_p_val and cor_pval, empty lists for alias and ensembl_possible_replacements) manually, since
    # normalize_null_values doesn't support filling NA values with arbitrary numbers.
    gene_info = normalize_null_values(
        gene_info,
        boolean_columns=["is_igap", "is_eqtl", "is_adi", "is_tep"],
        empty_list_columns=["alias", "ensembl_possible_replacements"],
    ).fillna({"adj_p_val": -1, "cor_pval": -1})

    # Add ensembl_info as a nested field. This is done after merging all other data sets so it applies to
    # all possible Ensembl IDs in all data sets.
    ensembl_info = gene_info[
        [
            "ensembl_gene_id",
            "ensembl_release",
            "ensembl_possible_replacements",
            "ensembl_permalink",
        ]
    ]
    ensembl_info = nest_fields(
        df=ensembl_info,
        grouping="ensembl_gene_id",
        new_column="ensembl_info",
        drop_columns=["ensembl_gene_id"],
        nested_field_is_list=False,
    )

    gene_info = pd.merge(
        left=gene_info,
        right=ensembl_info,
        on="ensembl_gene_id",
        how="outer",
        validate="one_to_one",
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
        lambda row: (
            len(row["target_nominations"])
            if isinstance(row["target_nominations"], list)
            else np.nan
        ),
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
            "ensembl_info",
            "uniprotkb_accessions",
        ]
    ]

    # Make sure there are no N/A Ensembl IDs
    gene_info = gene_info.dropna(subset=["ensembl_gene_id"])

    # Sort by Ensembl ID so that new genes are easy to spot-check via diff, and so that
    # output order doesn't depend on pandas' unspecified row order for outer merges (AG-1104)
    gene_info = gene_info.sort_values("ensembl_gene_id").reset_index(drop=True)

    return gene_info
