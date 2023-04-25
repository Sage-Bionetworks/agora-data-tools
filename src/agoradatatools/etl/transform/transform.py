import numpy as np
import pandas as pd

from agoradatatools.etl.transform.utils import *
from agoradatatools.etl.transform.transform_genes_biodomains import (
    transform_genes_biodomains,
)


def transform_team_info(datasets: dict):
    team_info = datasets["team_info"]
    team_member_info = datasets["team_member_info"]

    team_member_info = (
        team_member_info.groupby("team")
        .apply(
            lambda x: x[x.columns.difference(["team"])]
            .fillna("")
            .to_dict(orient="records")
        )
        .reset_index(name="members")
    )

    return join_datasets(left=team_info, right=team_member_info, how="left", on="team")


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
        ]
    ]

    # Make sure there are no N/A Ensembl IDs
    gene_info = gene_info.dropna(subset=["ensembl_gene_id"])

    return gene_info


def transform_distribution_data(
    datasets: dict,
    overall_max_score,
    genetics_max_score,
    omics_max_score,
    lit_max_score,
):
    overall_scores = datasets["overall_scores"]
    interesting_columns = [
        "ensg",
        "overall",
        "geneticsscore",
        "omicsscore",
        "literaturescore",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics", "isscored_lit"]
    mapping = dict(zip(interesting_columns[2:], scored))
    mapping["overall"] = None

    # create mapping for max score values from config
    max_score = dict(
        zip(
            interesting_columns[1:],
            [overall_max_score, genetics_max_score, omics_max_score, lit_max_score],
        )
    )

    overall_scores = overall_scores[interesting_columns + scored]

    neo_matrix = {}
    for col in interesting_columns[1:]:  # excludes the ENSG
        neo_matrix[col] = calculate_distribution(
            overall_scores, col, mapping[col], max_score[col]
        )

    neo_matrix["target_risk_score"] = neo_matrix.pop("overall")
    neo_matrix["genetics_score"] = neo_matrix.pop("geneticsscore")
    neo_matrix["multi_omics_score"] = neo_matrix.pop("omicsscore")
    neo_matrix["literature_score"] = neo_matrix.pop("literaturescore")

    additional_data = [
        {"name": "Target Risk Score", "syn_id": "syn25913473", "wiki_id": "621071"},
        {"name": "Genetic Risk Score", "syn_id": "syn25913473", "wiki_id": "621069"},
        {"name": "Multi-omic Risk Score", "syn_id": "syn25913473", "wiki_id": "621070"},
        {"name": "Literature Score", "syn_id": "syn25913473", "wiki_id": "613105"},
    ]
    for col, additional in zip(neo_matrix.keys(), additional_data):
        neo_matrix[col]["name"] = additional["name"]
        neo_matrix[col]["syn_id"] = additional["syn_id"]
        neo_matrix[col]["wiki_id"] = additional["wiki_id"]

    return neo_matrix


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


def transform_proteomics_distribution_data(
    proteomics_df: pd.DataFrame, datatype: str
) -> pd.DataFrame:
    """Transform proteomics data
    Args:
        proteomics_df (pd.DataFrame): Dataframe
        datatype (str): Data Type
    Returns:
        pd.DataFrame: Transformed data
    """
    proteomics_df = (
        proteomics_df.groupby(["tissue"])
        .agg("describe")["log2_fc"]
        .reset_index()[["tissue", "min", "max", "25%", "50%", "75%"]]
    )

    proteomics_df.rename(
        columns={"25%": "first_quartile", "50%": "median", "75%": "third_quartile"},
        inplace=True,
    )

    proteomics_df["IQR"] = (
        proteomics_df["third_quartile"] - proteomics_df["first_quartile"]
    )
    proteomics_df["min"] = proteomics_df["first_quartile"] - (
        1.5 * proteomics_df["IQR"]
    )
    proteomics_df["max"] = proteomics_df["third_quartile"] + (
        1.5 * proteomics_df["IQR"]
    )

    for col in ["min", "max", "median", "first_quartile", "third_quartile"]:
        proteomics_df[col] = np.around(proteomics_df[col], 4)

    proteomics_df.drop("IQR", axis=1, inplace=True)
    proteomics_df["type"] = datatype

    return proteomics_df


def create_proteomics_distribution_data(datasets: dict) -> pd.DataFrame:
    transformed = []
    for name, dataset in datasets.items():
        if name == "proteomics":
            transformed.append(
                transform_proteomics_distribution_data(
                    proteomics_df=dataset, datatype="LFQ"
                )
            )
        elif name == "proteomics_tmt":
            transformed.append(
                transform_proteomics_distribution_data(
                    proteomics_df=dataset, datatype="TMT"
                )
            )

    return pd.concat(transformed)


def apply_custom_transformations(datasets: dict, dataset_name: str, dataset_obj: dict):
    if type(datasets) is not dict or type(dataset_name) is not str:
        return None

    elif dataset_name == "genes_biodomains":
        return transform_genes_biodomains(datasets=datasets)
    if dataset_name == "overall_scores":
        df = datasets["overall_scores"]
        return transform_overall_scores(df=df)
    elif dataset_name == "distribution_data":
        return transform_distribution_data(
            datasets=datasets,
            overall_max_score=dataset_obj["custom_transformations"][
                "overall_max_score"
            ],
            genetics_max_score=dataset_obj["custom_transformations"][
                "genetics_max_score"
            ],
            omics_max_score=dataset_obj["custom_transformations"]["omics_max_score"],
            lit_max_score=dataset_obj["custom_transformations"]["lit_max_score"],
        )
    elif dataset_name == "team_info":
        return transform_team_info(datasets=datasets)
    elif dataset_name == "rnaseq_differential_expression":
        return transform_rna_seq_data(datasets=datasets)
    elif dataset_name == "gene_info":
        return transform_gene_info(
            datasets=datasets,
            adjusted_p_value_threshold=dataset_obj["custom_transformations"][
                "adjusted_p_value_threshold"
            ],
            protein_level_threshold=dataset_obj["custom_transformations"][
                "protein_level_threshold"
            ],
        )
    elif dataset_name == "rna_distribution_data":
        return transform_rna_distribution_data(datasets=datasets)
    elif dataset_name == "proteomics_distribution_data":
        return create_proteomics_distribution_data(datasets=datasets)
    else:
        return None
