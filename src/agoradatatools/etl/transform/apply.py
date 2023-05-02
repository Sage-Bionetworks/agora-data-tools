from agoradatatools.etl.transform.custom import *


# TODO refactor to avoid so many if's - maybe some sort of mapping to callables
def apply_custom_transformations(datasets: dict, dataset_name: str, dataset_obj: dict):
    if not isinstance(datasets, dict) or not isinstance(dataset_name, str):
        return None
    if dataset_name == "genes_biodomains":
        return transform_genes_biodomains(datasets=datasets)
    if dataset_name == "overall_scores":
        df = datasets["overall_scores"]
        return transform_overall_scores(df=df)
    if dataset_name == "distribution_data":
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
    if dataset_name == "team_info":
        return transform_team_info(datasets=datasets)
    if dataset_name == "rnaseq_differential_expression":
        return transform_rna_seq_data(datasets=datasets)
    if dataset_name == "gene_info":
        return transform_gene_info(
            datasets=datasets,
            adjusted_p_value_threshold=dataset_obj["custom_transformations"][
                "adjusted_p_value_threshold"
            ],
            protein_level_threshold=dataset_obj["custom_transformations"][
                "protein_level_threshold"
            ],
        )
    if dataset_name == "rna_distribution_data":
        return transform_rna_distribution_data(datasets=datasets)
    if dataset_name == "proteomics_distribution_data":
        return create_proteomics_distribution_data(datasets=datasets)
    else:
        return None
