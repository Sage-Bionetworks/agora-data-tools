from typing import Dict, List

from agoradatatools.etl.utils import general_utils as gu


REQUIRED_INPUT = {
    "diff_exp_data": [
        "ensembl_gene_id",
        "hgnc_symbol",
        "logfc",
        "ci_l",
        "ci_r",
        "adj_p_val",
        "tissue",
        "study",
        "sex",
        "model",
    ],
}


def transform_rnaseq_differential_expression(
    datasets: dict,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
):
    """Transforms the RNA-seq differential expression dataset.

    Applies study and sex label normalization, computes fold change from log fold change,
    and combines model and sex into a single display model string.

    Args:
        datasets (dict): Dictionary containing a "diff_exp_data" DataFrame with RNA-seq
            differential expression results.
        required_input (Dict[str, List[str]]): Dictionary of required datasets and their
            required columns. Defaults to REQUIRED_INPUT.

    Returns:
        pd.DataFrame: Transformed differential expression DataFrame.

    Raises:
        ValueError: If required datasets or columns are missing.
    """
    gu.check_required_datasets_and_columns(datasets, required_input)

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
