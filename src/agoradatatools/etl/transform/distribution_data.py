from typing import Dict, List

import numpy as np
import pandas as pd

from agoradatatools.etl.utils import general_utils as gu


REQUIRED_INPUT = {
    "overall_scores": [
        "ensg",
        "target_risk_score",
        "genetics_score",
        "multi_omics_score",
        "isscored_genetics",
        "isscored_omics",
    ],
}


def calculate_distribution(df: pd.DataFrame, col: str, is_scored, upper_bound) -> dict:
    """Calculates the distribution statistics for a single score column.

    Filters rows based on the is_scored flag, then computes bin distributions,
    min, max, mean, and quartile values across the specified column.

    Args:
        df (pd.DataFrame): DataFrame containing the score column and isscored columns.
        col (str): Name of the column to compute the distribution over.
        is_scored: Column name used to filter rows to scored entries, or None/falsy
            to include all rows with at least one "Y" in any isscored column.
        upper_bound: The theoretical maximum value for the score, used to anchor bins.

    Returns:
        dict: Distribution statistics including "distribution", "bins", "min", "max",
            "mean", "first_quartile", and "third_quartile".
    """
    if is_scored:
        df = df[df[is_scored] == "Y"]
    # If isscored is blank/NaN, take all rows with at least one "Y" in any isscored column
    else:
        df = df[df.isin(["Y"]).any(axis=1)]

    if df[col].dtype == object:
        df = df.copy()  # Necessary to prevent SettingWithCopy warning
        df[col] = df[col].astype(float)

    obj = {}

    # In order to smooth out the bins and make sure the entire range from 0
    # to the theoretical maximum value has been found, we create a copy of the
    # column with both 0 and that maximum value added to it.  We use the copy to calculate
    # distributions and bins, and subtract the values at the end

    distribution = pd.concat([df[col], pd.Series([0, upper_bound])], ignore_index=True)

    obj["distribution"] = list(
        pd.cut(
            distribution, bins=10, precision=3, include_lowest=True, right=True
        ).value_counts(sort=False)
    )

    # obj["distribution"][0] is for the lowest bin, which includes values of 0. Since this was
    # calculated with an extra artificial 0 value, we subtract 1 to get the real count.
    obj["distribution"][0] -= 1

    # obj["distribution"][-1] (end of the list) is for the highest bin, which includes the upper
    # bound. Since this was calculated with an extra artificial upper_bound, we subtract 1 as above.
    obj["distribution"][-1] -= 1

    discard, obj["bins"] = list(
        pd.cut(
            distribution,
            bins=10,
            precision=3,
            include_lowest=True,
            right=True,
            retbins=True,
        )
    )
    obj["bins"] = np.around(obj["bins"].tolist()[1:], 2)
    base = [0, *obj["bins"][:-1]]
    obj["bins"] = zip(base, obj["bins"])
    obj["bins"] = list(obj["bins"])

    obj["min"] = np.around(df[col].min(), 4)
    obj["max"] = np.around(df[col].max(), 4)
    obj["mean"] = np.around(df[col].mean(), 4)
    obj["first_quartile"] = np.around(
        df[col].quantile(q=0.25, interpolation="midpoint"), 4
    )
    obj["third_quartile"] = np.around(
        df[col].quantile(q=0.75, interpolation="midpoint"), 4
    )

    return obj


def transform_distribution_data(
    datasets: dict,
    overall_max_score,
    genetics_max_score,
    omics_max_score,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
):
    """Transforms the overall scores dataset into distribution statistics for each score type.

    Computes binned distributions, quartiles, and metadata for the target risk score,
    genetics score, and multi-omics score columns.

    Args:
        datasets (dict): Dictionary containing an "overall_scores" DataFrame.
        overall_max_score: Theoretical maximum value for the target risk score.
        genetics_max_score: Theoretical maximum value for the genetics score.
        omics_max_score: Theoretical maximum value for the multi-omics score.
        required_input (Dict[str, List[str]]): Dictionary of required datasets and their
            required columns. Defaults to REQUIRED_INPUT.

    Returns:
        dict: Nested dictionary keyed by score column name, each containing distribution
            statistics and display metadata (name, syn_id, wiki_id).

    Raises:
        ValueError: If required datasets or columns are missing.
    """
    gu.check_required_datasets_and_columns(datasets, required_input)

    overall_scores = datasets["overall_scores"]
    interesting_columns = [
        "ensg",
        "target_risk_score",
        "genetics_score",
        "multi_omics_score",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics"]
    mapping = dict(zip(interesting_columns[2:], scored))
    mapping["target_risk_score"] = None

    # create mapping for max score values from config
    max_score = dict(
        zip(
            interesting_columns[1:],
            [overall_max_score, genetics_max_score, omics_max_score],
        )
    )

    overall_scores = overall_scores[interesting_columns + scored].drop_duplicates()

    neo_matrix = {}
    for col in interesting_columns[1:]:  # excludes the ENSG
        neo_matrix[col] = calculate_distribution(
            overall_scores, col, mapping[col], max_score[col]
        )

    additional_data = [
        {"name": "Target Risk Score", "syn_id": "syn25913473", "wiki_id": "621071"},
        {"name": "Genetic Risk Score", "syn_id": "syn25913473", "wiki_id": "621069"},
        {"name": "Multi-omic Risk Score", "syn_id": "syn25913473", "wiki_id": "621070"},
    ]
    for col, additional in zip(neo_matrix.keys(), additional_data):
        neo_matrix[col]["name"] = additional["name"]
        neo_matrix[col]["syn_id"] = additional["syn_id"]
        neo_matrix[col]["wiki_id"] = additional["wiki_id"]

    return neo_matrix
