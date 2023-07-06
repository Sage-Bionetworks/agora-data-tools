import pandas as pd
import numpy as np


def calculate_distribution(df: pd.DataFrame, col: str, is_scored, upper_bound) -> dict:
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
        pd.cut(distribution, bins=10, precision=3, retbins=True)
    )
    obj["bins"] = np.around(obj["bins"].tolist()[1:], 2)
    base = [0, *obj["bins"][:-1]]
    obj["bins"] = zip(base, obj["bins"])
    obj["bins"] = list(obj["bins"])

    obj["min"] = np.around(df[col].min(), 4)
    obj["max"] = np.around(df[col].max(), 4)
    obj["mean"] = np.around(df[col].mean(), 4)
    obj["first_quartile"] = np.around(
        df[col].quantile(q=0.25, interpolation="midpoint")
    )
    obj["third_quartile"] = np.around(
        df[col].quantile(q=0.75, interpolation="midpoint")
    )

    return obj


def transform_distribution_data(
    datasets: dict,
    overall_max_score,
    genetics_max_score,
    omics_max_score,
):
    overall_scores = datasets["overall_scores"]
    interesting_columns = [
        "ensg",
        "overall",
        "geneticsscore",
        "omicsscore",
    ]

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ["isscored_genetics", "isscored_omics"]
    mapping = dict(zip(interesting_columns[2:], scored))
    mapping["overall"] = None

    # create mapping for max score values from config
    max_score = dict(
        zip(
            interesting_columns[1:],
            [overall_max_score, genetics_max_score, omics_max_score],
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
