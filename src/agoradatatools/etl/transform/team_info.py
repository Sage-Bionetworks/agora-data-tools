from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import check_required_datasets_and_columns


REQUIRED_INPUT = {
    "team_info": ["team"],
    "team_member_info": ["team"],
}


def transform_team_info(
    datasets: dict,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
):
    """Transforms team and team member info into a single joined DataFrame.

    Groups team members by team and merges them into the team info DataFrame.

    Args:
        datasets (dict): Dictionary containing "team_info" and "team_member_info" DataFrames.
        required_input (Dict[str, List[str]]): Dictionary of required datasets and their
            required columns. Defaults to REQUIRED_INPUT.

    Returns:
        pd.DataFrame: Merged DataFrame with team info and nested member records.

    Raises:
        ValueError: If required datasets or columns are missing.
    """
    check_required_datasets_and_columns(datasets, required_input)

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
    joined_df = pd.merge(left=team_info, right=team_member_info, how="left", on="team")
    return joined_df
