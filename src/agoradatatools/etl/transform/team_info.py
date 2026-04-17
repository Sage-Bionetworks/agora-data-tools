from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import check_required_datasets_and_columns

REQUIRED_INPUT: Dict[str, List[str]] = {
    "team_info": ["team"],
    "team_member_info": ["team"],
}


def transform_team_info(datasets: dict):
    check_required_datasets_and_columns(datasets, REQUIRED_INPUT)

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
