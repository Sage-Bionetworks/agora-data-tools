import pandas as pd


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
    joined_df = pd.merge(left=team_info, right=team_member_info, how="left", on="team")
    return joined_df
