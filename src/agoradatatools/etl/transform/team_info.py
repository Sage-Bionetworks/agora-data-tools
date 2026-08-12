from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import general_utils as gu

REQUIRED_INPUT = {
    "team_info": ["team"],
    "team_member_info": ["team", "isprimaryinvestigator", "name", "url"],
}


def transform_team_info(
    datasets: dict,
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
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
    gu.check_required_datasets_and_columns(datasets, required_input)

    team_info = datasets["team_info"]
    team_member_info = gu.normalize_null_values(
        datasets["team_member_info"],
        boolean_columns=["isprimaryinvestigator"],
        empty_string_columns=["name", "url"],
    )

    team_member_info = gu.nest_fields(
        team_member_info, grouping="team", new_column="members", drop_columns=["team"]
    )

    joined_df = pd.merge(
        left=team_info,
        right=team_member_info,
        how="left",
        on="team",
        validate="one_to_one",
    )
    return joined_df
