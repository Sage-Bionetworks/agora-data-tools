from agoradatatools.etl import transform
import pandas as pd
import numpy as np


def test_standardize_column_names():
    df = pd.DataFrame(
        {
            "a#": ["test_value"],
            "b@": ["test_value"],
            "c&": ["test_value"],
            "d*": ["test_value"],
            "e^": ["test_value"],
            "f?": ["test_value"],
            "g(": ["test_value"],
            "h)": ["test_value"],
            "i%": ["test_value"],
            "j$": ["test_value"],
            "k#": ["test_value"],
            "l!": ["test_value"],
            "m/": ["test_value"],
            "n ": ["test_value"],
            "o-": ["test_value"],
            "p.": ["test_value"],
            "AAA": ["test_value"],
        }
    )
    standard_df = transform.standardize_column_names(df=df)
    assert list(standard_df.columns) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n_",
        "o_",
        "p_",
        "aaa",
    ]


# df = pd.DataFrame(
#     {'team id': [np.nan, 0, 1, 2],
#      'team.Name': ['MSN', 'Team 1', 'Team 2', np.nan],
#      'team-Sco@#&': ['x', 'y', 'z', "na"]})

# def test_standardize_column_names():

#     result_df = transform.standardize_column_names(df)
#     assert type(result_df) is pd.core.frame.DataFrame
#     assert list(result_df.columns) == ['team_id', 'team_name', 'team-sco']


# def test_standardize_values():

#     assert df.isna().sum().sum() == 2

#     result_df = transform.standardize_values(df)

#     assert type(result_df) is pd.core.frame.DataFrame
#     assert result_df.isna().sum().sum() == 0
#     assert result_df.shape == (4, 3)

# def test_rename_columns():
#     refresh_df = pd.DataFrame(
#         {'team id': [np.nan, 0, 1, 2],
#          'team.Name': ['MSN', 'Team 1', 'Team 2', np.nan],
#          'team-Sco@#&': ['x', 'y', 'z', "na"]})

#     bad_result_df = transform.rename_columns(df=refresh_df, column_map={"team-Sco@#&"})
#     assert type(bad_result_df) is pd.core.frame.DataFrame
#     assert list(bad_result_df.columns) == ["team id", "team.Name", "team-Sco@#&"]

#     partial_good_result_df = transform.rename_columns(df=refresh_df, column_map={"team-Sco@#&": "team_scope"})
#     assert list(partial_good_result_df.columns) == ['team id', 'team.Name', 'team_scope']
#     assert type(partial_good_result_df) is pd.core.frame.DataFrame
