import sys
from io import StringIO

from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd

from agoradatatools.etl import transform


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


def test_standardize_values_success():
    df = pd.DataFrame(
        {
            "a": ["n/a"],
            "b": ["N/A"],
            "c": ["n/A"],
            "d": ["N/a"],
        }
    )
    standard_df = transform.standardize_values(df=df)
    for value in standard_df.iloc[0].tolist():
        assert np.isnan(value)


def test_standardize_values_TypeError():
    with patch.object(pd.DataFrame, "replace") as patch_replace:
        patch_replace.side_effect = TypeError
        df = pd.DataFrame(
            {
                "a": ["n/a"],
                "b": ["N/A"],
                "c": ["n/A"],
                "d": ["N/a"],
            }
        )
        captured_output = StringIO()
        sys.stdout = captured_output
        standard_df = transform.standardize_values(df=df)
        assert "Error comparing types." in captured_output.getvalue()
        assert standard_df.equals(df)


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
