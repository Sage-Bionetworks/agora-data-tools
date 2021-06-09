import pytest
from agoradatatools import transform
import pandas as pd
import numpy as np

df = pd.DataFrame(
    {'team id': [np.nan, 0, 1, 2],
     'team.Name': ['MSN', 'Team 1', 'Team 2', np.nan],
     'team-Sco@#&': ['x', 'y', 'z', "na"]})

def test_standardize_column_names():

    result_df = transform.standardize_column_names(df)
    assert type(result_df) is pd.core.frame.DataFrame
    assert list(result_df.columns) == ['team_id', 'team_name', 'team-sco']


def test_standardize_values():

    assert df.isna().sum().sum() == 2

    result_df = transform.standardize_values(df)

    assert type(result_df) is pd.core.frame.DataFrame
    assert result_df.isna().sum().sum() == 0
    assert result_df.shape == (4, 3)
