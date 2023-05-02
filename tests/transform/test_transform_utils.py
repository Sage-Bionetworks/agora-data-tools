import sys
from io import StringIO
from unittest.mock import patch

import numpy as np
import pandas as pd

from agoradatatools.etl.transform import utils


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
    standard_df = utils.standardize_column_names(df=df)
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


class TestStandardizeValues:
    df = pd.DataFrame(
        {
            "a": ["n/a"],
            "b": ["N/A"],
            "c": ["n/A"],
            "d": ["N/a"],
        }
    )

    def test_standardize_values_success(self):
        standard_df = utils.standardize_values(df=self.df.copy())
        for value in standard_df.iloc[0].tolist():
            assert np.isnan(value)

    def test_standardize_values_TypeError(self):
        with patch.object(pd.DataFrame, "replace") as patch_replace:
            patch_replace.side_effect = TypeError
            captured_output = StringIO()
            sys.stdout = captured_output
            standard_df = utils.standardize_values(df=self.df.copy())
            assert "Error comparing types." in captured_output.getvalue()
            assert standard_df.equals(self.df)


class TestRenameColumns:
    df = pd.DataFrame(
        {
            "a": ["test_value"],
            "b": ["test_value"],
            "c": ["test_value"],
            "d": ["test_value"],
        }
    )
    good_column_map = {"a": "e", "b": "f", "c": "g", "d": "h"}
    bad_column_map = []

    def test_rename_columns_success(self):
        renamed_df = utils.rename_columns(
            df=self.df.copy(), column_map=self.good_column_map
        )
        assert list(renamed_df.columns) == list(self.good_column_map.values())

    def test_rename_columns_TypeError(self):
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_renamed_df = utils.rename_columns(
            df=self.df.copy(), column_map=self.bad_column_map
        )
        assert "Column mapping must be a dictionary" in captured_output.getvalue()
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())


def test_nest_fields():
    df = pd.DataFrame(
        {
            "a": ["group_1", "group_1", "group_2", "group_2", "group_3", "group_3"],
            "b": ["1", "1", "1", "1", "1", "1"],
            "c": ["1", "1", "1", "1", "1", "1"],
            "d": ["1", "1", "1", "1", "1", "1"],
        }
    )
    expected_column_e = [
        [{"a": "group_1", "b": "1", "c": "1"}, {"a": "group_1", "b": "1", "c": "1"}],
        [{"a": "group_2", "b": "1", "c": "1"}, {"a": "group_2", "b": "1", "c": "1"}],
        [{"a": "group_3", "b": "1", "c": "1"}, {"a": "group_3", "b": "1", "c": "1"}],
    ]

    nested_df = utils.nest_fields(
        df=df, grouping="a", new_column="e", drop_columns=["d"]
    )
    assert list(nested_df["e"]) == expected_column_e
