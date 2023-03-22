import sys
from io import StringIO
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.agoradatatools.etl import transform


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
        standard_df = transform.standardize_values(df=self.df.copy())
        for value in standard_df.iloc[0].tolist():
            assert np.isnan(value)

    def test_standardize_values_TypeError(self):
        with patch.object(pd.DataFrame, "replace") as patch_replace:
            patch_replace.side_effect = TypeError
            captured_output = StringIO()
            sys.stdout = captured_output
            standard_df = transform.standardize_values(df=self.df.copy())
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
        renamed_df = transform.rename_columns(
            df=self.df.copy(), column_map=self.good_column_map
        )
        assert list(renamed_df.columns) == list(self.good_column_map.values())

    def test_rename_columns_TypeError(self):
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_renamed_df = transform.rename_columns(
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

    nested_df = transform.nest_fields(
        df=df, grouping="a", new_column="e", drop_columns=["d"]
    )
    assert list(nested_df["e"]) == expected_column_e


class TestCountGroupedTotal:
    df = pd.DataFrame(
        {
            "col_1": ["a", "a", "a", "b", "c", "c", "c"],  # 3 'Ensembl IDs'
            "col_2": ["x", "y", "z", "x", "y", "z", "z"],  # 3 'biodomains'
            "col_3": ["1", "1", "2", "3", "2", "1", "3"],  # 3 'go_terms'
            "col_4": [
                "m",
                "m",
                "n",
                "n",
                "o",
                "o",
                "o",
            ],  # An extra column that should get ignored
        }
    )

    # How many unique "col_2"'s per unique "col_1" value?
    def test_count_grouped_total_one_group(self):
        expected_df = pd.DataFrame({"col_1": ["a", "b", "c"], "output": [3, 1, 2]})
        counted = transform.count_grouped_total(
            df=self.df, grouping="col_1", input_colname="col_2", output_colname="output"
        )
        assert counted.equals(expected_df)

    # How many unique "col_3"'s per unique combination of "col_1" + "col_2"?
    def test_count_grouped_total_two_groups(self):
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "a", "a", "b", "c", "c"],
                "col_2": ["x", "y", "z", "x", "y", "z"],
                "output": [1, 1, 1, 1, 1, 2],
            }
        )

        counted = transform.count_grouped_total(
            df=self.df,
            grouping=["col_1", "col_2"],
            input_colname="col_3",
            output_colname="output",
        )
        assert counted.equals(expected_df)


# def test_transform_biodomains():
#     test_datasets = {
#         "biodomains": pd.DataFrame(
#             {
#                 "ensembl_gene_id": ["1", "1", "2", "2", "3", "3"],
#                 "biodomain": ["a", "b", "c", "d", "e", "f"],
#                 "go_terms": ["a", "b", "c", "d", "e", "f"],
#             }
#         )
#     }
#     expected_gene_biodomains_col = [
#         [{"biodomain": "a", "go_terms": ["a"]}, {"biodomain": "b", "go_terms": ["b"]}],
#         [{"biodomain": "c", "go_terms": ["c"]}, {"biodomain": "d", "go_terms": ["d"]}],
#         [{"biodomain": "e", "go_terms": ["e"]}, {"biodomain": "f", "go_terms": ["f"]}],
#     ]
#     test_biodomains = transform.transform_biodomains(datasets=test_datasets)
#     assert list(test_biodomains["gene_biodomains"]) == expected_gene_biodomains_col


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
