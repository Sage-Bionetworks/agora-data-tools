import pandas as pd

from agoradatatools.etl.transform.genes_biodomains import count_grouped_total


class TestCountGroupedTotal:
    df = pd.DataFrame(
        {
            "col_1": ["a", "a", "a", "b", "c", "c", "c"],  # 3 'Ensembl IDs'
            "col_2": ["x", "y", "z", "x", "y", "z", "z"],  # 3 'biodomains'
            "col_3": ["1", "1", "2", "3", "2", "1", "3"],  # 3 'go_terms'
            "col_4": ["m", "m", "n", "n", "o", "o", "o"],  # An ignored column
        }
    )

    # How many unique "col_2"'s per unique "col_1" value?
    def test_count_grouped_total_one_group(self):
        expected_df = pd.DataFrame({"col_1": ["a", "b", "c"], "output": [3, 1, 2]})
        counted = count_grouped_total(
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

        counted = count_grouped_total(
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
