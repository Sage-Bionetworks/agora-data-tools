import sys
from io import StringIO
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
import yaml
import re

from agoradatatools.etl import utils


class TestLoginToSynapse:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn: synapseclient.Synapse) -> None:
        self.patch_synapseclient = patch.object(
            synapseclient, "Synapse", return_value=syn
        ).start()
        self.patch_syn_login = patch.object(syn, "login", return_value=syn).start()

    def teardown_method(self) -> None:
        mock.patch.stopall()

    def test_login_with_token(self) -> None:
        utils._login_to_synapse(token="my_auth_token")
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with(authToken="my_auth_token")

    def test_login_no_token(self) -> None:
        utils._login_to_synapse(token=None)
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with()


def test_get_config_with_invalid_file_path() -> None:
    with pytest.raises(FileNotFoundError, match="File not found. *"):
        utils._get_config(config_path="this/is/a/bad/path")


def test_get_config_with_parser_error() -> None:
    with pytest.raises(
        yaml.parser.ParserError, match="YAML file unable to be parsed. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_parsing.yaml")


def test_get_config_with_scanner_error() -> None:
    with pytest.raises(
        yaml.scanner.ScannerError, match="YAML file unable to be scanned. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_scanning.yaml")


def test_get_config_with_no_config_path() -> None:
    config = utils._get_config(config_path=None)
    assert config["destination"] == "syn12177492"


def test_get_config_with_config_path() -> None:
    config = utils._get_config(config_path="./test_config.yaml")
    assert config["destination"] == "syn17015333"


def test_standardize_column_names() -> None:
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

    def test_standardize_values_success(self) -> None:
        standard_df = utils.standardize_values(df=self.df.copy())
        for value in standard_df.iloc[0].tolist():
            assert np.isnan(value)

    def test_standardize_values_TypeError(self) -> None:
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

    def test_rename_columns_success(self) -> None:
        renamed_df = utils.rename_columns(
            df=self.df.copy(), column_map=self.good_column_map
        )
        assert list(renamed_df.columns) == list(self.good_column_map.values())

    def test_rename_columns_TypeError(self) -> None:
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_renamed_df = utils.rename_columns(
            df=self.df.copy(), column_map=self.bad_column_map
        )
        assert "Column mapping must be a dictionary" in captured_output.getvalue()
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())


class TestNestFields:
    """Tests the nest_fields function using a dataframe that has multiple rows per group and
    one that only has one row per group.
    """

    df_multirow = pd.DataFrame(
        {
            "a": ["group_1", "group_1", "group_2", "group_2", "group_3", "group_3"],
            "b": ["1", "1", "1", "1", "1", "1"],
            "c": ["1", "1", "1", "1", "1", "1"],
            "d": ["1", "1", "1", "1", "1", "1"],
        }
    )
    df_singlerow = pd.DataFrame(
        {
            "a": ["group_1", "group_2", "group_3"],
            "b": ["1", "1", "1"],
            "c": ["1", "1", "1"],
            "d": ["1", "1", "1"],
        }
    )

    def test_nest_fields_with_dropped_column(self) -> None:
        expected_column_e = [
            [
                {"a": "group_1", "b": "1", "c": "1"},
                {"a": "group_1", "b": "1", "c": "1"},
            ],
            [
                {"a": "group_2", "b": "1", "c": "1"},
                {"a": "group_2", "b": "1", "c": "1"},
            ],
            [
                {"a": "group_3", "b": "1", "c": "1"},
                {"a": "group_3", "b": "1", "c": "1"},
            ],
        ]

        nested_df = utils.nest_fields(
            df=self.df_multirow, grouping="a", new_column="e", drop_columns=["d"]
        )
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_with_dropped_column_list(self) -> None:
        expected_column_e = [
            [
                {"a": "group_1", "c": "1"},
                {"a": "group_1", "c": "1"},
            ],
            [
                {"a": "group_2", "c": "1"},
                {"a": "group_2", "c": "1"},
            ],
            [
                {"a": "group_3", "c": "1"},
                {"a": "group_3", "c": "1"},
            ],
        ]

        nested_df = utils.nest_fields(
            df=self.df_multirow, grouping="a", new_column="e", drop_columns=["b", "d"]
        )
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_no_drop_column(self) -> None:
        expected_column_e = [
            [
                {"a": "group_1", "b": "1", "c": "1", "d": "1"},
                {"a": "group_1", "b": "1", "c": "1", "d": "1"},
            ],
            [
                {"a": "group_2", "b": "1", "c": "1", "d": "1"},
                {"a": "group_2", "b": "1", "c": "1", "d": "1"},
            ],
            [
                {"a": "group_3", "b": "1", "c": "1", "d": "1"},
                {"a": "group_3", "b": "1", "c": "1", "d": "1"},
            ],
        ]

        nested_df = utils.nest_fields(df=self.df_multirow, grouping="a", new_column="e")
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_multirow_ValueError(self) -> None:
        with pytest.raises(ValueError, match="nested_field_is_list *"):
            utils.nest_fields(
                df=self.df_multirow,
                grouping="a",
                new_column="e",
                drop_columns=["d"],
                nested_field_is_list=False,
            )

    def test_nest_fields_singlerow_nested_list_false(self) -> None:
        expected_column_e = [
            {"a": "group_1", "b": "1", "c": "1"},
            {"a": "group_2", "b": "1", "c": "1"},
            {"a": "group_3", "b": "1", "c": "1"},
        ]

        nested_df = utils.nest_fields(
            df=self.df_singlerow,
            grouping="a",
            new_column="e",
            drop_columns=["d"],
            nested_field_is_list=False,
        )
        assert list(nested_df["e"]) == expected_column_e


class TestCalculateDistribution:
    # NOTE: pd.describe() calls np.quantile() with interpolation when quantiles fall between values.
    # We calculate the expected quartile values on this data by calling np.quantile() on manually-
    # broken out groups. Then the min/max values are calculated as <quartile> +/- 1.5*IQR.
    df = pd.DataFrame(
        {
            "col_1": [
                "a",
                "a",
                "a",
                "a",
                "a",
                "b",
                "c",
                "c",
                "c",
                "c",
                "c",
                "c",
            ],  # 3 main groups
            "col_2": [
                "x",
                "x",
                "y",
                "y",
                "y",
                "x",
                "x",
                "x",
                "x",
                "y",
                "y",
                "y",
            ],  # 2 subgroups
            "col_3": [1, 5, 10, 12, 14, 2, 6, 7, 9, 16, 17, 19],  # Values of interest
            "col_4": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
            ],  # Ignored column of values
            "col_5": [
                "m",
                "m",
                "n",
                "n",
                "o",
                "o",
                "o",
                "p",
                "p",
                "p",
                "q",
                "q",
            ],  # an ignored column of strings
        }
    )

    # Stats on "col_3", grouped by "col_1" only
    def test_calculate_distribution_one_group(self) -> None:
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "b", "c"],
                "min": [-5.5, 2.0, -6.375],
                "max": [22.5, 2.0, 30.625],
                "first_quartile": [5.0, 2.0, 7.5],
                "median": [10.0, 2.0, 12.5],
                "third_quartile": [12.0, 2.0, 16.75],
            }
        )
        output_df = utils.calculate_distribution(
            df=self.df, grouping="col_1", distribution_column="col_3"
        )
        assert output_df.equals(expected_df)

    # Stats on "col_3", grouped by "col_1" and "col_2"
    def test_calculate_distribution_two_groups(self) -> None:
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "a", "b", "c", "c"],
                "col_2": ["x", "y", "x", "x", "y"],
                "min": [-1.0, 8.0, 2.0, 4.25, 14.25],
                "max": [7.0, 16.0, 2.0, 10.25, 20.25],
                "first_quartile": [2.0, 11.0, 2.0, 6.5, 16.5],
                "median": [3.0, 12.0, 2.0, 7.0, 17.0],
                "third_quartile": [4.0, 13.0, 2.0, 8.0, 18.0],
            }
        )
        output_df = utils.calculate_distribution(
            df=self.df, grouping=["col_1", "col_2"], distribution_column="col_3"
        )
        assert output_df.equals(expected_df)


class TestSplitDelimitedFieldToMultipleRows:
    """Tests the split_delimited_field_to_multiple_rows function with the 'delim' argument being either a string or a
    compiled regex. It also tests the case where this function is called but no values need to be split up.
    """

    expected_df = pd.DataFrame(
        {
            "col_1": ["a", "a", "b", "b", "d", "c", "d", "e f", "g"],
            "col_2": ["x", "y", "z", "x", "y", "x", "x", "x", "x"],
            "col_3": ["1", "2", "3", "4", "2", "4", "4", "4", "4"],
        }
    )

    def test_split_delimited_field_to_multiple_rows_with_character_delim(self) -> None:
        input_df = pd.DataFrame(
            {
                "col_1": ["a", "a;d", "b;", "b;c;d;e f;g"],  # 'Ensembl IDs'
                "col_2": ["x", "y", "z", "x"],  # 3 'biodomains'
                "col_3": ["1", "2", "3", "4"],  # 4 'go_terms'
            }
        )

        output = utils.split_delimited_field_to_multiple_rows(
            df=input_df, split_field="col_1", delim=";"
        )

        assert output.equals(self.expected_df)

    def test_split_delimited_field_to_multiple_rows_with_string_delim(self) -> None:
        # The ", " pattern should split on the ", " but not the space in "e f"
        input_df = pd.DataFrame(
            {
                "col_1": ["a", "a, d", "b, ", "b, c, d, e f, g"],  # 'Ensembl IDs'
                "col_2": ["x", "y", "z", "x"],  # 3 'biodomains'
                "col_3": ["1", "2", "3", "4"],  # 4 'go_terms'
            }
        )

        output = utils.split_delimited_field_to_multiple_rows(
            df=input_df, split_field="col_1", delim=", "
        )

        assert output.equals(self.expected_df)

    def test_split_delimited_field_to_multiple_rows_with_regex_delim(self) -> None:
        input_df = pd.DataFrame(
            {
                "col_1": ["a", "a;d", "b-", "b_c_d;e f;g"],  # 'Ensembl IDs'
                "col_2": ["x", "y", "z", "x"],  # 3 'biodomains'
                "col_3": ["1", "2", "3", "4"],  # 4 'go_terms'
            }
        )

        pattern = re.compile("[;_-]")
        output = utils.split_delimited_field_to_multiple_rows(
            df=input_df, split_field="col_1", delim=pattern
        )

        assert output.equals(self.expected_df)

    def test_split_delimited_field_to_multiple_rows_with_no_split(self) -> None:
        input_df = self.expected_df.copy()
        output = utils.split_delimited_field_to_multiple_rows(
            df=input_df, split_field="col_1", delim=";"
        )

        assert output.equals(self.expected_df)
