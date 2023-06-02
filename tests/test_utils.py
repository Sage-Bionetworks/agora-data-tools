from unittest import mock
from unittest.mock import patch

import pytest
import synapseclient
import sys
import yaml

import numpy as np
import pandas as pd

from io import StringIO

from agoradatatools.etl import utils


class TestLoginToSynapse:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.patch_synapseclient = patch.object(
            synapseclient, "Synapse", return_value=syn
        ).start()
        self.patch_syn_login = patch.object(syn, "login", return_value=syn).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_login_with_token(self):
        utils._login_to_synapse(token="my_auth_token")
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with(authToken="my_auth_token")

    def test_login_no_token(self):
        utils._login_to_synapse(token=None)
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with()


def test_get_config_with_invalid_file_path():
    with pytest.raises(FileNotFoundError, match="File not found. *"):
        utils._get_config(config_path="this/is/a/bad/path")


def test_get_config_with_parser_error():
    with pytest.raises(
        yaml.parser.ParserError, match="YAML file unable to be parsed. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_parsing.yaml")


def test_get_config_with_scanner_error():
    with pytest.raises(
        yaml.scanner.ScannerError, match="YAML file unable to be scanned. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_scanning.yaml")


def test_get_config_with_no_config_path():
    config = utils._get_config(config_path=None)
    assert list(config)[0] == {"destination": "syn12177492"}


def test_get_config_with_config_path():
    config = utils._get_config(config_path="./test_config.yaml")
    assert list(config)[0] == {"destination": "syn17015333"}


def test_find_config_by_name_where_name_in_config():
    config = [{"a": "b"}, {"c", "d"}]
    returned_object = utils._find_config_by_name(config=config, name="a")
    assert returned_object is not None


def test_find_config_by_name_where_name_not_in_config():
    config = [{"a": "b"}, {"c": "d"}]
    returned_object = utils._find_config_by_name(config=config, name="z")
    assert returned_object is None


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
