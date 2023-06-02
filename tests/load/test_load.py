import json
import os
import sys
from io import StringIO
from unittest import mock
from unittest.mock import ANY, patch

import numpy as np
import pandas as pd
import pytest
from synapseclient import File

from agoradatatools.etl import load, utils


def test_create_temp_location_success():
    load.create_temp_location(staging_path="./test_staging_dir")
    assert os.path.exists("./test_staging_dir")
    os.rmdir("./test_staging_dir")


def test_delete_temp_location():
    os.mkdir("./test_staging_dir")
    load.delete_temp_location(staging_path="./test_staging_dir")
    assert not os.path.exists("./test_staging_dir")


def test_remove_non_values():
    example_dict = {
        "a": {"b": "c"},  # is dictionary
        "d": {"e": "f", "g": None},  # is dictinoary with nested none
        "h": ["i", "j", "k"],  # is list
        "l": ["m", "n", {"o": None}],  # is list with nested dict
        "q": None,  # is none
        "r": "s",  # is anything else
    }
    cleaned_dict = load.remove_non_values(example_dict)
    assert isinstance(cleaned_dict, dict)
    assert cleaned_dict["a"] == example_dict["a"]
    assert cleaned_dict["d"] == {"e": "f"}
    assert cleaned_dict["h"] == example_dict["h"]
    assert cleaned_dict["l"] == ["m", "n"]
    assert "q" not in cleaned_dict.keys()
    assert cleaned_dict["r"] == example_dict["r"]


class TestLoad:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.patch_syn_login = patch.object(
            utils, "_login_to_synapse", return_value=syn
        ).start()
        self.patch_syn_store = patch.object(
            syn,
            "store",
            return_value=File(
                "fake/path/to/fake/file",
                parent="syn1111113",
                id="syn1111114",
                versionNumber=1,
            ),
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_load_syn_is_none(self):
        test_tuple = load.load(
            file_path="fake/path/to/fake/file",
            provenance=["syn1111111", "syn1111112"],
            destination="syn1111113",
            syn=None,
        )
        self.patch_syn_login.assert_called_once()
        assert test_tuple == ("syn1111114", 1)

    def test_load_syn_is_not_none(self, syn):
        test_tuple = load.load(
            file_path="fake/path/to/fake/file",
            provenance=["syn1111111", "syn1111112"],
            destination="syn1111113",
            syn=syn,
        )
        self.patch_syn_login.assert_not_called()
        assert test_tuple == ("syn1111114", 1)


class TestDFToJSON:
    def setup_method(self):
        self.patch_replace = patch.object(
            pd.DataFrame, "replace", return_value=pd.DataFrame()
        ).start()
        self.patch_to_dict = patch.object(
            pd.DataFrame, "to_dict", return_value=dict()
        ).start()
        self.patch_json_dump = patch.object(json, "dump", return_value=None).start()
        self.patch_NumpyEncoder = patch.object(load, "NumpyEncoder").start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_df_to_json_success(self):
        json_name = load.df_to_json(
            df=pd.DataFrame(), staging_path="./staging", filename="test.json"
        )
        self.patch_replace.assert_called_once_with({np.nan: None})
        self.patch_to_dict.assert_called_once_with(orient="records")
        self.patch_json_dump.assert_called_once_with(
            self.patch_to_dict.return_value,
            ANY,  # without mocking open() I was unable to get anything equating to `temp_json` to go here, it was failing even when the 'Expected call' and the 'Actual call' appeared to match perfectly
            cls=self.patch_NumpyEncoder,
            indent=2,
        )
        assert json_name == "./staging/test.json"


class TestDFToCSV:
    def setup_method(self):
        self.patch_to_csv = patch.object(
            pd.DataFrame, "to_csv", return_value=dict()
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_df_to_csv_success(self):
        csv_name = load.df_to_csv(
            df=pd.DataFrame(), staging_path="./staging", filename="test.json"
        )
        self.patch_to_csv.assert_called_once_with(
            path_or_buf=ANY,  # without mocking open() I was unable to get anything equating to `temp_json` to go here
            index=False,
        )
        assert csv_name == "./staging/test.json"


class TestDictToJSON:
    df_dict = {"a": "b", "c": {"d": "e"}}

    def setup_method(self):
        self.patch_remove_non_values = patch.object(
            load, "remove_non_values", return_value=dict()
        ).start()
        self.patch_json_dump = patch.object(json, "dump", return_value=None).start()
        self.patch_NumpyEncoder = patch.object(load, "NumpyEncoder").start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_dict_to_json_success(self):
        json_name = load.dict_to_json(
            df=self.df_dict, staging_path="./staging", filename="test.json"
        )
        self.patch_remove_non_values.assert_called_once_with({"d": "e"})
        self.patch_json_dump.assert_called_once_with(
            [{"a": "b", "c": {}}],
            ANY,  # without mocking open() I was unable to get anything equating to `temp_json` to go here
            cls=self.patch_NumpyEncoder,
            indent=2,
        )
        assert json_name == "./staging/test.json"
