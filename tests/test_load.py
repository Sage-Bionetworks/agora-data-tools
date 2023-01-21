import os
from os import path
from unittest import mock
from unittest.mock import patch, ANY

import numpy as np
import pytest
import pandas as pd
import json

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
        "l": None,  # is none
        "m": "n",  # is anything else
    }
    cleaned_dict = load.remove_non_values(example_dict)
    assert isinstance(cleaned_dict, dict)
    assert cleaned_dict["a"] == example_dict["a"]
    assert cleaned_dict["d"] == {"e": "f"}
    assert cleaned_dict["h"] == example_dict["h"]
    assert cleaned_dict["m"] == example_dict["m"]
    assert cleaned_dict.get("l") is None


# class TestLoad:
#     @pytest.fixture(scope="function", autouse=True)
#     def setup_method(self, syn):
#         self.patch_syn_login = patch.object(
#             utils, "_login_to_synapse", return_value=syn
#         ).start()
#         # self.patch_syn_store = patch.object(
#         #     syn, "store", return_value =
#         # )

#     def teardown_method(self):
#         mock.patch.stopall()

#     def test_load_syn_is_none(self):
#         test_tuple = load.load(
#             file_path="path/to/file",
#             provenance=["syn1111111", "syn1111112"],
#             destination="synsyn1111113",
#             syn=None,
#         )
#         self.patch_syn_login.assert_called_once()
#         assert test_tuple == (1, 2)


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

    def test_df_to_json_failure(self):
        json_name = load.df_to_json(
            df=pd.DataFrame(), staging_path=1, filename="test.json"
        )
        self.patch_replace.assert_called_once_with({np.nan: None})
        self.patch_to_dict.assert_called_once_with(orient="records")
        self.patch_json_dump.assert_not_called()  # should fail at the open() step
        assert json_name is None


# assert type(load.df_to_json(1, "test.json")) is type(None)


# def test_load():
#     path = "./tests/test_assets/test.json"
#     bad_path = "./tests/test_assets/invalid_path.json"
#     bad_used = ["", "xxx"]
#     used = ["syn25721515", "syn25721521"]
#     destination = "syn25871921"
#     bad_destination = "s923484y23"

#     # good_result = load.load(file_path=path, provenance=used, destination=destination)
#     # assert type(good_result) is tuple
#     # assert good_result[0] == 'syn25871925'

#     bad_path_result = load.load(
#         file_path=bad_path, provenance=used, destination=destination
#     )
#     assert type(bad_path_result) is type(None)

#     bad_used_result = load.load(
#         file_path=path, provenance=bad_used, destination=destination
#     )
#     assert type(bad_used_result) is type(None)

#     bad_destination = load.load(
#         file_path=path, provenance=used, destination=bad_destination
#     )
#     assert type(bad_used_result) is type(None)
