from typing import Any
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest

from agoradatatools import process
from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.etl import extract, load, utils

STAGING_PATH = "./staging"
GX_FOLDER = "test_folder"


class TestProcessDataset:
    dataset_object = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
        }
    }

    dataset_object_col_rename = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
            "column_rename": {"col_1": "new_col_1", "col_2": "new_col_2"},
        }
    }

    dataset_object_custom_transform = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
            "custom_transformations": "test_transformation",
        }
    }

    dataset_object_agora_rename = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
            "agora_rename": {"col_1": "new_col_1", "col_2": "new_col_2"},
        }
    }

    def setup_method(self):
        self.patch_get_entity_as_df = patch.object(
            extract, "get_entity_as_df", return_value=pd.DataFrame
        ).start()
        self.patch_standardize_column_names = patch.object(
            utils, "standardize_column_names", return_value=pd.DataFrame
        ).start()
        self.patch_standardize_values = patch.object(
            utils, "standardize_values", return_value=pd.DataFrame
        ).start()
        self.patch_rename_columns = patch.object(
            utils, "rename_columns", return_value=pd.DataFrame
        ).start()
        self.patch_df_to_json = patch.object(
            load, "df_to_json", return_value="path/to/json"
        ).start()
        self.patch_load = patch.object(load, "load", return_value=None).start()
        self.patch_custom_transform = patch.object(
            process, "apply_custom_transformations", return_value=pd.DataFrame
        ).start()
        self.patch_dict_to_json = patch.object(
            load, "dict_to_json", return_value="path/to/json"
        ).start()

    def teardown_method(self):
        self.patch_get_entity_as_df.stop()
        self.patch_standardize_column_names.stop()
        self.patch_standardize_values.stop()
        self.patch_rename_columns.stop()
        self.patch_df_to_json.stop()
        self.patch_load.stop()
        self.patch_custom_transform.stop()
        self.patch_dict_to_json.stop()

    def test_process_dataset_with_column_rename(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_col_rename,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()
        self.patch_dict_to_json.assert_not_called()

    def test_process_dataset_custom_transformations(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_custom_transform,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_custom_transform.assert_called_once_with(
            datasets={"test_file_1": pd.DataFrame},
            dataset_name="neuropath_corr",
            dataset_obj={
                "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
                "final_format": "json",
                "provenance": ["syn1111111"],
                "destination": "syn1111113",
                "custom_transformations": "test_transformation",
            },
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_dict_to_json.assert_not_called()

    def test_process_dataset_with_agora_rename(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_col_rename,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()
        self.patch_dict_to_json.assert_not_called()

    def test_process_dataset_type_dict(self, syn: Any):
        self.patch_standardize_values.return_value = (
            dict()
        )  # test if it is a dictionary later
        process.process_dataset(
            dataset_obj=self.dataset_object,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_dict_to_json.assert_called_once_with(
            df={}, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_not_called()


class TestCreateDataManifest:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn: Any):
        self.patch_syn_login = patch.object(
            utils, "_login_to_synapse", return_value=syn
        ).start()
        self.patch_get_children = patch.object(
            syn, "getChildren", return_value=[{"id": "123", "versionNumber": 1}]
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_create_data_manifest_parent_none(self, syn: Any):
        assert process.create_data_manifest(syn=syn, parent=None) is None
        self.patch_syn_login.assert_not_called()

    def test_create_data_manifest_no_none(self, syn: Any):
        df = process.create_data_manifest(syn=syn, parent="syn1111111")
        self.patch_get_children.assert_called_once_with("syn1111111")
        self.patch_syn_login.assert_not_called()
        assert isinstance(df, pd.DataFrame)


class TestProcessAllFiles:
    CONFIG_PATH = "./path/to/config"

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.patch_get_config = patch.object(
            utils,
            "_get_config",
            return_value={
                "destination": "destination",
                "gx_folder": GX_FOLDER,
                "datasets": [{"a": {"b": "c"}}, {"d": {"e": "f"}}, {"g": {"h": "i"}}],
            },
        ).start()
        self.patch_create_temp_location = patch.object(
            load, "create_temp_location", return_value=None
        ).start()
        self.patch_process_dataset = patch.object(
            process, "process_dataset", return_value=tuple()
        ).start()
        self.patch_create_data_manifest = patch.object(
            process,
            "create_data_manifest",
            return_value=pd.DataFrame({"id": ["a", "b", "c"]}),
        ).start()
        self.patch_df_to_csv = patch.object(
            load, "df_to_csv", return_value="path/to/csv"
        ).start()
        self.patch_load = patch.object(load, "load", return_value=None).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_process_all_files_config_path(self, syn: Any):
        process.process_all_files(syn=syn, config_path=self.CONFIG_PATH)
        self.patch_get_config.assert_called_once_with(config_path=self.CONFIG_PATH)

    def test_process_all_files_no_config_path(self, syn: Any):
        process.process_all_files(syn=syn, config_path=None)
        self.patch_get_config.assert_called_once_with(config_path=None)

    def test_process_all_files_process_dataset_fails(self, syn: Any):
        with pytest.raises(ADTDataProcessingError):
            self.patch_process_dataset.side_effect = Exception
            process.process_all_files(syn=syn, config_path=self.CONFIG_PATH)
            self.patch_create_data_manifest.assert_not_called()

    def test_process_all_files_full(self, syn: Any):
        process.process_all_files(syn=syn, config_path=None)
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"a": {"b": "c"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"d": {"e": "f"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"g": {"h": "i"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
        )
        self.patch_create_data_manifest.assert_called_once_with(
            parent="destination", syn=syn
        )
        self.patch_df_to_csv.assert_called_once_with(
            df=self.patch_create_data_manifest.return_value,
            staging_path=STAGING_PATH,
            filename="data_manifest.csv",
        )
