from typing import Any
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest

from agoradatatools import process
from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.etl import load, utils, extract
from agoradatatools.reporter import DatasetReport, ADTGXReporter
from agoradatatools.run_platform import Platform
from agoradatatools.gx import GreatExpectationsRunner

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

    dataset_object_gx_enabled = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
            "gx_enabled": True,
        }
    }

    def setup_method(self, syn):
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
        self.patch_load = patch.object(load, "load", return_value=("syn123", 1)).start()
        self.patch_custom_transform = patch.object(
            process, "apply_custom_transformations", return_value=pd.DataFrame
        ).start()
        self.patch_dict_to_json = patch.object(
            load, "dict_to_json", return_value="path/to/json"
        ).start()
        self.patch_gx_runner_run = patch.object(
            GreatExpectationsRunner,
            "run",
        ).start()
        self.patch_set_attributes = patch.object(
            DatasetReport, "set_attributes"
        ).start()
        self.patch_format_link = patch.object(
            DatasetReport, "format_link", return_value="test_link"
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
        self.patch_gx_runner_run.stop()
        self.patch_set_attributes.stop()
        self.patch_format_link.stop()
        mock.patch.stopall()

    def test_process_dataset_upload_false_gx_disabled(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_disabled_column_rename(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_col_rename,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_disabled_custom_transformations(
        self, syn: Any
    ):
        process.process_dataset(
            dataset_obj=self.dataset_object_custom_transform,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
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
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    # This test looks like a duplicate of test_process_dataset_upload_false_gx_disabled
    # but it uses the agora_rename configuration with the same util function
    def test_process_dataset_upload_false_gx_disabled_with_agora_rename(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_col_rename,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_disabled_type_dict(self, syn: Any):
        self.patch_standardize_values.return_value = dict()
        process.process_dataset(
            dataset_obj=self.dataset_object,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_not_called()
        self.patch_dict_to_json.assert_called_once_with(
            df={}, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_true_gx_disabled(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_called_once_with(
            file_path=self.patch_dict_to_json.return_value,
            provenance=self.dataset_object["neuropath_corr"]["provenance"],
            destination=self.dataset_object["neuropath_corr"]["destination"],
            syn=syn,
        )

    def test_process_dataset_upload_true_gx_enabled(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_gx_enabled,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_called_once()
        self.patch_set_attributes.assert_called()
        self.patch_format_link.assert_called()
        self.patch_load.assert_called_once_with(
            file_path=self.patch_dict_to_json.return_value,
            provenance=self.dataset_object["neuropath_corr"]["provenance"],
            destination=self.dataset_object["neuropath_corr"]["destination"],
            syn=syn,
        )

    def test_process_dataset_upload_false_gx_enabled(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_gx_enabled,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_get_entity_as_df.assert_called_once_with(
            syn_id="syn1111111", source="csv", syn=syn
        )
        self.patch_standardize_column_names.assert_called_once_with(
            df=self.patch_get_entity_as_df.return_value
        )
        self.patch_standardize_values.assert_called_once_with(
            df=self.patch_standardize_column_names.return_value
        )
        self.patch_rename_columns.assert_not_called()
        self.patch_custom_transform.assert_not_called()
        self.patch_df_to_json.assert_called_once_with(
            df=pd.DataFrame, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_called_once()
        self.patch_set_attributes.assert_called()
        self.patch_format_link.assert_called()
        self.patch_load.assert_not_called()


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
    config_path = "./path/to/config"
    test_reporter = DatasetReport(
        data_set="test_dataset",
        gx_report_file="syn123",
        gx_report_version=1,
        gx_report_link="test_link",
        gx_failures=False,
        gx_failure_message="test_message",
        adt_output_file="syn456",
        adt_output_version=1,
        adt_output_link="test_link",
    )

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.patch_get_config = patch.object(
            utils,
            "_get_config",
            return_value={
                "destination": "destination",
                "gx_folder": GX_FOLDER,
                "gx_table": "syn321",
                "staging_path": STAGING_PATH,
                "datasets": [{"a": {"b": "c"}}, {"d": {"e": "f"}}, {"g": {"h": "i"}}],
            },
        ).start()
        self.patch_create_temp_location = patch.object(
            load, "create_temp_location", return_value=None
        ).start()
        self.patch_process_dataset = patch.object(
            process, "process_dataset", return_value=self.test_reporter
        ).start()
        self.patch_add_report = patch.object(
            ADTGXReporter,
            "add_report",
        ).start()
        self.patch_format_link = patch.object(
            DatasetReport, "format_link", return_value="test_link"
        ).start()
        self.patch_create_data_manifest = patch.object(
            process,
            "create_data_manifest",
            return_value=pd.DataFrame({"id": ["a", "b", "c"]}),
        ).start()
        self.patch_df_to_csv = patch.object(
            load, "df_to_csv", return_value="path/to/csv"
        ).start()
        self.patch_load = patch.object(load, "load", return_value=("syn123", 1)).start()
        self.patch_update_table = patch.object(
            ADTGXReporter,
            "update_table",
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_process_all_files_upload_false(self, syn: Any):
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            run_id="123",
            upload=False,
        )
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"a": {"b": "c"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"d": {"e": "f"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"g": {"h": "i"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )
        self.patch_add_report.assert_any_call(self.patch_process_dataset.return_value)
        self.patch_create_data_manifest.assert_called_once_with(
            parent="destination", syn=syn
        )
        self.patch_df_to_csv.assert_called_once_with(
            df=self.patch_create_data_manifest.return_value,
            staging_path=STAGING_PATH,
            filename="data_manifest.csv",
        )
        self.patch_load.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_true(self, syn: Any):
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            run_id="123",
            upload=True,
        )
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"a": {"b": "c"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"d": {"e": "f"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"g": {"h": "i"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )
        self.patch_add_report.assert_any_call(self.patch_process_dataset.return_value)
        self.patch_create_data_manifest.assert_called_once_with(
            parent="destination", syn=syn
        )
        self.patch_df_to_csv.assert_called_once_with(
            df=self.patch_create_data_manifest.return_value,
            staging_path=STAGING_PATH,
            filename="data_manifest.csv",
        )
        self.patch_load.assert_called_once_with(
            file_path="path/to/csv",
            provenance=["a", "b", "c"],
            destination="destination",
            syn=syn,
        )
        self.patch_format_link.assert_called_once_with(syn_id="syn123", version=1)
        self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_false_gx_failure(self, syn: Any):
        with pytest.raises(
            ADTDataProcessingError,
            match="\nData Processing has failed for one or more data sources. Refer to the list of errors below to address issues:\na: test_message\nd: test_message\ng: test_message",
        ):
            self.patch_process_dataset.return_value.gx_failures = True
            process.process_all_files(
                syn=syn,
                config_path=self.config_path,
                platform=Platform.LOCAL,
                run_id="123",
                upload=False,
            )
            self.patch_get_config.assert_called_once_with(config_path=self.config_path)
            self.patch_create_temp_location.assert_called_once_with(
                staging_path=STAGING_PATH
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"a": {"b": "c"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"d": {"e": "f"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"g": {"h": "i"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_add_report.assert_any_call(
                self.patch_process_dataset.return_value
            )
            self.patch_create_data_manifest.assert_not_called()
            self.patch_df_to_csv.assert_not_called()
            self.patch_load.assert_not_called()
            self.patch_format_link.assert_not_called()
            self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_false_process_dataset_fail(self, syn: Any):
        with pytest.raises(ADTDataProcessingError, match="test"):
            self.patch_process_dataset.side_effect = Exception("test")
            process.process_all_files(
                syn=syn,
                config_path=self.config_path,
                platform=Platform.LOCAL,
                run_id="123",
                upload=False,
            )
            self.patch_get_config.assert_called_once_with(config_path=self.config_path)
            self.patch_create_temp_location.assert_called_once_with(
                staging_path=STAGING_PATH
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"a": {"b": "c"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"d": {"e": "f"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_process_dataset.assert_any_call(
                dataset_obj={"g": {"h": "i"}},
                staging_path=STAGING_PATH,
                gx_folder=GX_FOLDER,
                syn=syn,
                upload=False,
            )
            self.patch_add_report.assert_any_call(
                self.patch_process_dataset.return_value
            )
            self.patch_create_data_manifest.assert_not_called()
            self.patch_df_to_csv.assert_not_called()
            self.patch_load.assert_not_called()
            self.patch_format_link.assert_not_called()
            self.patch_update_table.assert_called_once()
