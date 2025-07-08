from typing import Any, Callable, ContextManager, Dict
from unittest import mock
from unittest.mock import patch
from agoradatatools.etl import transform
from contextlib import nullcontext as does_not_raise
import pandas as pd
import pytest

from synapseclient import File

from agoradatatools import process
from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.etl import load, utils, extract
from agoradatatools.reporter import DatasetReport, ADTGXReporter
from agoradatatools.constants import Platform
from agoradatatools.gx import GreatExpectationsRunner


STAGING_PATH = "./staging"
GX_FOLDER = "test_folder"


class TestUploadDataversionMetadata:
    file_id = "syn1111111"
    file_version = "1"
    team_images_id = "syn12861877"
    destination = "syn1111113"
    dataversion_dict_with_team_images_id = {
        "data_file": file_id,
        "data_version": file_version,
        "team_images_id": team_images_id,
    }
    dataversion_dict_without_team_images_id = {
        "data_file": file_id,
        "data_version": file_version,
    }

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.patch_dict_to_json = patch.object(
            load, "dict_to_json", return_value="path/to/json"
        ).start()
        self.patch_load = patch.object(load, "load", return_value=("syn123", 1)).start()

    def test_upload_dataversion_metadata_with_team_images_id(self, syn: Any):
        # WHEN I call upload_dataversion_metadata with a team_images_id
        process.upload_dataversion_metadata(
            syn=syn,
            file_id=self.file_id,
            file_version=self.file_version,
            team_images_id=self.team_images_id,
            staging_path=STAGING_PATH,
            destination=self.destination,
        )
        # THEN I expect the dict_to_json function to be called with the correct arguments
        self.patch_dict_to_json.assert_called_once_with(
            df=self.dataversion_dict_with_team_images_id,
            staging_path=STAGING_PATH,
            filename="dataversion.json",
        )
        # AND I expect the load function to be called with the correct arguments
        self.patch_load.assert_called_once_with(
            file_path="path/to/json",
            provenance=[self.file_id],
            destination=self.destination,
            syn=syn,
        )

    def test_upload_dataversion_metadata_without_team_images_id(self, syn: Any):
        # WHEN I call upload_dataversion_metadata without a team_images_id
        process.upload_dataversion_metadata(
            syn=syn,
            file_id=self.file_id,
            file_version=self.file_version,
            staging_path=STAGING_PATH,
            destination=self.destination,
            team_images_id=None,
        )
        # THEN I expect the dict_to_json function to be called with the correct arguments
        self.patch_dict_to_json.assert_called_once_with(
            df=self.dataversion_dict_without_team_images_id,
            staging_path=STAGING_PATH,
            filename="dataversion.json",
        )
        # AND I expect the load function to be called with the correct arguments
        self.patch_load.assert_called_once_with(
            file_path="path/to/json",
            provenance=[self.file_id],
            destination=self.destination,
            syn=syn,
        )


class TestApplyCustomTransformations:
    """Test the apply_custom_transformations function."""

    @pytest.fixture(scope="function", autouse=True)
    def standard_transform_function(
        self,
    ) -> Callable[[pd.DataFrame, str, Dict[str, pd.DataFrame]], pd.DataFrame]:
        """mock simple transform function that uses standard parameters such as dataframe, dataset_name, datasets"""

        def _standard_transform_function(
            df: pd.DataFrame, dataset_name: str, datasets: Dict[str, pd.DataFrame]
        ) -> pd.DataFrame:
            """mock simple transform function"""
            if dataset_name in datasets:
                aggregated_value = datasets[dataset_name]["value_column"].sum()
                df["aggregated_value"] = aggregated_value
            return df.assign(new_key=dataset_name + "_new_key")

        return _standard_transform_function

    @pytest.fixture(scope="function", autouse=True)
    def special_transform_function(
        self,
    ) -> Callable[[pd.DataFrame, int, Dict[str, pd.DataFrame]], pd.DataFrame]:
        """mock transform function that uses additional parameters"""

        def _mock_transform_with_args(
            df: pd.DataFrame, test_threshold: int
        ) -> pd.DataFrame:
            """mock transform function that takes test_threshold as an argument"""
            return df.assign(new_key=test_threshold)

        return _mock_transform_with_args

    @pytest.mark.parametrize(
        "example_config_with_custom_transform,expected_error",
        # Fails because mock_transform_function_false doesn't exist
        [
            (
                {
                    "neuropath_corr": {
                        "files": [
                            {"name": "test_file_1", "id": "syn1111111", "format": "csv"}
                        ],
                        "final_format": "json",
                        "provenance": ["syn1111111"],
                        "destination": "syn1111113",
                        "gx_enabled": False,
                        "custom_transformations": {
                            "mock_transform_function_false": {
                                "test_threshold": 1,
                                "test_p_value": 2,
                            }
                        },
                    }
                },
                pytest.raises(AttributeError, match="mock_transform_function_false"),
            ),
            # Fails because mock_transform_function_false doesn't exist
            (
                {
                    "neuropath_corr": {
                        "files": [
                            {"name": "test_file_1", "id": "syn1111111", "format": "csv"}
                        ],
                        "final_format": "json",
                        "provenance": ["syn1111111"],
                        "destination": "syn1111113",
                        "gx_enabled": False,
                        "custom_transformations": "mock_transform_function_false",
                    },
                },
                pytest.raises(AttributeError, match="mock_transform_function_false"),
            ),
            # Fails because custom_transformation is mapped to an integer, not a function
            (
                {
                    "neuropath_corr": {
                        "files": [
                            {"name": "test_file_1", "id": "syn1111111", "format": "csv"}
                        ],
                        "final_format": "json",
                        "provenance": ["syn1111111"],
                        "destination": "syn1111113",
                        "gx_enabled": False,
                        "custom_transformations": 1,
                    },
                },
                pytest.raises(
                    TypeError,
                    match="Custom transformation in the config for dataset 'neuropath_corr' should be mapped to a function name with custom parameters if needed. Received: int",
                ),
            ),
        ],
        ids=[
            "invalid_custom_transformations_with_special_params",
            "invalid_custom_transformations_with_standard_params",
            "invalid_type",
        ],
    )
    def test_apply_invalid_custom_transformations(
        self,
        example_config_with_custom_transform: Dict[str, Any],
        expected_error: Exception,
    ) -> None:
        """Test that invalid custom transformations raise an error."""
        with expected_error:
            process.apply_custom_transformations(
                datasets={"test_file_1": pd.DataFrame()},
                dataset_name="neuropath_corr",
                dataset_obj=example_config_with_custom_transform["neuropath_corr"],
            )

    def test_apply_invalid_multi_custom_transformations(
        self,
        special_transform_function: Callable[
            [pd.DataFrame, str, Dict[str, pd.DataFrame]], pd.DataFrame
        ],
    ):
        """Test that when the config file contains multiple custom transformations, the first one gets used and a warning is raised"""
        example_config_with_custom_transform = {
            "neuropath_corr": {
                "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
                "final_format": "json",
                "provenance": ["syn1111111"],
                "destination": "syn1111113",
                "gx_enabled": False,
                "custom_transformations": {
                    "special_transform_function": {"test_threshold": 1},
                    "mock_transform_function_false": {
                        "test_threshold": 1,
                        "test_p_value": 2,
                    },
                },
            },
        }
        with pytest.warns(
            UserWarning,
            match="Please provide a single custom transformation function in the configuration file. * ",
        ):
            with patch.object(
                transform,
                "special_transform_function",
                special_transform_function,
                create=True,
            ):
                process.apply_custom_transformations(
                    datasets={"test_file_1": pd.DataFrame()},
                    dataset_name="neuropath_corr",
                    dataset_obj=example_config_with_custom_transform["neuropath_corr"],
                )

    @pytest.mark.parametrize(
        "example_config_with_custom_transform, function_name, expectation, transformed_df",
        [
            (
                # valid custom transformations with standard parameters
                {
                    "neuropath_corr": {
                        "files": [
                            {"name": "test_file_1", "id": "syn1111111", "format": "csv"}
                        ],
                        "final_format": "json",
                        "provenance": ["syn1111111"],
                        "destination": "syn1111113",
                        "gx_enabled": False,
                        "custom_transformations": "standard_transform_function",
                    }
                },
                "standard_transform_function",
                does_not_raise(),
                pd.DataFrame(
                    {
                        "test_key": ["test_value1", "test_value2"],
                        "value_column": [1, 2],
                        "aggregated_value": [3, 3],
                        "new_key": ["test_file_1_new_key", "test_file_1_new_key"],
                    }
                ),
            ),
            (
                # valid custom transformations with additional parameters
                {
                    "neuropath_corr": {
                        "files": [
                            {"name": "test_file_1", "id": "syn1111111", "format": "csv"}
                        ],
                        "final_format": "json",
                        "provenance": ["syn1111111"],
                        "destination": "syn1111113",
                        "gx_enabled": False,
                        "custom_transformations": {
                            "special_transform_function": {"test_threshold": 1}
                        },
                    }
                },
                "special_transform_function",
                does_not_raise(),
                pd.DataFrame(
                    {
                        "test_key": ["test_value1", "test_value2"],
                        "value_column": [1, 2],
                        "new_key": [1, 1],
                    }
                ),
            ),
        ],
        ids=[
            "valid_custom_transformations_standard_params",
            "valid_custom_transformations_special_params",
        ],
    )
    def test_apply_valid_custom_transformations(
        self,
        standard_transform_function: Callable[
            [pd.DataFrame, str, Dict[str, pd.DataFrame]], pd.DataFrame
        ],
        special_transform_function: Callable[
            [pd.DataFrame, str, Dict[str, pd.DataFrame]], pd.DataFrame
        ],
        function_name: str,
        example_config_with_custom_transform: Dict[str, Any],
        expectation: ContextManager[None],
        transformed_df: pd.DataFrame,
    ) -> None:
        """Test that transformations are applied correctly when a valid transformation function is provided."""

        if function_name == "special_transform_function":
            mocked_transform = special_transform_function
        else:
            mocked_transform = standard_transform_function
        with patch.object(transform, function_name, mocked_transform, create=True):
            with expectation:
                df_transformed = process.apply_custom_transformations(
                    datasets={
                        "test_file_1": pd.DataFrame(
                            {
                                "test_key": ["test_value1", "test_value2"],
                                "value_column": [1, 2],
                            }
                        ),
                    },
                    dataset_name="test_file_1",
                    dataset_obj=example_config_with_custom_transform["neuropath_corr"],
                )
                assert df_transformed.equals(transformed_df)


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

    dataset_object_gx_disabled = {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111"],
            "destination": "syn1111113",
            "gx_enabled": False,
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
        self.patch_load = patch.object(load, "load", return_value=("syn123", 1)).start()
        self.patch_custom_transform = patch.object(
            process, "apply_custom_transformations", return_value=pd.DataFrame()
        ).start()
        self.patch_convert_transformation_result_to_dataframe = patch.object(
            utils,
            "convert_transformation_result_to_dataframe",
            return_value=pd.DataFrame(),
        ).start()
        self.patch_dict_to_json = patch.object(
            load, "dict_to_json", return_value="path/to/json"
        ).start()
        self.patch_list_to_json = patch.object(
            load, "list_to_json", return_value="path/to/json"
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
        self.patch_list_to_json.stop()
        self.patch_gx_runner_run.stop()
        self.patch_set_attributes.stop()
        self.patch_format_link.stop()
        mock.patch.stopall()

    def test_process_dataset_upload_false_gx_not_specified(self, syn: Any):
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
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_column_rename(
        self, syn: Any
    ):
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
            df=pd.DataFrame(), staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )

        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_custom_transformations(
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
        self.patch_df_to_json.assert_called_once()
        args, kwargs = self.patch_df_to_json.call_args

        assert kwargs["staging_path"] == STAGING_PATH
        assert kwargs["filename"] == "neuropath_corr.json"
        pd.testing.assert_frame_equal(kwargs["df"], pd.DataFrame())
        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    # This test looks like a duplicate of test_process_dataset_upload_false_gx_disabled
    # but it uses the agora_rename configuration with the same util function
    def test_process_dataset_upload_false_gx_not_specified_with_agora_rename(
        self, syn: Any
    ):
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
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_type_dict(self, syn: Any):
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
        self.patch_list_to_json.assert_not_called()
        self.patch_dict_to_json.assert_called_once_with(
            df={}, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_type_list(self, syn: Any):
        self.patch_standardize_values.return_value = list()
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
        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_called_once_with(
            df_as_list=[], staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_true_gx_disabled(self, syn: Any):
        process.process_dataset(
            dataset_obj=self.dataset_object_gx_disabled,
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
        self.patch_list_to_json.assert_not_called()
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
        self.patch_list_to_json.assert_not_called()
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
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_called_once()
        self.patch_set_attributes.assert_called()
        self.patch_format_link.assert_called()
        self.patch_load.assert_not_called()


class TestCreateDataManifest:
    files = [
        File(id="syn123", name="not_a_manifest", versionNumber=1),
        File(id="syn456", name="data_manifest.csv", versionNumber=1),
    ]
    manifest_rows = [
        {"id": "syn123", "version": 1},
        {"id": "syn456", "version": 2},
    ]

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn: Any):
        self.patch_get_children = patch.object(
            syn, "getChildren", return_value=self.files
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_create_data_manifest_parent_none(self, syn: Any):
        # WHEN I call create_data_manifest with a parent of None
        result = process.create_data_manifest(syn=syn, parent=None)
        # THEN I expect the result to be None
        assert result is None
        # AND I expect the getChildren method to not be called
        self.patch_get_children.assert_not_called()

    def test_create_data_manifest_with_parent(self, syn: Any):
        # WHEN I call create_data_manifest with a parent
        result_df = process.create_data_manifest(syn=syn, parent="syn1111111")
        # THEN I expect the getChildren method to be called with the parent
        self.patch_get_children.assert_called_once_with("syn1111111")
        # AND I expect the result to be a dataframe with the correct rows
        # Including incrementing the version number for the data_manifest.csv file
        pd.testing.assert_frame_equal(result_df, pd.DataFrame(self.manifest_rows))


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
                "team_images_id": "syn987",
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
        self.patch_upload_dataversion_metadata = patch.object(
            process, "upload_dataversion_metadata", return_value=None
        ).start()
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
        self.patch_upload_dataversion_metadata.assert_not_called()
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
        self.patch_upload_dataversion_metadata.assert_called_once_with(
            syn=syn,
            file_id="syn123",
            file_version=1,
            team_images_id="syn987",
            staging_path=STAGING_PATH,
            destination="destination",
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
            match="\nData Processing has failed for one or more data sources. Refer to the list of errors below to address issues:",
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
            self.patch_upload_dataversion_metadata.assert_not_called()
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
            self.patch_upload_dataversion_metadata.assert_not_called()
            self.patch_load.assert_not_called()
            self.patch_format_link.assert_not_called()
            self.patch_update_table.assert_called_once()
