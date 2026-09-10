from typing import Any, Callable, ContextManager, Dict
from unittest import mock
from unittest.mock import patch
from agoradatatools.etl import transform
from contextlib import nullcontext as does_not_raise
import pandas as pd
import pytest

from synapseclient import File
from typer.testing import CliRunner

from agoradatatools import process
from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.etl import load, utils, extract
from agoradatatools.reporter import DatasetReport, ADTGXReporter
from agoradatatools.constants import Platform
from agoradatatools.gx import GreatExpectationsRunner
import synapseclient


STAGING_PATH = "./staging"
GX_FOLDER = "test_folder"


@pytest.fixture
def dataset_object_no_provenance():
    """Dataset object without provenance configuration."""
    return {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "destination": "syn1111113",
            "gx_enabled": False,
        }
    }


@pytest.fixture
def dataset_provenance_file_ids_mismatch():
    """Dataset object with mismatched provenance and file IDs."""
    return {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111.1", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn1111111.18"],
            "destination": "syn1111113",
            "gx_enabled": False,
        }
    }


@pytest.fixture
def dataset_object_with_provenance():
    """Dataset object with provenance configuration."""
    return {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn11111145"],
            "destination": "syn1111113",
            "gx_enabled": False,
        }
    }


@pytest.fixture
def dataset_object_with_duplicated_provenance():
    """Dataset object with duplicated provenance IDs."""
    return {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn11111145", "format": "csv"}],
            "final_format": "json",
            "provenance": ["syn11111145", "syn11111145"],
            "destination": "syn1111113",
            "gx_enabled": False,
        }
    }


@pytest.fixture
def dataset_object_provenance_mix_list():
    """Dataset object with mixed provenance configuration."""
    return {
        "neuropath_corr": {
            "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
            "final_format": "json",
            "destination": "syn1111113",
            "provenance": [
                ["syn11111145", "syn11111145"],
                ["syn11111146"],
                "syn11111147",
            ],
            "gx_enabled": False,
        }
    }


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

    def test_upload_dataversion_metadata_with_team_images_id(
        self, syn: synapseclient.Synapse
    ):
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
            data_as_dict=self.dataversion_dict_with_team_images_id,
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

    def test_upload_dataversion_metadata_without_team_images_id(
        self, syn: synapseclient.Synapse
    ):
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
            data_as_dict=self.dataversion_dict_without_team_images_id,
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


class TestCheckProvenanceIdFileIdConsistency:
    """Test suite for check_provenance_id_file_id_consistency function."""

    def test_no_provenance_ids(self):
        """Test that empty provenance list returns without error."""
        file_ids = ["syn123.4", "syn456.2"]
        provenance_ids = []

        # Should not raise any exception
        process.check_provenance_id_file_id_consistency(provenance_ids, file_ids)

    def test_both_lists_empty(self):
        """Test with both lists empty."""
        file_ids = []
        provenance_ids = []

        # Should not raise
        process.check_provenance_id_file_id_consistency(provenance_ids, file_ids)

    def test_matching_versions(self):
        """Test that matching versions pass validation."""
        file_ids = ["syn123.4", "syn456.2"]
        provenance_ids = ["syn123.4", "syn789.1"]

        # Should not raise any exception
        process.check_provenance_id_file_id_consistency(provenance_ids, file_ids)

    def test_no_overlap_between_ids(self):
        """Test that non-overlapping IDs pass validation."""
        file_ids = ["syn123.4", "syn456.2"]
        provenance_ids = ["syn789.1", "syn999.3"]  # No overlap

        # Should not raise any exception
        process.check_provenance_id_file_id_consistency(provenance_ids, file_ids)

    def test_version_mismatch_raises_error(self):
        """Test that different versions for same entity raise ValueError."""
        file_ids = ["syn123.4", "syn456.2"]
        provenance_ids = ["syn123.5", "syn789.1"]  # syn123 versions differ

        with pytest.raises(ValueError) as exc_info:
            process.check_provenance_id_file_id_consistency(provenance_ids, file_ids)

        assert "Version mismatch" in str(exc_info.value)
        assert "syn123.5" in str(exc_info.value)
        assert "syn123.4" in str(exc_info.value)


class TestGetProvenanceIds:
    def test_get_provenance_ids_no_provenance(
        self, dataset_object_no_provenance: dict[str, Any]
    ) -> None:
        """Test that when no provenance is provided in the config, only file ids are returned."""
        file_ids = ["syn1111111"]
        expected_provenance_ids = ["syn1111111"]
        provenance_ids = process.get_provenance_ids(
            dataset_object_no_provenance,
            dataset_name="neuropath_corr",
            file_ids=file_ids,
        )
        assert provenance_ids == expected_provenance_ids

    def test_get_provenance_ids_with_provenance(
        self, dataset_object_with_provenance: dict[str, Any]
    ) -> None:
        """Test that when provenance is provided in the config, both file ids and provenance ids are returned."""
        file_ids = ["syn1111111"]
        expected_provenance_ids = ["syn1111111", "syn11111145"]
        provenance_ids = process.get_provenance_ids(
            dataset_object_with_provenance,
            dataset_name="neuropath_corr",
            file_ids=file_ids,
        )
        assert sorted(provenance_ids) == sorted(expected_provenance_ids)

    def test_get_provenance_ids_with_duplicated_provenance(
        self, dataset_object_with_duplicated_provenance: dict[str, Any]
    ) -> None:
        """Test that when duplicated provenance is provided as both file id and provenance, unique values are returned."""
        file_ids = ["syn11111145"]
        expected_provenance_ids = ["syn11111145"]
        provenance_ids = process.get_provenance_ids(
            dataset_object_with_duplicated_provenance,
            dataset_name="neuropath_corr",
            file_ids=file_ids,
        )
        assert provenance_ids == expected_provenance_ids

    def test_error_get_provenance_ids_empty_dataset(self) -> None:
        """Test that an error is raised when the dataset object is empty."""
        file_ids = ["syn1111111"]
        wrong_format_dataset_obj = {
            "neuropath_corr": {
                "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
                "final_format": "json",
                "provenance": "not a list",
                "destination": "syn1111113",
                "gx_enabled": False,
            }
        }
        with pytest.raises(
            ValueError, match="Provenance for dataset 'neuropath_corr' must be a list"
        ):
            process.get_provenance_ids(
                wrong_format_dataset_obj,
                dataset_name="neuropath_corr",
                file_ids=file_ids,
            )

    def test_get_provenance_ids_mixed_list(
        self, dataset_object_provenance_mix_list: dict[str, Any]
    ) -> None:
        """Test that when provenance is provided as a mix of lists and strings, all values are flattened and returned."""
        file_ids = ["syn1111111"]
        expected_provenance_ids = [
            "syn1111111",
            "syn11111145",
            "syn11111146",
            "syn11111147",
        ]
        provenance_ids = process.get_provenance_ids(
            dataset_object_provenance_mix_list,
            dataset_name="neuropath_corr",
            file_ids=file_ids,
        )
        assert sorted(provenance_ids) == sorted(expected_provenance_ids)

    def test_get_provenance_ids_mismatch_file_ids_version(
        self, dataset_provenance_file_ids_mismatch: dict[str, Any]
    ) -> None:
        """Test that when file IDs and provenance IDs have mismatched versions, an error is raised."""
        file_ids = ["syn1111111.1"]
        with pytest.raises(ValueError, match="Version mismatch: "):
            process.get_provenance_ids(
                dataset_provenance_file_ids_mismatch,
                dataset_name="neuropath_corr",
                file_ids=file_ids,
            )


class TestProcessProvenance:
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
        self.patch_df_to_json = patch.object(
            load, "df_to_json", return_value="path/to/json"
        ).start()
        self.patch_dict_to_json = patch.object(
            load, "dict_to_json", return_value="path/to/json"
        ).start()
        self.patch_load = patch.object(load, "load", return_value=("syn123", 1)).start()

    def teardown_method(self):
        self.patch_get_entity_as_df.stop()
        self.patch_standardize_column_names.stop()
        self.patch_standardize_values.stop()
        self.patch_df_to_json.stop()
        self.patch_dict_to_json.stop()
        self.patch_load.stop()
        mock.patch.stopall()

    def test_upload_data_without_provenance(
        self, syn: synapseclient.Synapse, dataset_object_no_provenance: dict[str, Any]
    ) -> None:
        """Test that when no provenance is provided in the config, the file id is used as provenance."""
        # WHEN I call upload_data_without_provenance
        process.process_dataset(
            syn=syn,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            upload=True,
            dataset_obj=dataset_object_no_provenance,
        )
        # THEN I expect the load function to be called with file id
        self.patch_load.assert_called_once_with(
            file_path="path/to/json",
            provenance=["syn1111111"],
            destination=dataset_object_no_provenance["neuropath_corr"]["destination"],
            syn=syn,
        )

    def test_upload_data_with_provenance(
        self, syn: synapseclient.Synapse, dataset_object_with_provenance: dict[str, Any]
    ) -> None:
        """Test that when provenance is provided in the config, it is used in the upload."""
        # WHEN I call upload_data_with_provenance
        process.process_dataset(
            syn=syn,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            upload=True,
            dataset_obj=dataset_object_with_provenance,
        )
        # THEN I expect the load function to be called with file id and provenance from config
        call_args = self.patch_load.call_args
        assert call_args[1]["file_path"] == "path/to/json"
        assert sorted(call_args[1]["provenance"]) == sorted(
            ["syn1111111", "syn11111145"]
        )
        assert (
            call_args[1]["destination"]
            == dataset_object_with_provenance["neuropath_corr"]["destination"]
        )

    def test_upload_data_with_duplicated_provenance(
        self,
        syn: synapseclient.Synapse,
        dataset_object_with_duplicated_provenance: dict[str, Any],
    ) -> None:
        """Test that when duplicated provenance is provided in the config, unique values are used in the upload."""
        # WHEN I call upload_data_with_duplicated_provenance
        process.process_dataset(
            syn=syn,
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            upload=True,
            dataset_obj=dataset_object_with_duplicated_provenance,
        )
        # THEN I expect the load function to be called with file id and unique provenance from config
        self.patch_load.assert_called_once_with(
            file_path="path/to/json",
            provenance=["syn11111145"],
            destination=dataset_object_with_duplicated_provenance["neuropath_corr"][
                "destination"
            ],
            syn=syn,
        )

    def test_get_provenance_ids_mixed_list(
        self, dataset_object_provenance_mix_list: dict[str, Any]
    ) -> None:
        """Test that when provenance is provided as a mix of lists and strings, all values are flattened and returned."""
        file_ids = ["syn1111111"]
        expected_provenance_ids = [
            "syn1111111",
            "syn11111145",
            "syn11111146",
            "syn11111147",
        ]
        provenance_ids = process.get_provenance_ids(
            dataset_object_provenance_mix_list,
            dataset_name="neuropath_corr",
            file_ids=file_ids,
        )
        assert sorted(provenance_ids) == sorted(expected_provenance_ids)


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


class TestUploadManifestAndDataversion:
    file_id = "syn123"
    file_version = 1
    team_images_id = "syn987"
    destination = "syn1111113"
    manifest_path = "path/to/manifest"
    manifest_df = pd.DataFrame({"id": ["a", "b", "c"]})

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.patch_upload_dataversion_metadata = patch.object(
            process, "upload_dataversion_metadata", return_value=None
        ).start()
        self.patch_load = patch.object(
            load, "load", return_value=(self.file_id, self.file_version)
        ).start()
        yield
        self.patch_upload_dataversion_metadata.stop()
        self.patch_load.stop()

    def test_upload_manifest_and_dataversion(self, syn: synapseclient.Synapse) -> None:
        """Test that upload_manifest_and_dataversion calls load.load and upload_dataversion_metadata with correct arguments."""
        process.upload_manifest_and_dataversion(
            syn=syn,
            manifest_path=self.manifest_path,
            manifest_df=self.manifest_df,
            destination=self.destination,
            team_images_id=self.team_images_id,
            staging_path=STAGING_PATH,
        )
        self.patch_load.assert_called_once_with(
            file_path=self.manifest_path,
            provenance=self.manifest_df.id.tolist(),
            destination=self.destination,
            syn=syn,
        )
        self.patch_upload_dataversion_metadata.assert_called_once_with(
            syn=syn,
            file_id=self.file_id,
            file_version=self.file_version,
            team_images_id=self.team_images_id,
            staging_path=STAGING_PATH,
            destination=self.destination,
        )


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

    def test_process_dataset_upload_false_gx_not_specified(
        self, syn: synapseclient.Synapse
    ):
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
            data_as_df=pd.DataFrame,
            staging_path=STAGING_PATH,
            filename="neuropath_corr.json",
        )
        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_column_rename(
        self, syn: synapseclient.Synapse
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
            data=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()

        self.patch_df_to_json.assert_called_once()
        _, kwargs = self.patch_df_to_json.call_args

        assert kwargs["staging_path"] == STAGING_PATH
        assert kwargs["filename"] == "neuropath_corr.json"
        df = kwargs["data_as_df"]
        pd.testing.assert_frame_equal(df(), pd.DataFrame())

        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_custom_transformations(
        self, syn: synapseclient.Synapse
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
        _, kwargs = self.patch_df_to_json.call_args

        assert kwargs["staging_path"] == STAGING_PATH
        assert kwargs["filename"] == "neuropath_corr.json"
        pd.testing.assert_frame_equal(kwargs["data_as_df"], pd.DataFrame())
        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    # This test looks like a duplicate of test_process_dataset_upload_false_gx_disabled
    # but it uses the agora_rename configuration with the same util function
    def test_process_dataset_upload_false_gx_not_specified_with_agora_rename(
        self, syn: synapseclient.Synapse
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
            data=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )
        self.patch_custom_transform.assert_not_called()

        self.patch_df_to_json.assert_called_once()
        _, kwargs = self.patch_df_to_json.call_args

        assert kwargs["staging_path"] == STAGING_PATH
        assert kwargs["filename"] == "neuropath_corr.json"
        df = kwargs["data_as_df"]
        pd.testing.assert_frame_equal(df(), pd.DataFrame())

        self.patch_dict_to_json.assert_not_called()
        self.patch_list_to_json.assert_not_called()
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_type_dict(
        self, syn: synapseclient.Synapse
    ):
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
            data_as_dict={}, staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_false_gx_not_specified_type_list(
        self, syn: synapseclient.Synapse
    ):
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
            data_as_list=[], staging_path=STAGING_PATH, filename="neuropath_corr.json"
        )
        self.patch_gx_runner_run.assert_not_called()
        self.patch_set_attributes.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_load.assert_not_called()

    def test_process_dataset_upload_true_gx_disabled(self, syn: synapseclient.Synapse):
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
            data_as_df=pd.DataFrame,
            staging_path=STAGING_PATH,
            filename="neuropath_corr.json",
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

    def test_process_dataset_upload_true_gx_enabled(self, syn: synapseclient.Synapse):
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
            data_as_df=pd.DataFrame,
            staging_path=STAGING_PATH,
            filename="neuropath_corr.json",
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

    def test_process_dataset_upload_false_gx_enabled(self, syn: synapseclient.Synapse):
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
            data_as_df=pd.DataFrame,
            staging_path=STAGING_PATH,
            filename="neuropath_corr.json",
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
    def setup_method(self, syn: synapseclient.Synapse):
        self.patch_get_children = patch.object(
            syn, "getChildren", return_value=self.files
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_create_data_manifest_parent_none(self, syn: synapseclient.Synapse):
        # WHEN I call create_data_manifest with a parent of None
        result = process.create_data_manifest(syn=syn, parent=None)
        # THEN I expect the result to be None
        assert result is None
        # AND I expect the getChildren method to not be called
        self.patch_get_children.assert_not_called()

    def test_create_data_manifest_with_parent(self, syn: synapseclient.Synapse):
        # WHEN I call create_data_manifest with a parent
        result_df = process.create_data_manifest(syn=syn, parent="syn1111111")
        # THEN I expect the getChildren method to be called with the parent
        self.patch_get_children.assert_called_once_with("syn1111111")
        # AND I expect the result to be a dataframe with the correct rows
        # Including incrementing the version number for the data_manifest.csv file
        pd.testing.assert_frame_equal(result_df, pd.DataFrame(self.manifest_rows))


class TestProcessAllFiles:
    config_path = "./path/to/config"

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.test_reporter = DatasetReport(
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
        self.patch_upload_manifest_and_dataversion = patch.object(
            process,
            "upload_manifest_and_dataversion",
            return_value=("syn123", 1),
        ).start()
        self.patch_update_table = patch.object(
            ADTGXReporter,
            "update_table",
        ).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_process_all_files_upload_false(self, syn: synapseclient.Synapse):
        # WHEN process_all_files is called with upload=False
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            run_id="123",
            upload=False,
        )
        # THEN each dataset is processed with upload=False
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
        # AND the manifest is not created or uploaded
        self.patch_create_data_manifest.assert_not_called()
        self.patch_df_to_csv.assert_not_called()
        self.patch_upload_manifest_and_dataversion.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_true(self, syn: synapseclient.Synapse):
        # WHEN process_all_files is called with upload=True and skip_manifest=False (default)
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            run_id="123",
            upload=True,
        )
        # THEN each dataset is processed with upload=True
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
        # AND the manifest CSV is created locally
        self.patch_create_data_manifest.assert_called_once_with(
            parent="destination", syn=syn
        )
        self.patch_df_to_csv.assert_called_once_with(
            df=self.patch_create_data_manifest.return_value,
            staging_path=STAGING_PATH,
            filename="data_manifest.csv",
        )
        # AND the manifest and dataversion.json are uploaded to Synapse
        self.patch_upload_manifest_and_dataversion.assert_called_once_with(
            syn=syn,
            staging_path=STAGING_PATH,
            manifest_path="path/to/csv",
            manifest_df=self.patch_create_data_manifest.return_value,
            team_images_id="syn987",
            destination="destination",
        )

        self.patch_format_link.assert_called_once_with(syn_id="syn123", version=1)
        self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_true_skip_manifest(
        self, syn: synapseclient.Synapse
    ):
        # WHEN process_all_files is called with upload=True and skip_manifest=True
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            run_id="123",
            upload=True,
            skip_manifest=True,
        )
        # THEN the manifest is not created or uploaded
        self.patch_create_data_manifest.assert_not_called()
        self.patch_df_to_csv.assert_not_called()
        self.patch_upload_manifest_and_dataversion.assert_not_called()
        self.patch_format_link.assert_not_called()
        self.patch_update_table.assert_called_once()

    def test_process_all_files_defaults_staging_path_when_undefined(
        self, syn: synapseclient.Synapse
    ):
        config_without_staging_path = {
            "destination": "destination",
            "gx_folder": GX_FOLDER,
            "gx_table": "syn321",
            "team_images_id": "syn987",
            "datasets": [{"a": {"b": "c"}}],
        }
        with patch.object(
            utils, "_get_config", return_value=config_without_staging_path
        ):
            process.process_all_files(
                syn=syn,
                config_path=self.config_path,
                platform=Platform.LOCAL,
                run_id="123",
                upload=False,
            )
        self.patch_create_temp_location.assert_called_once_with(
            staging_path="./staging"
        )
        self.patch_process_dataset.assert_any_call(
            dataset_obj={"a": {"b": "c"}},
            staging_path="./staging",
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=False,
        )

    def test_process_all_files_upload_false_gx_failure(
        self, syn: synapseclient.Synapse
    ):
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
            self.patch_upload_manifest_and_dataversion.assert_not_called()
            self.patch_format_link.assert_not_called()
            self.patch_update_table.assert_called_once()

    def test_process_all_files_upload_false_process_dataset_fail(
        self, syn: synapseclient.Synapse
    ):
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
            self.patch_upload_manifest_and_dataversion.assert_not_called()
            self.patch_format_link.assert_not_called()
            self.patch_update_table.assert_called_once()

    def test_process_all_files_filter_datasets(
        self, syn: synapseclient.Synapse
    ) -> None:
        """Verify that only the specified dataset is processed when filter_datasets is set to a single name.
        The config contains three datasets ("a", "d", "g"), but only "a" should be processed.
        """
        # WHEN process_all_files is called with filter_datasets=["a"]
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            filter_datasets=["a"],
            run_id="123",
        )

        # THEN the config is loaded and the staging location is created as normal
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )

        # AND only dataset "a" is processed — "d" and "g" are skipped
        called_dataset = [
            list(call.kwargs["dataset_obj"].keys())[0]
            for call in self.patch_process_dataset.call_args_list
        ]
        assert called_dataset == ["a"]
        assert self.patch_process_dataset.call_count == 1
        self.patch_process_dataset.assert_called_once_with(
            dataset_obj={"a": {"b": "c"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )

    def test_process_all_files_filter_datasets_no_match(
        self, syn: synapseclient.Synapse
    ) -> None:
        """Verify that a ValueError is raised when filter_datasets contains names not present in the config."""
        # WHEN process_all_files is called with a dataset name that does not exist in the config
        # THEN a ValueError is raised with a message identifying the unmatched names
        with pytest.raises(
            ValueError, match="No datasets found matching: \\['non_existent_dataset'\\]"
        ):
            process.process_all_files(
                syn=syn,
                config_path=self.config_path,
                platform=Platform.LOCAL,
                filter_datasets=["non_existent_dataset"],
                run_id="123",
            )

    def test_process_multiple_files_filter_datasets(
        self, syn: synapseclient.Synapse
    ) -> None:
        """Verify that multiple datasets are processed when filter_datasets contains more than one name.
        The config contains three datasets ("a", "d", "g"), and only "a" and "d" should be processed.
        """
        # WHEN process_all_files is called with filter_datasets=["a", "d"]
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            filter_datasets=["a", "d"],
            run_id="123",
        )

        # THEN the config is loaded and the staging location is created as normal
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )

        # AND exactly "a" and "d" are processed in config order — "g" is skipped
        called_dataset = [
            list(call.kwargs["dataset_obj"].keys())[0]
            for call in self.patch_process_dataset.call_args_list
        ]
        assert called_dataset == ["a", "d"]
        assert self.patch_process_dataset.call_count == 2
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

    def test_process_duplicated_dataset_names_filter_datasets(
        self, syn: synapseclient.Synapse
    ) -> None:
        """Verify that a dataset is not processed more than once when its name appears multiple times in filter_datasets.
        This guards against double-processing when a user passes --dataset a --dataset a or --dataset a,a.
        """
        # WHEN process_all_files is called with a duplicated dataset name in filter_datasets
        process.process_all_files(
            syn=syn,
            config_path=self.config_path,
            platform=Platform.LOCAL,
            filter_datasets=["a", "a"],
            run_id="123",
        )

        # THEN the config is loaded and the staging location is created as normal
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )

        # AND dataset "a" is processed exactly once despite appearing twice in the filter
        called_dataset = [
            list(call.kwargs["dataset_obj"].keys())[0]
            for call in self.patch_process_dataset.call_args_list
        ]
        assert called_dataset == ["a"]
        assert self.patch_process_dataset.call_count == 1
        self.patch_process_dataset.assert_called_once_with(
            dataset_obj={"a": {"b": "c"}},
            staging_path=STAGING_PATH,
            gx_folder=GX_FOLDER,
            syn=syn,
            upload=True,
        )


class TestProcessCLI:
    """Tests for the `process` CLI command"""

    runner = CliRunner()

    @pytest.fixture(autouse=True)
    def patch_dependencies(self):
        with (
            patch.object(utils, "_login_to_synapse", return_value=None),
            patch.object(process, "process_all_files") as mock_process_all_files,
        ):
            self.mock_process_all_files = mock_process_all_files
            yield

    def test_process_cli_no_dataset_flag(self) -> None:
        """When --dataset is omitted, filter_datasets should be None (process all)."""
        # WHEN the CLI is invoked without a --dataset flag
        result = self.runner.invoke(process.app, ["process", "path/to/config"])

        # THEN the command succeeds and filter_datasets is None, meaning all datasets are processed
        assert result.exit_code == 0
        self.mock_process_all_files.assert_called_once()
        assert self.mock_process_all_files.call_args.kwargs["filter_datasets"] is None

    def test_process_cli_single_dataset(self) -> None:
        """A single --dataset flag passes a one-element list to process_all_files."""
        # WHEN the CLI is invoked with a single --dataset flag
        result = self.runner.invoke(
            process.app, ["process", "path/to/config", "--dataset", "gene_info"]
        )

        # THEN the command succeeds and filter_datasets contains exactly the specified dataset name
        assert result.exit_code == 0
        assert self.mock_process_all_files.call_args.kwargs["filter_datasets"] == [
            "gene_info"
        ]

    def test_process_cli_repeated_dataset_flags(self) -> None:
        """Repeated --dataset flags are combined into a list."""
        # WHEN the CLI is invoked with --dataset specified multiple times
        result = self.runner.invoke(
            process.app,
            [
                "process",
                "path/to/config",
                "--dataset",
                "gene_info",
                "--dataset",
                "team_info",
            ],
        )

        # THEN the command succeeds and filter_datasets contains all specified dataset names
        assert result.exit_code == 0
        assert self.mock_process_all_files.call_args.kwargs["filter_datasets"] == [
            "gene_info",
            "team_info",
        ]

    def test_process_cli_comma_separated_datasets(self) -> None:
        """A comma-separated value in --dataset is split into individual names."""
        # WHEN the CLI is invoked with a comma-separated list of dataset names in a single --dataset flag
        result = self.runner.invoke(
            process.app,
            ["process", "path/to/config", "--dataset", "gene_info,team_info"],
        )

        # THEN the command succeeds and filter_datasets contains each name as a separate entry
        assert result.exit_code == 0
        assert self.mock_process_all_files.call_args.kwargs["filter_datasets"] == [
            "gene_info",
            "team_info",
        ]

    def test_process_cli_comma_separated_with_spaces(self) -> None:
        """Whitespace around comma-separated names is stripped."""
        # WHEN the CLI is invoked with a comma-separated list that includes surrounding whitespace
        result = self.runner.invoke(
            process.app,
            ["process", "path/to/config", "--dataset", "gene_info, team_info"],
        )

        # THEN the command succeeds and dataset names are trimmed before being passed to process_all_files
        assert result.exit_code == 0
        assert self.mock_process_all_files.call_args.kwargs["filter_datasets"] == [
            "gene_info",
            "team_info",
        ]
