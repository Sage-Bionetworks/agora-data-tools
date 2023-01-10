import pandas as pd
from unittest.mock import patch
import argparse

from agoradatatools.etl import extract, transform, load, utils
from agoradatatools import process


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


def test_process_dataset_with_column_rename():
    with patch.object(
        extract, "get_entity_as_df", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_column_names", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_values", return_value=pd.DataFrame
    ), patch.object(
        transform, "rename_columns", return_value=pd.DataFrame
    ) as patch_rename_columns, patch.object(
        load, "df_to_json", return_value="path/to/json"
    ), patch.object(
        load, "load", return_value=None
    ):
        process.process_dataset(dataset_obj=dataset_object_col_rename)
        patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )


dataset_object_custom_transform = {
    "neuropath_corr": {
        "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
        "final_format": "json",
        "provenance": ["syn1111111"],
        "destination": "syn1111113",
        "custom_transformations": "test_transformation",
    }
}


def test_process_dataset_custom_transformations():
    with patch.object(
        extract, "get_entity_as_df", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_column_names", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_values", return_value=pd.DataFrame
    ), patch.object(
        transform, "apply_custom_transformations", return_value=pd.DataFrame
    ) as patch_custom_transform, patch.object(
        load, "df_to_json", return_value="path/to/json"
    ), patch.object(
        load, "load", return_value=None
    ):
        process.process_dataset(dataset_obj=dataset_object_custom_transform)
        patch_custom_transform.assert_called_once_with(
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


dataset_object_agora_rename = {
    "neuropath_corr": {
        "files": [{"name": "test_file_1", "id": "syn1111111", "format": "csv"}],
        "final_format": "json",
        "provenance": ["syn1111111"],
        "destination": "syn1111113",
        "agora_rename": {"col_1": "new_col_1", "col_2": "new_col_2"},
    }
}


def test_process_dataset_with_agora_rename():
    with patch.object(
        extract, "get_entity_as_df", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_column_names", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_values", return_value=pd.DataFrame
    ), patch.object(
        transform, "rename_columns", return_value=pd.DataFrame
    ) as patch_rename_columns, patch.object(
        load, "df_to_json", return_value="path/to/json"
    ), patch.object(
        load, "load", return_value=None
    ):
        process.process_dataset(dataset_obj=dataset_object_col_rename)
        patch_rename_columns.assert_called_once_with(
            df=pd.DataFrame, column_map={"col_1": "new_col_1", "col_2": "new_col_2"}
        )


def test_process_dataset_type_dict():
    with patch.object(
        extract, "get_entity_as_df", return_value=pd.DataFrame
    ), patch.object(
        transform, "standardize_column_names", return_value=pd.DataFrame
    ), patch.object(
        transform,
        "standardize_values",
        return_value=dict(),  # test if it is a dictionary later
    ), patch.object(
        load, "dict_to_json", return_value="path/to/json"
    ) as patch_dict_to_json, patch.object(
        load, "load", return_value=None
    ):
        process.process_dataset(dataset_obj=dataset_object)
        patch_dict_to_json.assert_called_once_with(
            df={}, filename="neuropath_corr.json"
        )


def test_create_data_manifest_parent_none(syn):
    assert process.create_data_manifest(parent=None, syn=syn) is None


def test_create_data_manifest_syn_none(syn):
    with patch.object(
        utils, "_login_to_synapse", return_value=syn
    ) as patch_login_to_synapse, patch.object(
        syn, "getChildren", return_value=[{"id": "123", "versionNumber": 1}]
    ):
        process.create_data_manifest(parent="syn1111111", syn=None)
        patch_login_to_synapse.assert_called_once()


def test_create_data_manifest_no_none(syn):
    with patch.object(utils, "_login_to_synapse", return_value=syn), patch.object(
        syn, "getChildren", return_value=[{"id": "123", "versionNumber": 1}]
    ) as patch_get_children:
        df = process.create_data_manifest(parent="syn1111111", syn=syn)
        patch_get_children.assert_called_once_with("syn1111111")
        assert isinstance(df, pd.DataFrame)


def test_process_all_files_config_path(syn):
    with patch.object(
        utils,
        "_get_config",
        return_value=[{"destination": "destination"}, {"datasets": ["a", "b", "c"]}],
    ) as patch_get_config, patch.object(
        load, "create_temp_location", return_value=None
    ), patch.object(
        process, "process_dataset", return_value=tuple()
    ), patch.object(
        process,
        "create_data_manifest",
        return_value=pd.DataFrame({"id": ["a", "b", "c"]}),
    ), patch.object(
        load, "df_to_csv", return_value="path/to/csv"
    ), patch.object(
        load, "load", return_value=None
    ):
        process.process_all_files(config_path="path/to/config", syn=syn)
        patch_get_config.assert_called_once_with(config_path="path/to/config")


def test_process_all_files_no_config_path(syn):
    with patch.object(
        utils,
        "_get_config",
        return_value=[{"destination": "destination"}, {"datasets": ["a", "b", "c"]}],
    ) as patch_get_config, patch.object(
        load, "create_temp_location", return_value=None
    ), patch.object(
        process, "process_dataset", return_value=tuple()
    ), patch.object(
        process,
        "create_data_manifest",
        return_value=pd.DataFrame({"id": ["a", "b", "c"]}),
    ), patch.object(
        load, "df_to_csv", return_value="path/to/csv"
    ), patch.object(
        load, "load", return_value=None
    ):
        process.process_all_files(config_path=None, syn=syn)
        patch_get_config.assert_called_once_with()


def test_process_all_files_full(syn):
    with patch.object(
        utils,
        "_get_config",
        return_value=[{"destination": "destination"}, {"datasets": ["a"]}],
    ), patch.object(load, "create_temp_location", return_value=None), patch.object(
        process, "process_dataset", return_value=tuple()
    ) as patch_process_dataset, patch.object(
        process,
        "create_data_manifest",
        return_value=pd.DataFrame({"id": ["a", "b", "c"]}),
    ) as patch_create_data_manifest, patch.object(
        load, "df_to_csv", return_value="path/to/csv"
    ) as patch_df_to_csv, patch.object(
        load, "load", return_value=None
    ):
        process.process_all_files(config_path=None, syn=syn)
        patch_process_dataset.assert_called_once_with(dataset_obj="a", syn=syn)
        patch_create_data_manifest.assert_called_once_with(
            parent="destination", syn=syn
        )
        patch_df_to_csv.assert_called_once_with(
            df=patch_create_data_manifest.return_value, filename="data_manifest.csv"
        )


def test_build_parser():
    with patch.object(
        argparse,
        "ArgumentParser",
        return_value=argparse.ArgumentParser(),
    ) as patch_build_parser:
        parser = process.build_parser()
        patch_build_parser.assert_called_once_with(description="Agora data processing")
        assert (
            parser == argparse.ArgumentParser()
        )  # isinstance(parser, argparse.ArgumentParser) and type(parser) == argparse.argumentParser both failed here. they don't seem to recognize argparse.ArgumentParser as a type
