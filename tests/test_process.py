import pytest
import agoradatatools.process as process
import pandas as pd

file_object = {
    "id": "syn25838546",
    "format": "table",
    "final_filename": "teams.json",
    "provenance": ['syn25838546'],
    "destination": "syn25871921"
}

dataset_object_multi = {
        "team_info": {
            "files": [
                {
                    "syn12615624": "csv"
                },
                {
                    "syn12615633": "csv"
                }
            ],
            "final_format": "json",
            "additional_transformations": [
                {
                    "join_datasets": {
                        "left": "syn12615624",
                        "right": "syn12615633",
                        "how": "left",
                        "on": "team"
                    }
                }
            ],
            "provenance": [
                "syn12615624",
                "syn12615633"
            ],
            "destination": "syn25871921"
        }
    }

dataset_object_single = {
        "neuropath_corr": {
            "files": [
                {
                    "syn22017882": "csv"
                }
            ],
            "final_format": "json",
            "provenance": [
                "syn22017882"
            ],
            "column_rename": {
                "ensg": "ensembl_gene_id",
                "gname": "hgnc_gene_id"
            },
            "destination": "syn25871921"
        }
    }

def test_process_dataset():
    good_result = process.process_dataset(dataset_obj=dataset_object_single)
    assert type(good_result) is tuple

def test_create_data_manifest():
    assert type(process.create_data_manifest(parent='syn27406021')) == pd.DataFrame
    assert process.create_data_manifest() is None
