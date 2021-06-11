import pytest
import agoradatatools.process as process

file_object = {
    "id": "syn25838546",
    "format": "table",
    "final_filename": "teams.json",
    "provenance": ['syn25838546'],
    "destination": "syn25871921"
}

def test_process_single_file():
    good_result = process.process_single_file(file_obj=file_object)
    assert type(good_result) is tuple
