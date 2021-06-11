from agoradatatools.etl import load
import pandas as pd
import numpy as np

df = pd.DataFrame(
    {'team_id': [0, 1, np.nan],
     'team_name': ['MSN', 'Team 1', 'Team 2'],
     'team_score': ['x', 'y', 'z']})

def test_df_to_json():
    assert load.df_to_json(df, "test.json") == "./staging/test.json"
    assert type(load.df_to_json(1, "test.json")) is type(None)

def test_load():
    path = "./tests/test_assets/test.json"
    bad_path = "./tests/test_assets/invalid_path.json"
    bad_used = ['', 'xxx']
    used = ['syn25721515', 'syn25721521']
    destination = 'syn25871921'
    bad_destination = 's923484y23'

    good_result = load.load(file_path=path, provenance=used, destination=destination)
    assert type(good_result) is tuple
    assert good_result[0] == 'syn25871925'

    bad_path_result = load.load(file_path=bad_path, provenance=used, destination=destination)
    assert type(bad_path_result) is type(None)

    bad_used_result = load.load(file_path=path, provenance=bad_used, destination=destination)
    assert type(bad_used_result) is type(None)

    bad_destination = load.load(file_path=path, provenance=used, destination=bad_destination)
    assert type(bad_used_result) is type(None)
