import pytest
from agoradatatools import extract
from agoradatatools import utils
from pandas import DataFrame

syn = utils._login_to_synapse()

def test_read_csv_into_df():
    assert type(extract.read_csv_into_df(csv_path="./tests/test_assets/test_file.csv")) is DataFrame

    with pytest.raises(SystemExit) as err:
        extract.read_csv_into_df(csv_path="./tests/test_assets/test_file.tsv")
    assert err.type == SystemExit
    assert err.value.code == 9


def test_read_csv_into_df():
    assert type(extract.read_tsv_into_df(tsv_path="./tests/test_assets/test_file.tsv")) is DataFrame

    with pytest.raises(SystemExit) as err:
        extract.read_tsv_into_df(tsv_path="./tests/test_assets/test_file.csv")
    assert err.type == SystemExit
    assert err.value.code == 9


def test_read_table_into_df():
    assert type(extract.read_table_into_df(table_id="syn25838546", syn=syn)) is DataFrame

def test_get_entity_as_df():
    assert type(extract.get_entity_as_df(syn_id="syn25838562", format="csv")) is DataFrame
    with pytest.raises(SystemExit) as err:
        extract.get_entity_as_df(syn_id="syn25838562", format="tsv")
    assert err.type == SystemExit
    assert err.value.code == 9

    assert type(extract.get_entity_as_df(syn_id="syn25838563", format="tsv")) is DataFrame
    with pytest.raises(SystemExit) as err:
        extract.get_entity_as_df(syn_id="syn25838563", format="csv")
    assert err.type == SystemExit
    assert err.value.code == 9

    assert type(extract.get_entity_as_df(syn_id="syn25838546", format="table")) is DataFrame
    with pytest.raises(SystemExit) as err:
        extract.get_entity_as_df(syn_id="syn25838563", format="table")
    assert err.type == SystemExit
    assert err.value.code == 1

if __name__ == "__main__":
    pytest.main()

