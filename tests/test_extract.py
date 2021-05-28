import pytest
from agoradatatools import extract
from pandas import DataFrame

def test_read_csv_to_df():
    assert type(extract.read_csv_to_df(syn_id="syn12615624")) is DataFrame
    # test for error in format


if __name__ == "__main__":
    pytest.main()