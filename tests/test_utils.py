import pytest
from agoradatatools import utils
from synapseclient import Synapse

def test_login():
    assert type(utils._login_to_synapse()) is Synapse

if __name__ == "__main__":
    pytest.main()