from unittest import mock

import pytest
import synapseclient

from agoradatatools.etl import utils


@pytest.fixture(scope="session", autouse=True)
def syn():
    return mock.create_autospec(synapseclient.Synapse)
