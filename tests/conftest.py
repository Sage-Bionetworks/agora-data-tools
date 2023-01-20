from unittest import mock

import pytest
import synapseclient


@pytest.fixture(scope="session")
def syn():
    return mock.create_autospec(synapseclient.Synapse)
