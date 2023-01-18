import pytest
from agoradatatools.etl import utils
from unittest.mock import patch
import synapseclient

# file_object = {
#     "id": "syn25838546",
#     "format": "table",
#     "final_filename": "teams",
#     "provenance": [],
#     "destination": "syn25871921",
# }


class TestLoginToSynapse:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.patch_synapseclient = patch.object(
            synapseclient, "Synapse", return_value=syn
        ).start()
        self.patch_syn_login = patch.object(syn, "login", return_value=syn).start()

    def teardown_method(self):
        self.patch_synapseclient.stop()
        self.patch_syn_login.stop()

    def test_login_with_token(self):
        utils._login_to_synapse(authtoken="my_auth_token")
        self.patch_syn_login.assert_called_once_with(authToken="my_auth_token")

    def test_login_no_token(self):
        utils._login_to_synapse(authtoken=None)
        self.patch_syn_login.assert_called_once_with()


# class TestGetConfig:
#     @pytest.fixture(scope="function", autouse=True)
#     def setup_method(self, syn):
#         self.patch_synapseclient = patch.object(
#             synapseclient, "Synapse", return_value=syn
#         ).start()
#         self.patch_syn_login = patch.object(syn, "login", return_value=syn).start()

#     def teardown_method(self):
#         self.patch_synapseclient.stop()
#         self.patch_syn_login.stop()


def test_find_config_by_name_where_name_in_config():
    config = [{"a": "b"}, {"c", "d"}]
    returned_object = utils._find_config_by_name(config=config, name="a")
    assert returned_object is not None


def test_find_config_by_name_where_name_not_in_config():
    config = [{"a": "b"}, {"c": "d"}]
    returned_object = utils._find_config_by_name(config=config, name="z")
    assert returned_object is None


# def test_yaml():
#     # tests if a valid file renders a list
#     assert type(utils._get_config()) is list

#     # tests if a bad file will
#     with pytest.raises(SystemExit) as err:
#         utils._get_config(config_path="./tests/test_assets/bad_config.yaml")
#     assert err.type == SystemExit
#     assert err.value.code == 9

#     with pytest.raises(SystemExit) as err:
#         utils._get_config(config_path="./tests/test_assets/bad_config.yam")
#     assert err.type == SystemExit
#     assert err.value.code == 2
