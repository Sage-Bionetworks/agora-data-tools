import pytest
from agoradatatools.etl import utils
from unittest.mock import patch
import synapseclient
import yaml
import io
import builtins


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


def test_get_config_with_invalid_file_path():
    with pytest.raises(FileNotFoundError, match="File not found. *"):
        utils._get_config(config_path="this/is/a/bad/path")


def test_get_config_with_parser_error():
    with pytest.raises(
        yaml.parser.ParserError, match="YAML file unable to be parsed. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_parsing.yaml")


def test_get_config_with_scanner_error():
    with pytest.raises(
        yaml.scanner.ScannerError, match="YAML file unable to be scanned. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_scanning.yaml")


class TestGetConfig:
    def setup_method(self, syn):
        self.patch_open = patch.object(
            builtins, "open", return_value=io.TextIOWrapper
        ).start()
        self.patch_yaml_load = patch.object(
            yaml, "load", return_value=[{"a": "b"}, {"c", "d"}]
        ).start()

    def teardown_method(self):
        self.patch_open.stop()
        self.patch_yaml_load.stop()

    def test_get_config_with_no_config_path(self):
        config = utils._get_config(config_path=None)
        self.patch_open.assert_called_once_with("./config.yaml", "r")
        self.patch_yaml_load.assert_called_once_with(
            io.TextIOWrapper, Loader=yaml.FullLoader
        )
        assert config == self.patch_yaml_load.return_value

    def test_get_config_with_config_path(self):
        config = utils._get_config(config_path="this/is/a/good/path")
        self.patch_open.assert_called_once_with("this/is/a/good/path", "r")
        self.patch_yaml_load.assert_called_once_with(
            io.TextIOWrapper, Loader=yaml.FullLoader
        )
        assert config == self.patch_yaml_load.return_value


def test_find_config_by_name_where_name_in_config():
    config = [{"a": "b"}, {"c", "d"}]
    returned_object = utils._find_config_by_name(config=config, name="a")
    assert returned_object is not None


def test_find_config_by_name_where_name_not_in_config():
    config = [{"a": "b"}, {"c": "d"}]
    returned_object = utils._find_config_by_name(config=config, name="z")
    assert returned_object is None
