import synapseclient
import yaml
import errno
import sys
from pandas import DataFrame


def _login_to_synapse() -> object:
    syn = synapseclient.Synapse()
    syn.login()
    return syn


def _get_config(config_path: str = None):
    if not config_path:
        config_path = "./config.yaml"

    file = None
    config = None

    try:
        file = open(config_path, "r")
        config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print("File not found.  Please provide a valid path.")
        sys.exit(errno.ENOENT)
    except yaml.parser.ParserError or yaml.scanner.ScannerError as e:
        print("Invalid file.  Please provide a valid YAML file.")
        sys.exit(errno.EBADF)
    return config
