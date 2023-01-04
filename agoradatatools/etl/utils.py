import synapseclient
import yaml
import errno
import sys


def _login_to_synapse(authtoken: str = None) -> object:
    syn = synapseclient.Synapse()
    if authtoken is None:
        syn.login()
    else:
        syn.login(authToken=authtoken)
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
    except yaml.parser.ParserError or yaml.scanner.ScannerError:
        print("Invalid file.  Please provide a valid YAML file.")
        sys.exit(errno.EBADF)
    return config


def _find_config_by_name(config: list, name: str):
    """Iterates through the list to find a dictionary with a key matching 'name'

    Args:
        config: A list of dicts, each of which usually contain a single key.
            These come from the config yaml file.
        name: The name of that key.

    Returns:
        object: If a dictionary is found, returns the contents of dict[name].
            Otherwise returns None.

    """
    for item in config:
        if name in item.keys():
            return item[name]
    return None
