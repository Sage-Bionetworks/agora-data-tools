import synapseclient
import yaml
import errno
import sys
from agoradatatools import extract, transform, load


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
    except yaml.parser.ParserError or yaml.scanner.ScannerError:
        print("Invalid file.  Please provide a valid YAML file.")
        sys.exit(errno.EBADF)

    return config[1]['files']

def process_single_file(file_obj: dict):
    """
    Puts a single file through the entire ETL process.
    If process is successful, it will return a string
    with the Synapse id of the newly loaded file
    :param file_obj: a file object from the configuration
    :return: a Synapse id
    """

    # individual exceptions are defined in each file
    try:
        df = extract.get_entity_as_df(syn_id=file_obj['id'], format=file_obj['format'])
    except Exception as extract_error:
        print("There was an error extracting " + file_obj['id'])
        print(extract_error)
        return

    try:
        df = transform.standardize_column_names(df=df)
        df = transform.standardize_values(df=df)
    except Exception as transform_error:
        print("There was an error transforming " + file_obj['id'])
        print(transform_error)
        return

    try:
        json_path = load.df_to_json(df=df, filename=file_obj['final_filename'])
        syn_id = load.load(file_path=json_path, provenance=file_obj['provenance'], destination=file_obj['destination'])
    except Exception as load_error:
        print("There was an error loading " + file_obj['id'])
        print(load_error)
        return

    return syn_id
