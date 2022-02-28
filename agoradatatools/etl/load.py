import pandas as pd
import json
from os import mkdir, rmdir
from . import utils
from synapseclient import File, Activity
import numpy as np

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def create_temp_location():
    """
    Creates a temporary location to store the json files
    """
    try:
        mkdir('./staging')
    except FileExistsError:
        return


def delete_temp_location():
    """
    Deletes the default temporary location
    """
    rmdir('./staging')


def load(file_path: str, provenance: list[str], destination: str, syn=None):
    """
    Calls df_to_json, add_to_manifest, add_to_report
    :param filename: the name of the file to be loaded into Synapse
    :param provenance: array of files that originate the one being loaded
    :param syn: synapse object
    :return: synapse id of the file loaded into Synapse.  Returns None if it
    fails
    """

    if not syn:
        syn = utils._login_to_synapse()

    try:
        activity = Activity(used=provenance)
    except ValueError:
        print(str(provenance) + " has one or more invalid syn ids")
        return

    try:
        file = File(file_path, parent=destination)
        file = syn.store(file, activity=activity)
    except OSError as e:
        print("Either the file path (" + file_path +
              ") or the destination(" + destination +
              ") are invalid.")

        print(e)
        return
    except ValueError:
        print("Please make sure that the Synapse id of " +
              "the provenances and the destination are valid")
        return

    return (file.id, file.versionNumber)


def df_to_json(df: pd.core.frame.DataFrame, filename: str):
    """
    Converts a data frame into a json file.
    :param df: a dataframe
    :param filename: the final file name included in the config file
    :return: the path of the newly created temporary json file
    """

    try:
        temp_json = open("./staging/" + filename, 'w+')
        json_str = df.to_json(orient='records', indent=2)
        json_parsed = json.loads(json_str)
        json.dump(json_parsed, temp_json, indent=2)
    except AttributeError as e:
        print("Invalid dataframe.")
        print(e)
        return None

    return temp_json.name


def df_to_csv(df: pd.core.frame.DataFrame, filename: str):
    """
    Converts a data frame into a csv file.
    :param df: a dataframe
    :param filename: the final file name included in the config file
    :return: the path of the newly created temporary csv file
    """
    try:
        temp_csv = open("./staging/" + filename, 'w+')
        df.to_csv(path_or_buf=temp_csv, index=False)
    except AttributeError:
        print("Invalid dataframe.")
        return None

    return temp_csv.name


def dict_to_json(df: dict, filename = str):
    try:
        # df = df.replace({np.nan: None})
        temp_json = open("./staging/" + filename, 'w+')

        json.dump(df, temp_json, indent=2)
    except Exception as e:
        print(e)
        return None

    return temp_json.name
