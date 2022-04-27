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


def remove_non_values(d: dict) -> dict:
    """
    Given a dictionary, remove all keys whose values are null.
    Values can be of a few types: a dict, a list, None/NaN, and a regular element - such as str or number;
    each one of the cases is handled separately, if a key contains a list, the list can contain elements,
    or nested dicts.  The same goes for dictionaries.
    """
    cleaned_dict = {}

    for key, value in d.items():
        # case 1: dict
        if isinstance(value, dict):
            nested_dict = remove_non_values(value)
            if len(nested_dict.keys()) > 0:
                cleaned_dict[key] = nested_dict
        # case 2: list
        if isinstance(value, list):
            for elem in value: # value is a list
                if isinstance(elem, dict):
                    nested_dict = remove_non_values(elem)
                else:
                    cleaned_dict[key] = value
        # case 3: None/NaN
        elif pd.isna(value) or value is None:
            continue
        #case 4: regular element
        elif value is not None:
            cleaned_dict[key] = value

    return cleaned_dict


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
        df = df.replace({np.nan: None})

        df_as_dict = df.to_dict(orient='records')
        df_as_dict = [remove_non_values(d) for d in df_as_dict]

        temp_json = open("./staging/" + filename, 'w+')
        json.dump(df_as_dict, temp_json,
                 cls=NumpyEncoder,
                 indent=2)
    except AttributeError as e:
        print("Invalid dataframe.")
        temp_json.close()
        return None

    temp_json.close()
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
        temp_csv.close()
        return None

    temp_csv.close()
    return temp_csv.name


def dict_to_json(df: dict, filename: str):
    try:

        df_as_dict = df.to_dict(orient='records')
        df_as_dict = [remove_non_values(d) for d in df_as_dict]

        temp_json = open("./staging/" + filename, 'w+')
        json.dump(df_as_dict, temp_json,
                  cls=NumpyEncoder,
                  indent=2)
    except Exception as e:
        print(e)
        temp_json.close()
        return None

    temp_json.close()
    return temp_json.name
