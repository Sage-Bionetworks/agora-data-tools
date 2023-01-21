import json
from os import mkdir, path, rmdir
from typing import Union

import numpy as np
import pandas as pd
from synapseclient import Activity, File

from . import utils


class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def create_temp_location(staging_path: str):
    """Creates a temporary location to store the json files.
        Does nothing if directory already exists.

    Args:
        staging_path (str): path to directory to be created
    """
    try:
        mkdir(staging_path)
    except FileExistsError:
        return


def delete_temp_location(staging_path: str):
    """Deletes the default temporary location

    Args:
        staging_path (str): path to temporary directory to be deleted
    """
    rmdir(staging_path)


def remove_non_values(d: dict) -> dict:
    """Given a dictionary, remove all keys whose values are null.
    Values can be of a few types: a dict, a list, None/NaN, and a regular element - such as str or number;
    each one of the cases is handled separately, if a key contains a list, the list can contain elements,
    or nested dicts.  The same goes for dictionaries.

    Args:
        d (dict): input dictionary to be cleaned

    Returns:
        dict: final cleaned dictionary which has had null values removed
    """
    cleaned_dict = {}

    for key, value in d.items():
        # case 1: dict
        if isinstance(value, dict):
            nested_dict = remove_non_values(value)
            if len(nested_dict.keys()) > 0:
                cleaned_dict[key] = nested_dict
        # case 2: list
        elif isinstance(
            value, list
        ):  # was missing elif before - was just if and was breaking the recursion
            for i, elem in enumerate(value):  # value is a list
                if isinstance(elem, dict):
                    value[i] = remove_non_values(elem)
                    if value[i] == {}:
                        value.pop(i)
                cleaned_dict[key] = value
        # case 3: None/NaN
        elif pd.isna(value) or value is None:
            continue
        # case 4: regular element
        elif value is not None:
            cleaned_dict[key] = value

    return cleaned_dict


def load(file_path: str, provenance: list, destination: str, syn=None):

    """
    Calls df_to_json, add_to_manifest, add_to_report
    :param filename: the name of the file to be loaded into Synapse
    :param provenance: array of files that originate the one being loaded
    :param syn: synapse object
    :return: synapse id of the file loaded into Synapse.  Returns None if it
    fails
    """

    if syn is None:
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
        print(
            f"Either the file path ({file_path}) or the destination ({destination}) are invalid."
        )

        print(e)
        return
    except ValueError:
        print(
            "Please make sure that the Synapse id of "
            + "the provenances and the destination are valid"
        )
        return

    return (file.id, file.versionNumber)


def df_to_json(df: pd.DataFrame, staging_path: str, filename: str) -> Union[None, str]:
    """Converts a data frame into a json file.

    Args:
        df (pd.DataFrame): DataFrame to be converted to JSON
        staging_path (str): Path to staging directory
        filename (str): name of JSON file to be created

    Returns:
        Union[None, str]: can return None (if the first `try` fails), or a string containing the name of the new JSON file if the function succeeds
    """

    try:
        df = df.replace({np.nan: None})

        df_as_dict = df.to_dict(orient="records")

        temp_json = open(path.join(staging_path, filename), "w+")
        json.dump(df_as_dict, temp_json, cls=NumpyEncoder, indent=2)
    except Exception as e:
        print(e)
        try:  # this was failing if the `try` above fails before creating `temp_json`
            temp_json.close()
        except:
            return None
        return None

    temp_json.close()
    return temp_json.name


def df_to_csv(df: pd.DataFrame, staging_path: str, filename: str) -> Union[None, str]:
    """Converts a data frame into a csv file.

    Args:
        df (pd.DataFrame): DataFrame to be converted to a csv file
        staging_path (str): Path to staging directory
        filename (str): name of JSON file to be created

    Returns:
        Union[None, str]: can return None (if the first `try` fails), or a string containing the name of the new csv file if the function succeeds
    """
    try:
        temp_csv = open(path.join(staging_path, filename), "w+")
        df.to_csv(path_or_buf=temp_csv, index=False)
    except AttributeError:
        print("Invalid dataframe.")
        temp_csv.close()
        return None

    temp_csv.close()
    return temp_csv.name


def dict_to_json(df: dict, staging_path: str, filename: str):
    try:
        df_as_dict = [  # TODO explore the df.to_dict() function for this case
            {
                d: remove_non_values(v) if isinstance(v, dict) else v
                for d, v in df.items()
            }
        ]
        temp_json = open(path.join(staging_path, filename), "w+")
        json.dump(df_as_dict, temp_json, cls=NumpyEncoder, indent=2)
    except Exception as e:
        print(e)
        temp_json.close()
        return None

    temp_json.close()
    return temp_json.name
