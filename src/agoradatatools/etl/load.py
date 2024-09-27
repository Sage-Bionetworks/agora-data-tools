import json
import os

import numpy as np
import pandas as pd
from synapseclient import Activity, File, Synapse

from typing import Dict, List, Any

class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def create_temp_location(staging_path: str):
    """Creates a temporary location to store the json files.
        Does nothing if directory already exists.

    Args:
        staging_path (str): path to directory to be created
    """
    try:
        os.mkdir(staging_path)
    except FileExistsError:
        return


def delete_temp_location(staging_path: str):
    """Deletes the default temporary location

    Args:
        staging_path (str): path to temporary directory to be deleted
    """
    os.rmdir(staging_path)


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


def load(file_path: str, provenance: list, destination: str, syn: Synapse) -> tuple:
    """Reads file to be loaded into Synapse
    :param syn: synapse object
    :return: synapse id of the file loaded into Synapse.  Returns None if it
    fails

    Args:
        file_path (str): Path of the file to be loaded into Synapse
        provenance (list): Array of files that originate the one being loaded
        destination (str): Location where the file should be loaded in Synapse
        syn (synapseclient.Synapse): synapseclient session.

    Returns:
        tuple: Returns a tuple of the name fo the file and the version number.
    """

    activity = Activity(used=provenance)
    file = File(file_path, parent=destination)
    file = syn.store(file, activity=activity, forceVersion=False)
    return (file.id, file.versionNumber)


def df_to_json(df: pd.DataFrame, staging_path: str, filename: str) -> str:
    """Converts a data frame into a json file.

    Args:
        df (pd.DataFrame): DataFrame to be converted to JSON
        staging_path (str): Path to staging directory
        filename (str): name of JSON file to be created

    Returns:
       str: Returns a string containing the name of the new JSON file
    """

    df = df.replace({np.nan: None})
    df_as_dict = df.to_dict(orient="records")
    temp_json = open(os.path.join(staging_path, filename), "w+")
    json.dump(df_as_dict, temp_json, cls=NumpyEncoder, indent=2)
    temp_json.close()
    return temp_json.name


def df_to_csv(df: pd.DataFrame, staging_path: str, filename: str) -> str:
    """Converts a data frame into a csv file.

    Args:
        df (pd.DataFrame): DataFrame to be converted to a csv file
        staging_path (str): Path to staging directory
        filename (str): name of csv file to be created

    Returns:
        str: Returns a string containing the name of the new CSV file
    """

    temp_csv = open(os.path.join(staging_path, filename), "w+")
    df.to_csv(path_or_buf=temp_csv, index=False)
    temp_csv.close()
    return temp_csv.name


def dict_to_json(df: dict, staging_path: str, filename: str) -> str:
    """Converts a data dictionary into a JSON file.

    Args:
        df (dict): Dictionary to be converted to a JSON file
        staging_path (str): Path to staging directory
        filename (str): name of JSON file to be created

    Returns:
        str: Returns a string containing the name of the new JSON file
    """

    df_as_dict = [  # TODO explore the df.to_dict() function for this case
        {d: remove_non_values(v) if isinstance(v, dict) else v for d, v in df.items()}
    ]
    temp_json = open(os.path.join(staging_path, filename), "w+")
    json.dump(df_as_dict, temp_json, cls=NumpyEncoder, indent=2)
    temp_json.close()
    return temp_json.name


def list_to_json(df: List[Dict[str, Any]], staging_path: str, filename: str) -> str:
    """Converts a list into a JSON file.

    Args:
        df (list): List to be converted to a JSON file
        staging_path (str): Path to staging directory
        filename (str): name of JSON file to be created

    Returns:
        str: Returns a string containing the name of the new JSON file
    """

    temp_json = open(os.path.join(staging_path, filename), "w+")
    json.dump(df, temp_json, cls=NumpyEncoder, indent=2)
    temp_json.close()
    return temp_json.name
