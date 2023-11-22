import pandas as pd

import synapseclient


def get_entity_as_df(
    syn_id: str, source: str, syn: synapseclient.Synapse
) -> pd.DataFrame:
    """
    1. Creates and logs into synapseclient session (if not provided)
    2. Gets synapse entity from id string (and version number if provided)
    3. Converts data source to pd.Dataframe

    Args:
        syn_id (str): Synapse ID of entity to be loaded to df
        source (str): the source of the data to be loaded to df
        syn (synapseclient.Synapse): synapseclient.Synapse session object.

    Returns:
        pd.DataFrame: data frame generated from data source provided
    """

    syn_id_version = syn_id.split(".")
    synapse_id = syn_id_version[0]
    if len(syn_id_version) > 1:
        version = syn_id_version[1]
    else:
        version = None

    entity = syn.get(synapse_id, version=version)

    if source == "table":
        dataset = read_table_into_df(table_id=syn_id, syn=syn)
    elif source == "csv":
        dataset = read_csv_into_df(csv_path=entity.path)
    elif source == "tsv":
        dataset = read_tsv_into_df(tsv_path=entity.path)
    elif source == "feather":
        dataset = read_feather_into_df(feather_path=entity.path)
    elif source == "json":
        dataset = read_json_into_df(json_path=entity.path)
    else:
        raise ValueError("File type not supported.")

    return dataset


def read_csv_into_df(csv_path: str) -> pd.DataFrame:
    """
    Reads provided csv file into dataframe using file path

    Args:
        csv_path (str): path to input csv file

    Raises:
        ValueError: If file source is not .csv, raise error indicating that the
        configuration does not match the extension of the file provided

    Returns:
        pd.DataFrame: data frame created from csv file path
    """

    if csv_path.split(".")[-1] != "csv":
        raise ValueError(
            "Please make sure the source parameter in the configuration for "
            + f"{str(csv_path)} matches the file extension."
        )

    return pd.read_csv(csv_path, float_precision="round_trip")


def read_tsv_into_df(tsv_path: str) -> pd.DataFrame:
    """
    Reads provided tsv file into dataframe using file path

    Args:
        tsv_path (str): path to input tsv file

    Raises:
        ValueError: If file source is not .tsv, raise error indicating that the
        configuration does not match the extension of the file provided

    Returns:
        pd.DataFrame: data frame created from tsv file path
    """

    if tsv_path.split(".")[-1] != "tsv":
        raise ValueError(
            "Please make sure the source parameter in the configuration for "
            + f"{str(tsv_path)} matches the file extension."
        )

    return pd.read_csv(tsv_path, sep="\t")


def read_table_into_df(table_id: str, syn: synapseclient.Synapse) -> pd.DataFrame:
    """
    Reads a Synapse table into a dataframe.

    Args:
        table_id (str): Synapse ID for the Synapse table to be queried
        syn (synapseclient.Synapse): Synapse session object

    Returns:
        pd.DataFrame: data frame created from the Synaspe table query results
    """

    query = str("select * from {0}".format(table_id))
    query_result = syn.tableQuery(query)

    return query_result.asDataFrame()


def read_feather_into_df(feather_path: str) -> pd.DataFrame:
    """
    Reads provided feather file into dataframe using file path

    Args:
        feather_path (str): path to input feather file

    Raises:
        ValueError: If file source is not .feather, raise error indicating that the
        configuration does not match the extension of the file provided

    Returns:
        pd.DataFrame: data frame created from feather file path
    """

    if feather_path.split(".")[-1] != "feather":
        raise ValueError(
            "Please make sure the source parameter in the configuration for "
            + f"{str(feather_path)} matches the file extension."
        )

    return pd.read_feather(feather_path)


def read_json_into_df(json_path: str) -> pd.DataFrame:
    """
    Reads provided json file into dataframe using file path

    Args:
        json_path (str): path to input json file

    Raises:
        ValueError: If file source is not .json, raise error indicating that the
        configuration does not match the extension of the file provided

    Returns:
        pd.DataFrame: data frame created from json file path
    """

    if json_path.split(".")[-1] != "json":
        raise ValueError(
            "Please make sure the source parameter in the configuration for "
            + f"{str(json_path)} matches the file extension."
        )

    return pd.read_json(json_path, orient="records")
