from . import utils
import pandas as pd
import synapseclient
import sys
import errno


def get_entity_as_df(syn_id: str, format: str, syn=None):
    """
    Looks at the format of the file at the source, extracts it,
    and returns it as a data frame
    :param syn_id: synapse id of the entity
    :param format: original format of the file in synapse
    :return: a data frame
    """
    if syn is None:
        syn = utils._login_to_synapse()

    try:
        entity = syn.get(syn_id)
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(str(syn_id) + " is not a valid Synapse id")
        sys.exit(1)

    if format == "table":
        dataset = read_table_into_df(table_id=syn_id, syn=syn)
    elif format == "csv":
        dataset = read_csv_into_df(csv_path=entity.path)
    elif format == "tsv":
        dataset = read_tsv_into_df(tsv_path=entity.path)
    elif format == "feather":
        dataset = read_feather_into_df(feather_path=entity.path)
    elif format == 'json':
        dataset = read_json_into_df(json_path=entity.path)
    else:
        print("File type not supported.")
        sys.exit(errno.EBADF)

    return dataset



'''Reads in a csv file into a dataframe'''
def read_csv_into_df(csv_path: str):
    if csv_path.split(".")[-1] != "csv":
        print("Please make sure the format parameter in the configuration for "
              + f"{str(csv_path)} matches the file extension.")
        sys.exit(errno.EBADF)

    return pd.read_csv(csv_path)

'''Reads in a tsv file into a dataframe'''
def read_tsv_into_df(tsv_path: str):

    if tsv_path.split(".")[-1] != "tsv":
        print("Please make sure the format parameter in the configuration for "
              + f"{str(tsv_path)} matches the file extension.")
        sys.exit(errno.EBADF)

    return pd.read_csv(tsv_path, sep="\t")


def read_table_into_df(table_id: str, syn) -> pd.DataFrame:
    '''Reads a Synapse table into a dataframe.\n
    Arguments: the id of a Synapse Table and a Synapse Object
    Returns: a pandas dataframe
    '''

    query = str("select * from {0}".format(table_id))

    try:
        query_result = syn.tableQuery(query)
    except:
        print("Please provide a queriable entity.")
        sys.exit(1)

    return query_result.asDataFrame()


def read_feather_into_df(feather_path: str):
    """
    Reads a feather file from synapse
    """

    if feather_path.split(".")[-1] != "feather":
        print("Please make sure the format parameter in the configuration for "
              + f"{str(feather_path)} matches the file extension.")
        sys.exit(errno.EBADF)

    return pd.read_feather(feather_path)

def read_json_into_df(json_path: str):
    """
    Reads a json file from synapse into a dataframe
    """

    if json_path.split(".")[-1] != "json":
        print("Please make sure the format parameter in the configuration for "
              + f"{str(json_path)} matches the file extension.")
        sys.exit(errno.EBADF)

    return pd.read_json(json_path, orient='records')
