import pandas as pd
import requests
import re
from io import StringIO


def manual_query_biomart(attributes: list[str], filters: dict) -> pd.DataFrame:
    """Performs a GET request to the Biomart web service and returns the response. There is no
    canonical Python library to query Biomart and no Python library at all to query on
    'external_gene_name', so this function is necessary for those cases where importing and using
    R in the notebook is cumbersome.

    Args:
        attributes (list): a list of attributes that Biomart should return as columns. Common
                           options are ['ensembl_gene_id', 'external_gene_name', 'chromosome_name']
        filters (dict): a dict where the keys are the attribute to filter on, and the values are a
                        list of valid items. Example: {'external_gene_name': set(list_of_symbols)}

    Returns:
        result (pd.DataFrame): Biomart's response in DataFrame format, where columns should match
                               the attributes list and rows contain results that match the filter
                               values.
    """
    query = (
        '<Query  virtualSchemaName = "default" formatter = "TSV" header = "1" uniqueRows = "0"'
        + ' count = "" datasetConfigVersion = "0.6" >'
    )
    query = query + '<Dataset name = "hsapiens_gene_ensembl" interface = "default" >'

    for name, value in filters.items():
        query = (
            query + '<Filter name = "' + name + '" value = "' + ",".join(value) + '"/>'
        )

    for attr in attributes:
        query = query + '<Attribute name = "' + attr + '" />'

    query = query + "</Dataset>"
    query = query + "</Query>"

    response = requests.get(
        url="https://www.ensembl.org/biomart/martservice", params={"query": query}
    )

    result = pd.read_csv(StringIO(response.text), sep="\t")
    return result


def filter_HASGs(df: pd.DataFrame, chromosome_name_column: str) -> pd.DataFrame:
    """Filters human alternative sequence genes (HASGs) from a data frame by using a regex to
    identify them for removal. Valid genes will either have a numerical chromosome name or have
    "X", "Y", or "MT" as the chromosome name. All other chromosome name formats correspond to
    HASGs, pathches, and other genes that should be removed from the list.

    Args:
        df (pd.DataFrame): a data frame as returned from a Biomart query, that must have a column
                           containing chromosome names. Biomart already returns chromosome names
                           that fit the above regex pattern, but any manual creation of chromosome
                           names should be formatted properly so the regex works before using this
                           function.
        chromosome_name_column (str): the name of the column that contains chromosome names

    Returns:
        df_filt (pd.DataFrame): a copy of the input data frame with rows corresponding to HASGs
                                removed.
    """
    regex = re.compile("^[0-9]|X|Y|MT")
    keep = df[chromosome_name_column].apply(
        # Keep rows if they have a numerical chromosome name, or have X, Y, or MT
        lambda row: re.match(regex, row) is not None
        if isinstance(row, str)
        else True  # Always true for numbers
    )

    df_filt = df.copy().loc[keep].reset_index(drop=True)
    return df_filt
