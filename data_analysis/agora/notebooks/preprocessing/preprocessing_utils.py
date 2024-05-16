import pandas as pd
import requests
import re
from io import StringIO
from typing import Union


def manual_query_biomart(
    attributes: list[str], filters: dict[Union[list, set]]
) -> pd.DataFrame:
    """Performs a GET request to the Biomart web service and returns the response. There is no
    canonical Python library to query Biomart and no Python library at all to query on
    'external_gene_name', so this function is necessary for those cases where importing and using
    R in the notebook is cumbersome.

    Args:
        attributes (list[str]): a list of attributes that Biomart should return as columns. Common
                                options are ['ensembl_gene_id', 'external_gene_name', 'chromosome_name']
        filters (dict[list,set]): a dict where the keys are the attribute to filter on, and the values are a
                                list or set of valid items. Example: {'external_gene_name': set(list_of_symbols)}

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


def filter_hasgs(df: pd.DataFrame, chromosome_name_column: str) -> pd.DataFrame:
    """Filters human alternative sequence genes (HASGs) from a data frame by using a regex to
    identify them for removal. Valid genes will either have a numerical chromosome name or have
    "X", "Y", or "MT" as the chromosome name. All other chromosome name formats correspond to
    HASGs, patches, and other genes that should be removed from the list.

    Args:
        df (pd.DataFrame): a data frame as returned from a Biomart query, that must have a column
                           containing chromosome names.
        chromosome_name_column (str): the name of the column that contains chromosome names

    Returns:
        df_filt (pd.DataFrame): a copy of the input data frame with rows corresponding to HASGs
                                removed.
    """
    regex = re.compile("^(\d|X|Y|MT)")
    keep = df[chromosome_name_column].apply(
        # Keep rows if they have a numerical chromosome name, or have X, Y, or MT
        lambda row: (
            re.match(regex, row) is not None if isinstance(row, str) else True
        )  # Always true for numbers
    )

    df_filt = df.copy().loc[keep].reset_index(drop=True)
    return df_filt


def r_query_biomart() -> pd.DataFrame:
    """Uses rpy2 to query BioMart for all genes. This function is no longer used but is here in case we need it again.

    Args:
        none

    Returns:
        ensembl_ids_df (pd.DataFrame): a data frame including columns "ensembl_gene_id",
                                      "chromosome_name", and "hgnc_symbol" retrived from BioMart
    """
    from rpy2.robjects import r

    r(
        'if (!require("BiocManager", character.only = TRUE)) { install.packages("BiocManager") }'
    )
    r('if (!require("biomaRt")) { BiocManager::install("biomaRt") }')

    r.library("biomaRt")

    # Sometimes Biomart doesn't respond and the command needs to be sent again. Try up to 5 times.
    for T in range(5):
        try:
            mart = r.useEnsembl(biomart="ensembl", dataset="hsapiens_gene_ensembl")
            ensembl_ids = r.getBM(
                attributes=r.c("ensembl_gene_id", "chromosome_name", "hgnc_symbol"),
                mart=mart,
                useCache=False,
            )

        except:
            print("Trying again...")
            ensembl_ids = None
        else:
            break

    if ensembl_ids is None or ensembl_ids.nrow == 0:
        print("Biomart was unresponsive after 5 attempts. Try again later.")
        return pd.DataFrame()
    else:
        # Convert the ensembl_gene_id column from R object to a python list
        ensembl_ids_df = pd.DataFrame(
            {
                "ensembl_gene_id": list(ensembl_ids.rx2("ensembl_gene_id")),
                "chromosome_name": list(ensembl_ids.rx2("chromosome_name")),
                "hgnc_symbol": list(ensembl_ids.rx2("hgnc_symbol")),
            }
        )
        return ensembl_ids_df
