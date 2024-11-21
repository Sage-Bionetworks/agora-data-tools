"""
This file includes several helper functions that are called from one or more of the pre-processing
notebooks. This helps avoid code duplication and/or keeps the notebooks cleaner and more straightforward.
Current public-facing functions:
    manual_query_biomart - queries Biomart with a GET request
    query_ensembl_version_api - queries the Ensembl API for Ensembl ID version info
    r_query_biomart - queries Biomart using rpy2
    filter_hasgs - removes human alternative sequence genes from a data frame
    get_all_adt_ensembl_ids - gets the Ensembl IDs in all of the files ingested by ADT
"""

import pandas as pd
import numpy as np
import requests
import re
import synapseclient
from io import StringIO
from typing import Union, Dict, List, Set
import agoradatatools.etl.utils as utils
import agoradatatools.etl.extract as extract


def manual_query_biomart(
    attributes: List[str], filters: Dict[str, Union[List[str], Set[str]]]
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


def query_ensembl_version_api(ensembl_ids: List[str]) -> pd.DataFrame:
    """
    Queries the Ensembl API via POST to get version information for each Ensembl ID. The API can only
    process 1000 IDs at a time so the query is broken into batches of 1000. If a request fails, this
    function will try again up to 5 times on that batch before quitting and raising an error.

    Args:
        ensembl_ids: a list of Ensembl IDs to query

    Returns:
        a pandas data frame with Ensembl IDs, version, and release information
    """
    url = "https://rest.ensembl.org/archive/id"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # We can only query 1000 genes at a time
    batch_ind = range(0, len(ensembl_ids), 1000)
    results = []

    for B in batch_ind:
        end = min(len(ensembl_ids), B + 1000)
        print("Querying genes " + str(B + 1) + " - " + str(end))

        request_data = '{ "id" : ' + str(ensembl_ids[B:end]) + " }"
        request_data = request_data.replace("'", '"')

        ok = False
        tries = 0

        while tries < 5 and not ok:
            try:
                res = requests.post(url, headers=headers, data=request_data)
                ok = res.ok
            except requests.RequestException as ex:
                print(ex)
                ok = False

            tries = tries + 1

            if not ok and tries == 5:
                res.raise_for_status()
            elif not ok:
                print(
                    "Error retrieving Ensembl versions for genes "
                    + str(B + 1)
                    + " - "
                    + str(end)
                    + ". Trying again..."
                )
            else:
                results = results + res.json()
                break

    versions = pd.json_normalize(results)
    return versions


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
    from rpy2.rinterface_lib.embedded import RRuntimeError

    r(
        'if (!require("BiocManager", character.only = TRUE)) { install.packages("BiocManager") }'
    )
    r('if (!require("biomaRt")) { BiocManager::install("biomaRt") }')

    r.library("biomaRt")

    # Sometimes Biomart doesn't respond and the command needs to be sent again. Try up to 5 times.
    for _ in range(5):
        try:
            mart = r.useEnsembl(biomart="ensembl", dataset="hsapiens_gene_ensembl")
            ensembl_ids = r.getBM(
                attributes=r.c("ensembl_gene_id", "chromosome_name", "hgnc_symbol"),
                mart=mart,
                useCache=False,
            )

        except RRuntimeError as ex:
            print(ex)
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


def get_all_adt_ensembl_ids(
    config_filename: str, exclude_files: List[str] = [], token: str = None
) -> List[str]:
    """
    Loops through an ADT config file, finds all data files that are ingested by ADT, and returns a
    list containing all Ensembl IDs present in those files. Specific files can be excluded from the
    list with the exclude_files argument.

    Args:
        config_filename: full or relative file path to the ADT config.yaml file
        exclude_files: list of file names to exclude when searching files for IDs. These names must
                       match what is in "name" field of the file specification in the config.yaml
                       file. Typical values are "gene_metadata" and "druggability".
        token: a Synapse auth token, or None if the user has Synapse credentials saved.

    Returns:
        a list of unique Ensembl IDs that exist in at least one data set ingested by ADT
    """
    syn = utils._login_to_synapse(token=token)
    config = utils._get_config(config_path=config_filename)
    datasets = config["datasets"]

    # Get all unique files in the config since some files are listed multiple times by being
    # included in multiple data sets. Also fetch all column rename values for standardizing Ensembl
    # ID column names
    unique_files = {}
    column_renames = {}

    for dataset in datasets:
        dataset_name = list(dataset.keys())[0]

        for file in dataset[dataset_name]["files"]:
            # Make the Synapse ID the key so that "update" will only add a new item if the ID doesn't
            # already exist
            unique_files.update({file["id"]: file})

        # Only some data sets have column rename values
        if "column_rename" in dataset[dataset_name].keys():
            column_renames.update(dataset[dataset_name]["column_rename"])

    # Print all the files we found
    print("Found " + str(len(unique_files)) + " files:")
    [print(x["name"] + ":\t" + x["id"]) for x in unique_files.values()]
    print("")

    # Create a list of all Ensembl IDs in all files
    file_ensembl_list = []

    for entity in unique_files.values():
        # Ignore json files, which are post-processed and not what we're interested in.
        # Also ignore any other files specified by 'exclude_files', which likely includes
        # "gene_metadata" and "druggability".
        if entity["format"] == "json" or entity["name"] in exclude_files:
            continue

        file_ensembl_ids = _extract_ensembl_ids(syn, entity, column_renames)
        file_ensembl_list = file_ensembl_list + file_ensembl_ids

    # Remove duplicate values
    return list(set(file_ensembl_list))


def _extract_ensembl_ids(
    syn: synapseclient.Synapse, entity: Dict[str, str], column_renames: Dict[str, str]
) -> List[str]:
    """
    Internal function used by get_all_adt_ensembl_ids to exctract a list of Ensembl IDs from a file.
    The file is downloaded from Synapse and read in as a pandas data frame, column names are renamed
    if necessary to ensure that most Ensembl ID columns are renamed to "ensembl_gene_id", and all
    Ensembl IDs from relevant columns are put in a list.

    Note that the "networks" data set contains two columns with Ensembl IDs (genea_ensembl_gene_id
    and geneb_ensembl_gene_id) which are not renamed, so this function searches for columns named
    with any of those two names or with "ensembl_gene_id" when finding Ensembl ID columns.

    Note that this function depends on the column_rename specifications in the config to accurately
    convert all Ensembl ID-containing columns in all files except networks to "ensembl_gene_id", so
    that we don't have to hard-code a list of all possible column names. This assumption is valid
    for the current set of data files and will likely remain valid for future data, but a warning
    is printed out if no matching column is found, just in case.

    Args:
        syn: a syanpseclient object which has already been initialized and successfully logged in
        entity: a dictionary containing keys "id", "name", and "format"
        column_renames: a dictionary containing all column rename pairs from the config file, where
                        key = old column name, and value = new column name

    Returns:
        a list of unique Ensembl IDs in the file, or an empty list if no Ensembl ID column found
    """
    df = extract.get_entity_as_df(syn_id=entity["id"], source=entity["format"], syn=syn)

    # Use column_renames from the config to convert most Ensembl ID column names to "ensembl_gene_id".
    df = utils.standardize_column_names(df=df)
    df = utils.rename_columns(df=df, column_map=column_renames)

    # Exception to the above comment: the 'networks' file has two ID columns (genea_ and geneb_ ensembl_gene_id)
    # which do not get renamed
    possible_col_names = [
        "ensembl_gene_id",
        "genea_ensembl_gene_id",
        "geneb_ensembl_gene_id",
    ]

    file_ensembl_ids = []

    # The data may have zero, one, or more than one (in the case of 'networks') column of Ensembl IDs
    for C in possible_col_names:
        if C in df.columns:
            file_ensembl_ids = file_ensembl_ids + df[C].tolist()

    # Print any warnings and remove any NA values from the list before returning
    if len(file_ensembl_ids) == 0:
        print("WARNING: no Ensembl ID column found for " + entity["name"] + "!")

    if "n/A" in file_ensembl_ids:
        print(entity["name"] + " has an n/A Ensembl ID")
        file_ensembl_ids.remove("n/A")

    if np.NaN in file_ensembl_ids:
        print(
            entity["name"]
            + " has "
            + str(file_ensembl_ids.count(np.NaN))
            + " NaN Ensembl IDs"
        )
        file_ensembl_ids = [x for x in file_ensembl_ids if x is not np.NaN]

    # Remove duplicate values
    return list(set(file_ensembl_ids))
