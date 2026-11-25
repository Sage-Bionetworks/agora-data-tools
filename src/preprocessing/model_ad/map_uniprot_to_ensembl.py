"""
Create a UniProt -> Ensembl ID Mapping File

This script creates a file that maps UniProt accession numbers onto Ensembl IDs. It uses all UniProt IDs that are
present in the Model-AD proteomics source files, queries `mygene` for matching Ensembl IDs, and writes the file to a
csv. The output file has columns "uniprot_id" and "ensembl_gene_id", and only contains UniProt IDs that successfully map
to at least one Ensembl ID.

Installation requirements:

Install Python and agora-data-tools following the instructions in this repository's README. This script assumes it is
being run from the same `pipenv` virtual environment as agora-data-tools.

This script also requires the `mygene` Python package, which can be installed using `pip`:
```
pip install mygene
```
"""

import os
from pathlib import Path
import pandas as pd
import mygene
import synapseclient
from agoradatatools.etl import extract, load, utils
from preprocessing.preprocessing_utils import (
    DEFAULT_MODEL_AD_CONFIG_FILE,
    DEFAULT_MODEL_AD_SOURCE_FOLDER_ID,
    get_nested_dict_item,
    get_staging_path,
)

# Default filename for the UniProt -> Ensembl mapping output
DEFAULT_OUTPUT_FILENAME = "uniprot_to_ensembl_mapping.csv"

# Config file item names
PROTEOMICS_DATA_SETS = ["protein_de_individual", "protein_de_aggregate"]
PROTEOMICS_FILE_MAPS = {
    "protein_de_individual": "model_map",
    "protein_de_aggregate": "file_metadata",
}


def get_proteomics_file_syn_ids(config_file: str) -> list[str]:
    """
    Look through the Model-AD config file, find all proteomics source files, and return their Synapse IDs. This
    function assumes that names of each proteomics data file can be in the config file found under:
        "protein_de_individual" -> the "model_map" section
        "protein_de_aggregate" -> the "file_metadata" section

    Searching for files this way avoids having to pattern match file names or unnecessarily read in metadata files that
    are also listed in each data set's set of files.

    Args:
        config_file: Path to the Model-AD configuration file

    Returns:
        A list of Synapse IDs corresponding to the proteomics source files

    Raises:
        ValueError: If the individual or aggregate proteomics datasets are missing from the config
        ValueError: If no proteomics data files exist in the "model_map" and/or "file_metadata" config section for the
            respective datasets.
    """
    config = utils._get_config(config_path=config_file)

    # config["datasets"] is a list of single-key dicts of the format {<dataset_name>: <dataset_dict>}.
    # Extract the two proteomics datasets from the list.
    datasets = {
        key: dataset[key]
        for dataset in config["datasets"]
        for key in dataset
        if key in PROTEOMICS_DATA_SETS
    }

    # Get the names of the proteomics data files from the dataset configs. Filenames are the keys in the "model_map" or
    # "file_metadata" section
    data_files = []

    for dataset_name, file_map in PROTEOMICS_FILE_MAPS.items():
        if dataset_name not in datasets:
            raise ValueError(f"Missing {dataset_name} dataset in the config.")

        files = get_nested_dict_item(datasets[dataset_name], file_map)
        if not files:
            raise ValueError(
                f"No proteomics data files found in {dataset_name}/{file_map} in the config."
            )

        data_files.extend(files.keys())

    # Get the Synapse ID for each file. The names in "data_files" match the "name" field in the config["files"] list.
    syn_ids = {file["id"] for file in config["files"] if file["name"] in data_files}
    print(f"\nFound {len(syn_ids)} proteomics data files.\n")

    return list(syn_ids)


def extract_ids_from_file(df: pd.DataFrame) -> list[str]:
    """
    Pull out UniProt IDs based on dataframe format. There are two different file formats: individual results have the
    uniprot IDs as column headers plus columns for "individualID" and "specimenID", while DE results have IDs in the
    "Protein ID" column.

    Args:
        df: A pandas DataFrame containing proteomics data.

    Returns:
        A list of IDs extracted from the DataFrame.

    Raises:
        ValueError: If the DataFrame format is unrecognized.
    """
    if {"specimenID", "individualID"}.issubset(df.columns):
        return df.drop(columns=["specimenID", "individualID"]).columns.tolist()
    elif "Protein ID" in df.columns:
        return df["Protein ID"].tolist()
    raise ValueError("Unrecognized dataframe format")


def format_uniprot_ids(header_ids: set[str]) -> pd.DataFrame:
    """
    Converts IDs from the format "gene_symbol|uniprot_id" into a DataFrame with separate columns for gene symbols and
    UniProt IDs. Isoform notation (e.g., "-1") is also removed.

    Args:
        header_ids: A set of header IDs in the format "gene_symbol|uniprot_id".

    Returns:
        A pandas DataFrame with columns "gene_symbol", "original_uniprot_id", and "uniprot_id".
    """
    # Remove any NA or empty string values from header_ids before splitting the strings
    header_ids_clean = {
        header_id for header_id in header_ids if header_id and "|" in header_id
    }

    # Print a message if NA values were found, as a warning
    if len(header_ids) != len(header_ids_clean):
        print(
            f"{len(header_ids) - len(header_ids_clean)} NA values found in `header_ids`"
        )

    # Split into gene symbols and Uniprot IDs
    uniprot_dict = [
        {"gene_symbol": symbol, "original_uniprot_id": uniprot}
        for symbol, uniprot in (header_id.split("|") for header_id in header_ids_clean)
    ]
    uniprot_df = pd.DataFrame(uniprot_dict)

    # Remove isoform notation from uniprot IDs
    uniprot_df["uniprot_id"] = uniprot_df["original_uniprot_id"].str.split("-").str[0]

    return uniprot_df


def _extract_ensembl_ids(item: dict | list | float) -> list[str]:
    """
    Helper function for `format_mygene_results`.
    Extracts Ensembl IDs from the given item, which may be a dict of {"gene": "<ensembl_id>"}, a list of such dicts, or
    NA (np.NaN is a float type).
    """
    if isinstance(item, list):
        return [x["gene"] for x in item]
    if isinstance(item, dict):
        return [item["gene"]]
    return []


def format_mygene_results(results: dict[str, list[dict | str]]) -> pd.DataFrame:
    """
    Formats the results returned by MyGene into a DataFrame with one row per Ensembl ID. MyGene results are a nested
    dictionary, and also include Ensembl IDs for related species that share the same UniProt ID. This function filters
    to human and mouse Ensembl IDs only, and expands lists of Ensembl IDs into separate rows in cases where a UniProt
    ID maps to more than one Ensembl ID.

    Args:
        results: The nested dictionary returned by MyGene, with the mapped IDs stored under the "out" key

    Returns:
        A pandas DataFrame with columns "uniprot_id" and "ensembl_gene_id", containing only UniProt IDs that were
        successfully mapped to at least one Ensembl ID.
    """
    res_df = pd.DataFrame(results["out"]).rename(columns={"query": "uniprot_id"})
    res_df["ensembl_gene_id"] = res_df["ensembl"].apply(_extract_ensembl_ids)

    # Make one row per Ensembl ID and drop any rows that didn't map to any Ensembl ID
    res_df = (
        res_df[["uniprot_id", "ensembl_gene_id"]]
        .explode("ensembl_gene_id")
        .dropna(subset=["ensembl_gene_id"])
    )

    # Remove non-MUSG Ensembl IDs: mygene searches all mouse species, not just Mus musculus, and
    # returns those results as well.
    res_df = res_df[res_df["ensembl_gene_id"].str.startswith("ENSMUSG")]

    # Sort the result for easier file diffs during updates
    return res_df.sort_values(["uniprot_id", "ensembl_gene_id"])


def print_stats(data_uniprot_df: pd.DataFrame, mapping: pd.DataFrame) -> None:
    """
    Prints statistics about the mapping between UniProt IDs and Ensembl IDs:
    - Number of UniProt IDs that do not map to any Ensembl ID
    - Number of UniProt IDs that map to at least one Ensembl ID
    - Number of Ensembl IDs that map to more than one UniProt ID
    - Number of UniProt IDs that map to more than one Ensembl ID

    Args:
        data_uniprot_df: DataFrame containing all UniProt IDs in all the proteomics data files
        mapping: DataFrame containing the mapping between UniProt IDs and Ensembl IDs, which does not include unmapped
            UniProt IDs.

    Returns:
        None
    """
    total = len(set(data_uniprot_df["uniprot_id"]))
    print(f"\n{total} unique UniProt IDs in the data files")

    # Number of unmapped IDs
    missing = len(set(data_uniprot_df["uniprot_id"]) - set(mapping["uniprot_id"]))
    pct = round(missing * 100 / total, ndigits=2)

    print(f"{missing} ({pct:.2f}%) UniProt IDs do not map to any Ensembl ID")

    # Total number of Ensembl IDs that match to a UniProt accession
    matches = len(set(mapping["uniprot_id"]))
    pct2 = round(matches * 100 / total, ndigits=2)

    print(f"{matches} ({pct2:.2f}%) Uniprot IDs map to at least one Ensembl ID")

    # Ensembl IDs that match to more than one UniProt accession
    dupes = mapping["ensembl_gene_id"][mapping["ensembl_gene_id"].duplicated()]
    print(f"{len(set(dupes))} Ensembl IDs map to more than one UniProt accession")

    # UniProt accessions that match to more than one Ensembl ID
    dupes2 = mapping["uniprot_id"][mapping["uniprot_id"].duplicated()]
    print(f"{len(set(dupes2))} UniProt accessions map to more than one Ensembl ID\n")


def map_accessions(
    config_file: str,
    output_file: str,
    upload_to_synapse: bool = False,
    upload_synid: str | None = None,
) -> None:
    """
    Main mapping function for UniProt to Ensembl IDs. The process is as follows:
        1. Read all proteomics data files specified in the config
        2. Extract all UniProt IDs from the files and put them into a DataFrame
        3. Query MyGene for Uniprot -> Ensembl mapping
        4. Filter and clean the results
        5. Save the mapping to a file and upload to Synapse

    Args:
        config_file: Path to the Model-AD configuration file used for the main ADT pipeline
        output_file: Path to save the resulting UniProt to Ensembl mapping.
        upload_to_synapse: Whether to upload the resulting file to Synapse.
        upload_synid: Synapse ID of the destination folder for the upload. Required if upload_to_synapse is True.

    Returns:
        None
    """
    syn = synapseclient.Synapse()
    syn.login(silent=True)

    # 1. Read all proteomics files specified in the config
    syn_ids = get_proteomics_file_syn_ids(config_file)

    dfs = [
        extract.get_entity_as_df(syn_id=syn_id, source="csv", syn=syn)
        for syn_id in syn_ids
    ]

    # 2. Extract all UniProt IDs from the files and put them into a DataFrame
    header_ids = [extract_ids_from_file(df) for df in dfs]

    # flatten into one list with unique values
    header_ids = {item for sublist in header_ids for item in sublist}

    uniprot_df = format_uniprot_ids(header_ids)

    # 3. Query MyGene for Uniprot -> Ensembl mapping
    mg = mygene.MyGeneInfo()
    results = mg.querymany(
        set(uniprot_df["uniprot_id"]),
        scopes="uniprot",
        fields=["ensembl.gene"],
        species="mouse",
        returnall=True,
    )

    # 4. Filter and clean the results
    mapping = format_mygene_results(results)

    # Extra information printouts
    print_stats(uniprot_df, mapping)

    # 5. Save the mapping to a file and upload to Synapse
    staging_path = get_staging_path(config_file)
    output_file = load.df_to_csv(
        mapping, staging_path=staging_path, filename=output_file
    )

    if upload_to_synapse:
        if upload_synid is None:
            raise ValueError(
                "upload_synid must be provided if upload_to_synapse is True"
            )

        syn_file = load.load(
            output_file, provenance=syn_ids, destination=upload_synid, syn=syn
        )
        print(f"\nUploaded to {syn_file[0]}.{syn_file[1]}\n")


if __name__ == "__main__":
    # This file is located at agora-data-tools/src/preprocessing/model_ad/map_uniprot_to_ensembl.py,
    # so the repository root is 4 parents up (0-indexed)
    repo_root = Path(__file__).parents[3]

    # Force the working directory to be the repository root no matter where this script is run from
    os.chdir(repo_root)

    # Run main function
    map_accessions(
        config_file=DEFAULT_MODEL_AD_CONFIG_FILE,
        output_file=DEFAULT_OUTPUT_FILENAME,
        upload_to_synapse=True,
        upload_synid=DEFAULT_MODEL_AD_SOURCE_FOLDER_ID,
    )
