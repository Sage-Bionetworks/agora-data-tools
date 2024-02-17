from typing import Union

import pandas as pd

from agoradatatools.etl.utils import nest_fields


def count_grouped_total(
    df: pd.DataFrame,
    grouping: Union[str, list],
    input_colname: str,
    output_colname: str,
) -> pd.DataFrame:
    """For each unique item/combination in the column(s) specified by grouping,
    counts the number of unique items in the column [input_colname] that
    correspond to that grouping. The calculated counts are put in a new
    column and named with [output_colname].
    Args:
        df (pd.DataFrame): contains columns listed in grouping and
                           input_colname. May contain other columns as well, but
                           these will be dropped from the returned data frame.
        grouping (str or list): a string with a single column name, or a list of
                                strings for multiple column names
        input_colname (str): the name of the column to count
        output_colname (str): the name of the new column with calculated counts
    Returns:
        pd.DataFrame: a data frame containing the grouping column(s) and a
                      new column for output_colname, which contains the count of
                      unique items in input_colname for each grouping item.
    """
    df = (
        df.groupby(grouping)[input_colname]
        .nunique()
        .reset_index()
        .rename(columns={input_colname: output_colname})
    )
    return df


def split_ensembl_ids(genes_biodomains: pd.DataFrame) -> pd.DataFrame:
    """The "ensembl_gene_id" column in the genes_biodomains data frame has some single Ensembl IDs and some rows with a
    semicolon-separated list of Ensembl IDs. This function finds the rows with semicolons, adds rows to the
    genes_biodomains dataframe such that there is one row per Ensembl ID in that list, and assigns a single Ensembl ID
    to each row.

    Args:
        genes_biodomains (pd.DataFrame): DataFrame containing a column named "ensembl_gene_id"

    Returns:
        pd.DataFrame: a DataFrame with the same columns as the input but with additional rows added, plus the
                      "ensembl_gene_id" column only has one Ensembl ID per row.
    """

    # Split the whole column on ";". Rows that don't need to be split will have a length of 1, while rows that do need
    # to be split will have 2 or more in the list.
    ens_lists = genes_biodomains["ensembl_gene_id"].str.split(pat=";")
    needs_split = ens_lists.apply(len) > 1

    # Edit the rows where needs_split is True, referencing by the DataFrame index
    for df_ind in needs_split.index[needs_split]:
        ensembl_ids = ens_lists[df_ind]

        # Guard against extra semicolons or ending the string with a semicolon, which will both result in a blank
        # character as an Ensembl ID
        ensembl_ids = [x for x in ensembl_ids if x != ""]

        # If there is still more than one Ensembl ID in the list after removing '', add as many new rows as there are
        # (Ensembl IDs - 1), since there is already 1 row in the data frame for this group of IDs
        if len(ensembl_ids) > 1:
            row_dupe = genes_biodomains.loc[df_ind].copy().to_frame().T

            genes_biodomains = pd.concat(
                [genes_biodomains] + [row_dupe] * (len(ensembl_ids) - 1)
            )

            # The added rows plus the original row all have the same index, so this sets all rows with that index at once.
            genes_biodomains.at[df_ind, "ensembl_gene_id"] = ensembl_ids

        else:
            genes_biodomains.at[df_ind, "ensembl_gene_id"] = ensembl_ids[0]

    return genes_biodomains


def transform_genes_biodomains(datasets: dict) -> pd.DataFrame:
    """Takes dictionary of dataset DataFrames, extracts the genes_biodomains
    DataFrame, calculates some metrics on GO terms per gene / biodomain, and
    performs nest_fields on the final DataFrame. This results in a 2 column
    DataFrame grouped by "ensembl_gene_id" and includes a collapsed nested
    dictionary field "gene_biodomains"

    Args:
        datasets (dict[str, pd.DataFrame]): dictionary of dataset names mapped to their DataFrame

    Returns:
        pd.DataFrame: 2 column DataFrame grouped by "ensembl_gene_id" including
                      a collapsed nested dictionary field "gene_biodomains"
    """
    genes_biodomains = datasets["genes_biodomains"]
    interesting_columns = ["ensembl_gene_id", "biodomain", "go_terms"]
    genes_biodomains = genes_biodomains[interesting_columns].dropna()

    genes_biodomains = split_ensembl_ids(genes_biodomains)
    genes_biodomains = genes_biodomains.reset_index(drop=True)

    # Count the number of go_terms associated with each biodomain
    n_biodomain_terms = count_grouped_total(
        genes_biodomains, "biodomain", "go_terms", "n_biodomain_terms"
    )

    # Count the number of go_terms associated with each gene, ignoring biodomain
    n_gene_total_terms = count_grouped_total(
        genes_biodomains, "ensembl_gene_id", "go_terms", "n_gene_total_terms"
    )

    # Count the number of go_terms associated with each gene / biodomain combo
    n_gene_biodomain_terms = count_grouped_total(
        genes_biodomains,
        ["ensembl_gene_id", "biodomain"],
        "go_terms",
        "n_gene_biodomain_terms",
    )

    # Group rows by ensg and biodomain to produce nested lists of go_terms per ensg/biodomain
    genes_biodomains = (
        genes_biodomains.groupby(["ensembl_gene_id", "biodomain"])["go_terms"]
        .apply(list)
        .reset_index()
    )

    # Merge all the different count metrics into the main data frame so each
    # ensembl_gene_id / biodomain combo has an entry for each count
    genes_biodomains = (
        genes_biodomains.merge(n_gene_total_terms, on="ensembl_gene_id", how="left")
        .merge(n_biodomain_terms, on="biodomain", how="left")
        .merge(n_gene_biodomain_terms, on=["ensembl_gene_id", "biodomain"], how="left")
    )

    # Calculate percent linking terms:
    # n_gene_biodomain_terms / n_gene_total_terms * 100
    genes_biodomains["pct_linking_terms"] = (
        genes_biodomains["n_gene_biodomain_terms"]
        / genes_biodomains["n_gene_total_terms"]
        * 100
    ).round(decimals=2)

    # Remove n_gene_total_terms column
    genes_biodomains = genes_biodomains.drop(columns="n_gene_total_terms")

    genes_biodomains = nest_fields(
        df=genes_biodomains,
        grouping="ensembl_gene_id",
        new_column="gene_biodomains",
        drop_columns="ensembl_gene_id",
    )

    return genes_biodomains
