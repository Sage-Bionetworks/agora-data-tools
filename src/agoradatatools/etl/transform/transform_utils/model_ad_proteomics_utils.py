"""UniProt accession and Comparison Tool identity helpers for protein differential expression.

Used by protein_de_individual and protein_de_aggregate. Gene resolution is a shared
contract: both collections take the smallest mouse Ensembl id for a UniProt accession,
looked up on the base accession with any isoform notation removed.
"""

import logging

import pandas as pd

from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    filter_to_mouse_genes,
)

logger = logging.getLogger(__name__)


def build_uniprot_to_ensembl(mapping_df: pd.DataFrame) -> dict[str, str]:
    """Map each UniProt accession to the smallest of its mouse Ensembl gene ids."""
    return (
        filter_to_mouse_genes(mapping_df)
        .groupby("uniprot_id")["ensembl_gene_id"]
        .min()
        .to_dict()
    )


def lookup_ensembl(
    accessions: pd.Series, uniprot_to_ensembl: dict[str, str]
) -> pd.Series:
    """Map each accession to an Ensembl ID, removing isoform notation ("-<number>") from the
    accessions before mapping."""
    base_accessions = accessions.str.split("-").str[0]
    return base_accessions.map(uniprot_to_ensembl)


def unmapped_accessions(
    accessions: pd.Series, uniprot_to_ensembl: dict[str, str]
) -> list[str]:
    """Return sorted accessions missing from the map. The base accession (isoform notation removed)
    is used for lookup."""
    return sorted(
        {
            accession
            for accession in accessions
            if accession.split("-")[0] not in uniprot_to_ensembl
        }
    )


def canonical_accession(headers: pd.Series) -> pd.Series:
    """Recover the canonical UniProt accession from gene_symbol|uniprotid strings.

    The pipeline lowercases wide-file headers and converts isoform hyphens to
    underscores; upper-casing and restoring the hyphen recovers the accession
    losslessly (ank2|q8c8r3_2 -> Q8C8R3-2). Aggregate DE files store the same
    string as a cell value that is already canonical; the same steps are
    idempotent. UniProt accessions never contain an underscore.
    """
    return (
        headers.str.rsplit("|", n=1)
        .str[-1]
        .str.upper()
        .str.replace("_", "-", regex=False)
    )


def protein_unique_id(ensembl_gene_id: pd.Series, uniprotid: pd.Series) -> pd.Series:
    """Concatenate ensembl_gene_id and uniprotid into the Comparison Tool unique_id."""
    return ensembl_gene_id.astype(str) + uniprotid.astype(str)


def protein_display_symbol(
    gene_symbol: pd.Series, ensembl_gene_id: pd.Series, uniprotid: pd.Series
) -> pd.Series:
    """Format gene_symbol (uniprotid), falling back to the Ensembl id when the symbol is empty."""
    return (
        gene_symbol.where(gene_symbol != "", ensembl_gene_id) + " (" + uniprotid + ")"
    )


def map_accessions_to_genes(
    df: pd.DataFrame,
    uniprot_to_ensembl: dict[str, str],
    context: str,
) -> pd.DataFrame:
    """Attach ensembl_gene_id from the base accession and drop rows the map does not cover.

    context is the phrase that follows "No rows remained" in the error, so each
    caller can name the file or model_group that produced the empty result.
    """
    mapped = df.copy()
    mapped["ensembl_gene_id"] = lookup_ensembl(mapped["uniprotid"], uniprot_to_ensembl)
    mapped = mapped.dropna(subset=["ensembl_gene_id"])
    if mapped.empty:
        raise ValueError(
            f"No rows remained {context} after mapping proteins to "
            "genes — check the UniProt to Ensembl mapping file."
        )
    return mapped


def add_protein_identity_columns(
    df: pd.DataFrame, gene_symbols: dict[str, str]
) -> pd.DataFrame:
    """Attach gene_symbol, unique_id, and display_symbol from the resolved Ensembl id."""
    identified = df.copy()
    identified["gene_symbol"] = (
        identified["ensembl_gene_id"].map(gene_symbols).fillna("")
    )
    identified["unique_id"] = protein_unique_id(
        identified["ensembl_gene_id"], identified["uniprotid"]
    )
    identified["display_symbol"] = protein_display_symbol(
        identified["gene_symbol"],
        identified["ensembl_gene_id"],
        identified["uniprotid"],
    )
    return identified


def log_unmapped_accessions(
    accessions: pd.Series, uniprot_to_ensembl: dict[str, str]
) -> None:
    """Log accessions the map does not cover. Callers drop those rows during gene lookup."""
    unmapped = unmapped_accessions(accessions, uniprot_to_ensembl)
    if unmapped:
        logger.info(
            f"{len(unmapped)} UniProt IDs absent from the map (dropped): {unmapped[:10]}"
        )
