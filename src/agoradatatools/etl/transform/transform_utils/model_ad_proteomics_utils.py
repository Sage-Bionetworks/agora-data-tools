"""UniProt accession and Comparison Tool identity helpers for protein differential expression.

Used by protein_de_individual and protein_de_aggregate. Gene resolution is a shared
contract: both collections take the smallest mouse Ensembl id for a UniProt accession,
try the full accession, then fall back to the base accession.
"""

import pandas as pd

from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    filter_to_mouse_genes,
)


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
    """Map each accession to an Ensembl id, trying the full accession then the base."""
    mapped = accessions.map(uniprot_to_ensembl)
    base_mapped = accessions.str.split("-").str[0].map(uniprot_to_ensembl)
    return mapped.fillna(base_mapped)


def unmapped_accessions(
    accessions: pd.Series, uniprot_to_ensembl: dict[str, str]
) -> list[str]:
    """Return sorted accessions missing from the map as themselves and as their base."""
    return sorted(
        {
            accession
            for accession in accessions
            if accession not in uniprot_to_ensembl
            and accession.split("-")[0] not in uniprot_to_ensembl
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
    # MG-985: fall back to the Ensembl gene id when no symbol is known.
    return (
        gene_symbol.where(gene_symbol != "", ensembl_gene_id) + " (" + uniprotid + ")"
    )
