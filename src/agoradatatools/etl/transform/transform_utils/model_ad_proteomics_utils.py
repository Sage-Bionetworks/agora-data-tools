"""Shared UniProt and protein-identity helpers for the Model AD protein transforms.

Used by protein_de_individual and protein_de_aggregate. Gene resolution is a shared
contract: both collections render the same protein, so they must pick the same Ensembl
gene. unique_id and display_symbol are the Comparison Tool identity fields and must
not drift between the two.
"""

import pandas as pd


def build_uniprot_candidates(mapping_df: pd.DataFrame) -> dict[str, list[str]]:
    """Map each UniProt accession to its candidate mouse Ensembl gene ids, smallest first."""
    mouse = mapping_df[
        mapping_df["ensembl_gene_id"].astype(str).str.startswith("ENSMUSG")
    ]
    return {
        accession: sorted(genes)
        for accession, genes in mouse.groupby("uniprotkb_accession")["ensembl_gene_id"]
        .unique()
        .items()
    }


def build_gene_aliases(mouse_gene_metadata_df: pd.DataFrame) -> dict[str, set[str]]:
    """Map each Ensembl gene id to its case-folded alias set."""
    return {
        gene: {alias.casefold() for alias in aliases if isinstance(alias, str)}
        for gene, aliases in zip(
            mouse_gene_metadata_df["ensembl_gene_id"],
            mouse_gene_metadata_df["alias"],
        )
        if isinstance(aliases, list)
    }


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


def pairs_from_protein_ids(protein_ids: pd.Series) -> pd.DataFrame:
    """Build uniprotid/header_symbol pairs from gene_symbol|uniprotid values.

    Aggregate DE files carry the identifier as a protein_id column rather than as
    wide headers. A value without a pipe cannot be split into a symbol and an
    accession, so it raises rather than silently treating the whole string as one.
    """
    ids = protein_ids.astype(str)
    missing_pipe = ~ids.str.contains("|", regex=False)
    if missing_pipe.any():
        examples = ids.loc[missing_pipe].unique()[:5].tolist()
        raise ValueError(
            "protein_id values must be gene_symbol|uniprotid. "
            f"Values without a pipe (first 5): {examples}"
        )
    return pd.DataFrame(
        {
            "uniprotid": canonical_accession(ids),
            "header_symbol": ids.str.rsplit("|", n=1).str[0],
        }
    ).drop_duplicates()


def observed_gene_names(header_pairs: pd.DataFrame) -> dict[str, set[str]]:
    """Collect the case-folded gene names each accession is labeled with.

    Names are unioned per accession rather than resolved per file: two files sharing a
    model_group can head one accession differently, and resolving per file would split one
    protein's age trajectory across two unique_ids.

    Underscores are restored to hyphens because the pipeline mangles hyphenated symbols the
    same way it mangles isoform accessions (h3_3b -> h3-3b), and mangles the separator
    between several genes too, so H4c1; H4c2 arrives as h4c1;_h4c2. The leading
    underscore is stripped before the interior ones are converted, otherwise every name
    after the first would read as -h4c2 and match no gene.

    Isoform accessions contribute to their base accession, which carries the gene mapping.
    """
    names: dict[str, set[str]] = {}
    for accession, symbol in (
        header_pairs[["uniprotid", "header_symbol"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        base = accession.split("-")[0]
        for name in str(symbol).split(";"):
            name = name.strip(" _").casefold().replace("_", "-")
            if name and name != "na":
                names.setdefault(base, set()).add(name)
    return names


def resolve_gene_ids(
    header_pairs: pd.DataFrame,
    candidates: dict[str, list[str]],
    gene_symbols: dict[str, str],
    gene_aliases: dict[str, set[str]],
) -> dict[str, str]:
    """Pick one Ensembl gene per accession, preferring the gene the data file names.

    Candidates come only from the UniProt mapping file; a header symbol naming a gene that
    file does not offer for the accession cannot pull that gene in. Among the candidates,
    the header symbol decides, because Ensembl ids carry no annotation-quality signal and
    retrogenes often have lower ids than the parent gene, so the smallest id alone would
    label cytochrome c as Gm10053. Aliases catch nomenclature drift, where the file still
    says Srp54 and mouse_gene_metadata says Srp54a. The smallest id breaks what neither can.
    """
    names = observed_gene_names(header_pairs)
    resolved = {}
    for accession, genes in candidates.items():
        wanted = names.get(accession, set())
        matches = [
            gene for gene in genes if gene_symbols.get(gene, "").casefold() in wanted
        ]
        if not matches:
            matches = [gene for gene in genes if wanted & gene_aliases.get(gene, set())]
        # candidates arrive sorted, so matches[0] is the smallest matching id. Falling back
        # to min(genes) when several candidates match would pick a gene the header never
        # named, which no current accession hits but which the sort order would hide.
        resolved[accession] = matches[0] if matches else min(genes)
    return resolved


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
