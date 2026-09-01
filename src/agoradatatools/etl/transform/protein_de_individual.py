"""
Protein Individual Expression Transform Module

Transforms individual proteomics (normalized abundance) data for Model AD into the same
nested shape as the RNA individual transform, plus the proteomics-specific fields
uniprotid, unique_id, and display_symbol.

The proteomics source files are wide (one column per protein, header gene_symbol|uniprotid)
and carry no biology metadata, so this transform melts them to long form and joins
per-animal harmonized metadata before building the output.

Unlike the RNA data files, the proteomics files have no model column and their source
cannot be changed, so each data file's model is declared in the config and passed in as
model_map. Any number of models is supported: name, matched_control, and result_order are
computed per model_group rather than once for the whole run.

Multiple studies are not yet supported. The harmonized metadata is study-scoped and its
dataset key is fixed below, so a second study would arrive with its own metadata file and
need that input generalized.

Required Inputs:
    - genotype_label_map: display labels and model_groups per (model, genotype)
    - mouse_gene_metadata: gene symbols and aliases for Ensembl gene ids
    - load2_harmonized_metadata: per-animal sex, ageDeath, genotype, and tissue,
      keyed on individualID
    - uniprot_ensembl_map: UniProt accession to Ensembl gene id
    - One or more wide proteomics data files, whose id columns are defined by
      DATA_FILE_REQUIRED_COLUMNS and whose model is declared in model_map
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    extract_age_numeric,
    nest_fields,
    ColumnRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.rna_de_individual_utils import (
    create_gene_metadata_dict,
    determine_result_order,
    validate_data_file_not_empty,
    validate_model_group_consistency,
)

logger = logging.getLogger(__name__)

# Carried over from the RNA individual transform. MG-985 flagged this label as an
# assumption: the source is TMT batch-regressed normalized abundance, not log2 CPM.
# Confirm with the data team before release.
# ponytail: one units string for every model. A model reporting different units would need
# this to move into the genotype label map or the config alongside model_map.
UNITS = "Log2 Counts per Million"

# Continuous ageDeath (months) is bucketed into nominal age groups. Thresholds were
# confirmed with JAX on MG-985; intervals are closed on the right, so ageDeath 6 is
# "4 months" and the real 14.2-month animals belong to "12 months". The outer edges are
# infinite, so only a missing ageDeath falls outside every bucket.
# ponytail: these buckets encode LOAD2's 4/8/12/18/24-month design. A model on a different
# timepoint schedule would land in the wrong bucket; make the bins a model_map-style config
# parameter when one arrives.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = ["4 months", "8 months", "12 months", "18 months", "24 months"]

# MG-985: JAX proteomics samples are all hemibrain and the harmonized metadata may not
# carry a tissue value at all, so missing values default instead of the value being
# hard-coded for every row. A populated non-JAX tissue passes through unchanged.
# ponytail: unlike the other simplifications here this one fails quietly -- a non-JAX study
# with a blank tissue is labeled Hemibrain rather than raising. Make the default per-model,
# or drop it and require a populated tissue, once a non-JAX study exists.
TISSUE_ALIASES = {"right cerebral hemisphere": "Hemibrain"}
DEFAULT_TISSUE = "Hemibrain"

HARMONIZED_COLUMNS = ["individualid", "sex", "agedeath", "genotype", "tissue"]

REQUIRED_INPUT = {
    "genotype_label_map": [
        "model",
        "model_group",
        "display_label",
        "genotype",
        "result_order",
    ],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
    "load2_harmonized_metadata": HARMONIZED_COLUMNS,
    "uniprot_ensembl_map": ["uniprotkb_accession", "resource_identifier"],
}

# Wide proteomics files carry exactly these id columns; every other column is a protein
# feature named gene_symbol|uniprotid.
DATA_FILE_REQUIRED_COLUMNS = ["specimenid", "individualid"]

COLUMN_RULES: Dict[str, Dict[str, List[ColumnRule]]] = {
    "genotype_label_map": {
        "model": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "display_label": [NotEmptyRule()],
        "model_group": [NotEmptyRule()],
        "result_order": [NotEmptyRule()],
    },
    "uniprot_ensembl_map": {
        "uniprotkb_accession": [NotEmptyRule()],
        "resource_identifier": [NotEmptyRule()],
    },
    "load2_harmonized_metadata": {
        "individualid": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
    },
}


def _build_uniprot_candidates(mapping_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Map each UniProt accession to its candidate mouse Ensembl gene ids.

    Human (ENSG) rows are dropped. 105 accessions match several mouse genes, so the
    mapping alone cannot pick one; see _resolve_gene_ids.
    """
    mouse = mapping_df[
        mapping_df["resource_identifier"].astype(str).str.startswith("ENSMUSG")
    ]
    return {
        accession: sorted(genes)
        for accession, genes in mouse.groupby("uniprotkb_accession")[
            "resource_identifier"
        ]
        .unique()
        .items()
    }


def _build_gene_aliases(mouse_gene_metadata_df: pd.DataFrame) -> Dict[str, set]:
    """Map each Ensembl gene id to its case-folded alias set."""
    return {
        gene: {alias.casefold() for alias in aliases if isinstance(alias, str)}
        for gene, aliases in zip(
            mouse_gene_metadata_df["ensembl_gene_id"],
            mouse_gene_metadata_df["alias"],
        )
        if isinstance(aliases, list)
    }


def _observed_gene_names(long_df: pd.DataFrame) -> Dict[str, set]:
    """Collect the case-folded gene names each accession is labeled with in the data files.

    A header symbol may name several genes ("h4c1; h4c2"), may be the literal string NA,
    and may differ between files for one accession, so names are unioned per accession.
    Underscores are restored to hyphens because the pipeline mangles hyphenated symbols the
    same way it mangles isoform accessions (h3_3b -> h3-3b). Isoform accessions contribute
    to their base accession, which is what carries the gene mapping.
    """
    names: Dict[str, set] = {}
    for accession, symbol in (
        long_df[["uniprotid", "header_symbol"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        base = accession.split("-")[0]
        for name in str(symbol).split(";"):
            name = name.strip().casefold().replace("_", "-")
            if name and name != "na":
                names.setdefault(base, set()).add(name)
    return names


def _resolve_gene_ids(
    long_df: pd.DataFrame,
    candidates: Dict[str, List[str]],
    gene_symbols: Dict[str, str],
    gene_aliases: Dict[str, set],
) -> Dict[str, str]:
    """Pick one Ensembl gene per accession, preferring the gene the data file names.

    Ensembl ids carry no annotation-quality signal and retrogenes often have lower ids than
    the parent gene, so choosing the smallest id alone would label cytochrome c as Gm10053.
    The proteomics header symbol comes from the UniProt entry the spectra were searched
    against, so it identifies the intended gene. Aliases catch nomenclature drift, where
    the file still says Srp54 and mouse_gene_metadata says Srp54a. Accessions the header
    cannot resolve -- multi-copy families such as the histones, whose peptides genuinely
    cannot be attributed to one locus -- keep the smallest id so runs stay reproducible.
    """
    names = _observed_gene_names(long_df)
    resolved = {}
    for accession, genes in candidates.items():
        wanted = names.get(accession, set())
        matches = [
            gene for gene in genes if gene_symbols.get(gene, "").casefold() in wanted
        ]
        if not matches:
            matches = [gene for gene in genes if wanted & gene_aliases.get(gene, set())]
        resolved[accession] = matches[0] if len(matches) == 1 else min(genes)
    return resolved


def _melt_proteomics_file(
    data_file: pd.DataFrame, id_columns: List[str], model: str
) -> pd.DataFrame:
    """Melt one wide proteomics file into individualid, model, uniprotid, value rows.

    The file's model comes from the caller because the proteomics files carry no model
    column; it is set here so the returned frame needs no further copies.
    """
    long_df = data_file.melt(
        id_vars=id_columns, var_name="header", value_name="value"
    ).dropna(subset=["value"])
    # The pipeline lowercases headers and converts isoform hyphens to underscores;
    # upper-casing and restoring the hyphen recovers the canonical accession losslessly
    # (ank2|q8c8r3_2 -> Q8C8R3-2). UniProt accessions never contain an underscore.
    long_df["uniprotid"] = (
        long_df["header"]
        .str.rsplit("|", n=1)
        .str[-1]
        .str.upper()
        .str.replace("_", "-", regex=False)
    )
    # The symbol is not used as the output gene_symbol, which comes from
    # mouse_gene_metadata, only to pick between genes sharing an accession. It is empty for
    # some features of the 24-month file.
    long_df["header_symbol"] = long_df["header"].str.rsplit("|", n=1).str[0]
    long_df["individualid"] = long_df["individualid"].astype(str)
    long_df["model"] = model
    return long_df[["individualid", "model", "uniprotid", "header_symbol", "value"]]


def _normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Apply TISSUE_ALIASES case-insensitively and default missing values to DEFAULT_TISSUE."""
    normalized = tissue.astype("string").str.strip()
    aliased = normalized.str.casefold().map(TISSUE_ALIASES).fillna(normalized)
    return aliased.replace("", pd.NA).fillna(DEFAULT_TISSUE)


def _build_output(
    long_df: pd.DataFrame,
    harmonized_df: pd.DataFrame,
    uniprot_candidates: Dict[str, List[str]],
    gene_symbols: Dict[str, str],
    gene_aliases: Dict[str, set],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Join metadata onto the long proteomics data, derive output fields, and nest records.

    long_df carries a model column set from model_map, so it may hold several models. Every
    per-model field is resolved by model_group rather than assumed constant.

    Raises:
        ValueError: If an animal has an unbucketable ageDeath, or if no rows remain after
            filtering to mapped genes and genotypes.
    """
    uniprot_to_ensembl = _resolve_gene_ids(
        long_df, uniprot_candidates, gene_symbols, gene_aliases
    )

    # An inner join drops proteomics animals absent from the harmonized metadata. Per
    # MG-985 those are all 24-month wildtypes, which the genotype filter below would drop.
    df = long_df.merge(harmonized_df, on="individualid", how="inner")

    # Isoform accessions (Q8C8R3-2) inherit the base accession's gene mapping, but the full
    # accession stays in the output so distinct proteoforms stay distinct.
    df["ensembl_gene_id"] = (
        df["uniprotid"].str.split("-").str[0].map(uniprot_to_ensembl)
    )
    df = df.drop(columns=["header_symbol"]).dropna(subset=["ensembl_gene_id"])

    # Rows whose (model, genotype) is absent from the label map get NA result_order after
    # the left merge. Dropping them excludes the wildtype and heterozygous animals, which
    # MG-985 confirmed should not be shown. The model comes from model_map via the melt.
    df = df.merge(
        genotype_label_map_df,
        on=["model", "genotype"],
        how="left",
        validate="many_to_one",
    ).dropna(subset=["result_order"])

    if df.empty:
        raise ValueError(
            "No rows remained after filtering to mapped genes and genotypes — check the "
            "UniProt/Ensembl mapping and that genotypes are present in the label map."
        )

    # determine_result_order expects rows from one model_group, and result_order and
    # matched_control differ between groups, so both are resolved per group below rather
    # than once for the whole frame.
    result_orders = {
        model_group: determine_result_order(group)
        for model_group, group in df.groupby("model_group")
    }

    # Cast out of the categorical pd.cut returns: grouping on a categorical would emit an
    # entry for every unused age label.
    df["age"] = pd.cut(df["agedeath"], bins=AGE_BINS, labels=AGE_LABELS).astype(object)
    if df["age"].isna().any():
        raise ValueError(
            "Missing or unbucketable ageDeath for individualID(s): "
            f"{sorted(df.loc[df['age'].isna(), 'individualid'].unique())}"
        )

    df["tissue"] = _normalize_tissue(df["tissue"])
    df["sex"] = df["sex"].str.title()
    df["gene_symbol"] = df["ensembl_gene_id"].map(gene_symbols).fillna("")
    df["unique_id"] = df["ensembl_gene_id"] + df["uniprotid"]
    # MG-985: display_symbol falls back to the Ensembl gene id when no symbol is known.
    df["display_symbol"] = (
        df["gene_symbol"].where(df["gene_symbol"] != "", df["ensembl_gene_id"])
        + " ("
        + df["uniprotid"]
        + ")"
    )
    df["value"] = df["value"].astype(float).round(5)

    # Drop the raw genotype before renaming display_label so there is no duplicate column.
    df = df.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype", "individualid": "individual_id"}
    )

    # ensembl_gene_id, uniprotid, gene_symbol, and display_symbol are functionally
    # determined by unique_id, so grouping on them keeps them as top-level columns without
    # creating extra groups.
    group_cols = [
        "unique_id",
        "ensembl_gene_id",
        "uniprotid",
        "gene_symbol",
        "display_symbol",
        "tissue",
        "model_group",
        "age",
    ]
    data_cols = ["genotype", "sex", "individual_id", "value"]
    entries = nest_fields(
        df[group_cols + data_cols],
        grouping=group_cols,
        new_column="data",
        drop_columns=group_cols,
    )

    entries["age_numeric"] = entries["age"].apply(extract_age_numeric)
    entries["units"] = UNITS
    # name mirrors model_group, as in the RNA individual transform.
    entries["name"] = entries["model_group"]
    entries["matched_control"] = entries["model_group"].map(
        {model_group: order[0] for model_group, order in result_orders.items()}
    )
    entries["result_order"] = entries["model_group"].map(result_orders)

    output_cols = [
        "ensembl_gene_id",
        "gene_symbol",
        "uniprotid",
        "unique_id",
        "display_symbol",
        "tissue",
        "name",
        "model_group",
        "matched_control",
        "units",
        "age",
        "age_numeric",
        "result_order",
        "data",
    ]
    return (
        entries[output_cols]
        .sort_values(by=["unique_id", "age_numeric"])
        .to_dict(orient="records")
    )


def _validate_model_map(
    model_map: Dict[str, str], file_list: List[str], known_models: set
) -> None:
    """Check that every data file has a declared model and every model can be labeled.

    A model absent from the label map is caught here rather than left to the label map
    merge, which would drop every one of its rows and report the unrelated "No rows
    remained" error.

    Raises:
        ValueError: If a data file has no model_map entry, a model_map key is not one of
            the data files, or a declared model is absent from the genotype label map.
    """
    missing_files = sorted(set(file_list) - set(model_map))
    if missing_files:
        raise ValueError(
            f"No model declared for proteomics data file(s) {missing_files}. Add an entry "
            "to model_map in the config for every data file."
        )

    unknown_files = sorted(set(model_map) - set(file_list))
    if unknown_files:
        raise ValueError(
            f"model_map declares a model for {unknown_files}, which are not proteomics "
            "data files in this dataset. Check the config for a typo."
        )

    unknown_models = sorted(set(model_map.values()) - known_models)
    if unknown_models:
        raise ValueError(
            f"model_map refers to model(s) {unknown_models} that are absent from the "
            "genotype label map, so none of their rows could be labeled. Add the model to "
            "the label map or correct the config."
        )


def transform_protein_de_individual(
    datasets: Dict[str, pd.DataFrame],
    model_map: Dict[str, str],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
    data_file_required_columns: List[str] = DATA_FILE_REQUIRED_COLUMNS,
    column_rules: Dict[str, Dict[str, List[ColumnRule]]] = COLUMN_RULES,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for Model AD individual proteomics data.

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include the datasets
            listed in REQUIRED_INPUT plus one or more wide proteomics data files whose keys
            are not in required_input.
        model_map: Model name for each proteomics data file, keyed on the data file's
            dataset name, declared in the config under custom_transformations. The
            proteomics files have no model column, so this is the only source of model.
            Every data file must have an entry, every entry must name a data file, and
            every model must exist in the genotype label map.
        required_input: Required dataset names mapped to their required columns.
        data_file_required_columns: Required id columns for each wide data file.
        column_rules: Per-column content rules for the static datasets.

    Returns:
        List of dictionaries, one per (unique_id, tissue, model_group, age). Each contains
        ensembl_gene_id, gene_symbol, uniprotid, unique_id, display_symbol, tissue, name,
        model_group, matched_control, units, age, age_numeric, result_order, and a data list
        of individual records (genotype, sex, individual_id, value).

    Raises:
        ValueError: If required datasets or columns are missing, a column rule is violated,
            no data files are provided, model_map does not cover the data files exactly or
            names a model absent from the label map, a data file is empty or missing id
            columns, an animal has an unbucketable ageDeath, or no rows remain after
            filtering to mapped genes and genotypes.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    genotype_label_map_df = datasets["genotype_label_map"].copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    validate_model_group_consistency(genotype_label_map_df)

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])
    gene_aliases = _build_gene_aliases(datasets["mouse_gene_metadata"])
    uniprot_candidates = _build_uniprot_candidates(datasets["uniprot_ensembl_map"])

    # individualID repeats in the harmonized metadata for animals with more than one
    # specimen; the duplicate rows agree on sex, ageDeath, genotype, and tissue, so
    # dropping them keeps the join from fanning out.
    harmonized_df = (
        datasets["load2_harmonized_metadata"]
        .drop_duplicates(subset=["individualid"])[HARMONIZED_COLUMNS]
        .copy()
    )
    harmonized_df["individualid"] = harmonized_df["individualid"].astype(str)

    file_list = [key for key in datasets if key not in required_input]
    if not file_list:
        raise ValueError(
            "No proteomics data files provided. Provide at least one wide proteomics file "
            "whose dataset key is not one of the required metadata inputs."
        )
    _validate_model_map(model_map, file_list, set(genotype_label_map_df["model"]))
    logger.info(
        f"Transform protein_de_individual: processing {len(file_list)} data files: "
        f"{ {name: model_map[name] for name in file_list} }"
    )

    long_frames = []
    for file_name in file_list:
        data_file = datasets[file_name]
        validate_data_file_not_empty(file_name, data_file)
        check_required_datasets_and_columns(
            {file_name: data_file}, {file_name: data_file_required_columns}
        )
        # ponytail: one model per file, matching the RNA transform. The proteomics files are
        # split by study and timepoint rather than by model, so a study delivering several
        # models in one file would need model_map values widened from a string to a list.
        long_frames.append(
            _melt_proteomics_file(
                data_file, data_file_required_columns, model_map[file_name]
            )
        )

    output = _build_output(
        pd.concat(long_frames, ignore_index=True),
        harmonized_df,
        uniprot_candidates,
        gene_symbols,
        gene_aliases,
        genotype_label_map_df,
    )

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
