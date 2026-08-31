"""
Protein Individual Expression Transform Module

Transforms individual proteomics (normalized abundance) data for Model AD into the same
nested shape as the RNA individual transform, plus the proteomics-specific fields
uniprotid, unique_id, and display_symbol.

The proteomics source files are wide (one column per protein, header gene_symbol|uniprotid)
and carry no biology metadata, so this transform melts them to long form and joins
per-animal harmonized metadata before building the output.

Required Inputs:
    - genotype_label_map: display labels and model_groups per (model, genotype)
    - mouse_gene_metadata: gene symbols for Ensembl gene ids
    - load2_harmonized_metadata: per-animal sex, ageDeath, genotype, and tissue,
      keyed on individualID
    - uniprot_ensembl_map: UniProt accession to Ensembl gene id
    - One or more wide proteomics data files, whose id columns are defined by
      DATA_FILE_REQUIRED_COLUMNS
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

# All proteomics animals belong to the single LOAD2 study. The harmonized metadata has no
# model column, so model is set to this constant to join the genotype label map. Future
# multi-study proteomics would need a study to model mapping instead of a constant.
MODEL = "LOAD2"

# Carried over from the RNA individual transform. MG-985 flagged this label as an
# assumption: the source is TMT batch-regressed normalized abundance, not log2 CPM.
# Confirm with the data team before release.
UNITS = "Log2 Counts per Million"

# Continuous ageDeath (months) is bucketed into nominal age groups. Thresholds were
# confirmed with JAX on MG-985; intervals are closed on the right, so ageDeath 6 is
# "4 months" and the real 14.2-month animals belong to "12 months". The outer edges are
# infinite, so only a missing ageDeath falls outside every bucket.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = ["4 months", "8 months", "12 months", "18 months", "24 months"]

# MG-985: JAX proteomics samples are all hemibrain and the harmonized metadata may not
# carry a tissue value at all, so missing values default instead of the value being
# hard-coded for every row. A populated non-JAX tissue passes through unchanged.
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
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol"],
    "load2_harmonized_metadata": HARMONIZED_COLUMNS,
    "uniprot_ensembl_map": ["uniprotkb_accession", "resource_identifier"],
}

# Wide proteomics files carry exactly these id columns; every other column is a protein
# feature. The gene symbol in the header is ignored -- it is absent from the 24-month file
# and gene_symbol comes from mouse_gene_metadata instead.
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


def _build_uniprot_to_ensembl(mapping_df: pd.DataFrame) -> Dict[str, str]:
    """Map each UniProt accession to a single mouse Ensembl gene id.

    The source contains a handful of human (ENSG) rows and ~100 accessions that map to
    several mouse genes; keeping the lexicographically smallest id makes the result stable
    across runs.
    """
    mouse = mapping_df[
        mapping_df["resource_identifier"].astype(str).str.startswith("ENSMUSG")
    ]
    return mouse.groupby("uniprotkb_accession")["resource_identifier"].min().to_dict()


def _melt_proteomics_file(
    data_file: pd.DataFrame, id_columns: List[str]
) -> pd.DataFrame:
    """Melt one wide proteomics file into individualid, uniprotid, value rows."""
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
    long_df["individualid"] = long_df["individualid"].astype(str)
    return long_df[["individualid", "uniprotid", "value"]]


def _normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Apply TISSUE_ALIASES case-insensitively and default missing values to DEFAULT_TISSUE."""
    normalized = tissue.astype("string").str.strip()
    aliased = normalized.str.casefold().map(TISSUE_ALIASES).fillna(normalized)
    return aliased.replace("", pd.NA).fillna(DEFAULT_TISSUE)


def _build_output(
    long_df: pd.DataFrame,
    harmonized_df: pd.DataFrame,
    uniprot_to_ensembl: Dict[str, str],
    gene_symbols: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Join metadata onto the long proteomics data, derive output fields, and nest records.

    Raises:
        ValueError: If an animal has an unbucketable ageDeath, or if no rows remain after
            filtering to mapped genes and genotypes.
    """
    # An inner join drops proteomics animals absent from the harmonized metadata. Per
    # MG-985 those are all 24-month wildtypes, which the genotype filter below would drop.
    df = long_df.merge(harmonized_df, on="individualid", how="inner")

    # Isoform accessions (Q8C8R3-2) inherit the base accession's gene mapping, but the full
    # accession stays in the output so distinct proteoforms stay distinct.
    df["ensembl_gene_id"] = (
        df["uniprotid"].str.split("-").str[0].map(uniprot_to_ensembl)
    )
    df = df.dropna(subset=["ensembl_gene_id"])

    # Rows whose (model, genotype) is absent from the label map get NA result_order after
    # the left merge. Dropping them excludes the wildtype and heterozygous animals, which
    # MG-985 confirmed should not be shown.
    df["model"] = MODEL
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

    # There is a single LOAD2 model_group, so result_order and matched_control are constant.
    result_order = determine_result_order(df)

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
    entries["name"] = MODEL
    entries["matched_control"] = result_order[0]
    entries["result_order"] = [result_order] * len(entries)

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


def transform_protein_de_individual(
    datasets: Dict[str, pd.DataFrame],
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
            no data files are provided, a data file is empty or missing id columns, an
            animal has an unbucketable ageDeath, or no rows remain after filtering to
            mapped genes and genotypes.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    genotype_label_map_df = datasets["genotype_label_map"].copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    validate_model_group_consistency(genotype_label_map_df)

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])
    uniprot_to_ensembl = _build_uniprot_to_ensembl(datasets["uniprot_ensembl_map"])

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
    logger.info(
        f"Transform protein_de_individual: processing {len(file_list)} data files: "
        f"{file_list}"
    )

    long_frames = []
    for file_name in file_list:
        data_file = datasets[file_name]
        validate_data_file_not_empty(file_name, data_file)
        check_required_datasets_and_columns(
            {file_name: data_file}, {file_name: data_file_required_columns}
        )
        long_frames.append(_melt_proteomics_file(data_file, data_file_required_columns))

    output = _build_output(
        pd.concat(long_frames, ignore_index=True),
        harmonized_df,
        uniprot_to_ensembl,
        gene_symbols,
        genotype_label_map_df,
    )

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
