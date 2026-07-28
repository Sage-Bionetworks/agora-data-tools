"""
Protein Individual Expression Transform Module

This module transforms individual proteomics (normalized abundance) data for Model AD.
Unlike the RNA individual transform, the proteomics source files are WIDE (one column per
protein, named gene_symbol|uniprotid) and carry no biology metadata, so this transform
first reshapes them and joins per-animal harmonized metadata before producing an
RNA-style output object plus three proteomics-specific fields: uniprotid, unique_id, and
display_symbol.

The transformation:
- Validates required datasets/columns and static column values up-front
- Melts each wide proteomics file to long form (one row per animal per protein) using the
  reusable melt_wide_dataframe helper, dropping unmeasured (null) values
- Recovers the canonical UniProt accession from the column header (the pipeline lowercases
  headers and turns isoform hyphens into underscores; upper-casing and restoring the hyphen
  reverses this losslessly)
- Joins harmonized per-animal metadata (sex, ageDeath, genotype, tissue) on individualID
- Maps each UniProt base accession to an Ensembl gene id, keeping mouse genes (ENSMUSG*)
- Maps Ensembl id to gene symbol from mouse_gene_metadata
- Assigns model LOAD2 and merges the genotype label map on (model, genotype); rows whose
  genotype is absent from the label map are dropped, which excludes the wildtype and
  heterozygous animals
- Buckets continuous ageDeath into nominal age groups via the reusable bucket_age helper
- Maps tissue "right cerebral hemisphere" to "Hemibrain" (defaulting to Hemibrain when
  missing) and title-cases sex
- Builds unique_id (ensembl_gene_id + uniprotid) and display_symbol (gene_symbol
  (uniprotid)), preserving the UniProt isoform so distinct proteoforms stay distinct
- Creates one output entry per (unique_id, tissue, model_group, age), nesting all individual
  records for that combination into a "data" list

Key Functions:
    transform_protein_de_individual: Main transformation function that orchestrates processing

Required Inputs:
    - genotype_label_map: Maps models and genotypes to display labels and model_groups
    - mouse_gene_metadata: Gene symbols for Ensembl IDs
    - load2_harmonized_metadata: Per-animal metadata keyed on individualID
    - uniprot_ensembl_map: UniProt accession to Ensembl gene id mapping
    - Data files: One or more WIDE proteomics files; required id columns are defined by the
      DATA_FILE_REQUIRED_COLUMNS module constant, and every remaining column is treated as a
      protein feature named gene_symbol|uniprotid
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    bucket_age,
    check_column_rules,
    check_required_datasets_and_columns,
    extract_age_numeric,
    melt_wide_dataframe,
    nest_fields,
    ColumnRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.rna_de_individual_utils import (
    create_gene_metadata_dict,
    filter_to_mouse_genes,
    validate_data_file_not_empty,
    validate_model_group_consistency,
)
from agoradatatools.etl.transform.rna_de_individual import _determine_result_order

logger = logging.getLogger(__name__)

# All proteomics animals belong to the single LOAD2 study. The harmonized metadata has no
# model column, so model is set to this constant to join the genotype label map. Future
# multi-study proteomics would need a study to model mapping instead of a constant.
MODEL = "LOAD2"

UNITS = "Log2 Counts per Million"

# Continuous ageDeath (months) is bucketed into nominal age groups. Boundaries are closed on
# the right (value <= edge), confirmed with the data team. Kept here so they are easy to fix.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = ["4 months", "8 months", "12 months", "18 months", "24 months"]

# Wide proteomics files have exactly these id columns; every other column is a protein
# feature whose header is gene_symbol|uniprotid.
WIDE_ID_VARS = ["specimenid", "individualid"]
WIDE_HEADER_NAMES = ["gene_symbol_header", "uniprotid"]

REQUIRED_INPUT = {
    "genotype_label_map": [
        "model",
        "model_group",
        "display_label",
        "genotype",
        "result_order",
    ],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol"],
    "load2_harmonized_metadata": [
        "individualid",
        "sex",
        "agedeath",
        "genotype",
        "tissue",
    ],
    "uniprot_ensembl_map": ["uniprotkb_accession", "resource_identifier"],
}

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
    """Build a UniProt accession to mouse Ensembl gene id lookup.

    The mapping source contains a handful of human (ENSG*) rows and 100+ UniProt
    accessions that map to more than one Ensembl gene. Mouse genes are selected first, then
    accessions with multiple mouse genes are de-duplicated deterministically by keeping the
    lexicographically smallest Ensembl id so the result is stable across runs.

    Args:
        mapping_df: DataFrame with columns uniprotkb_accession and resource_identifier.

    Returns:
        Dictionary mapping UniProt accession to a single mouse Ensembl gene id.
    """
    mouse = mapping_df[
        mapping_df["resource_identifier"].astype(str).str.startswith("ENSMUSG")
    ].copy()
    mouse = mouse.sort_values("resource_identifier").drop_duplicates(
        subset=["uniprotkb_accession"], keep="first"
    )
    return mouse.set_index("uniprotkb_accession")["resource_identifier"].to_dict()


def _prepare_harmonized_metadata(harmonized_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare the harmonized per-animal metadata for joining on individualID.

    individualID is not unique in the source (a few animals have two rows whose
    sex/age/genotype/tissue agree), so rows are de-duplicated on individualID before the
    join to avoid fan-out.

    Args:
        harmonized_df: Harmonized metadata DataFrame.

    Returns:
        DataFrame with one row per individualID and columns individualid, sex, agedeath,
        genotype, tissue.
    """
    harmonized = harmonized_df.copy()
    harmonized["individualid"] = harmonized["individualid"].astype(str)
    harmonized = harmonized.drop_duplicates(subset=["individualid"], keep="first")
    return harmonized[["individualid", "sex", "agedeath", "genotype", "tissue"]]


def _melt_proteomics_file(data_file: pd.DataFrame) -> pd.DataFrame:
    """Melt one wide proteomics file to long form and recover canonical UniProt ids.

    Args:
        data_file: Wide proteomics DataFrame with id columns plus one column per protein.

    Returns:
        Long DataFrame with columns individualid, uniprotid, value (unmeasured values
        dropped).
    """
    long_df = melt_wide_dataframe(
        data_file,
        id_vars=WIDE_ID_VARS,
        header_names=WIDE_HEADER_NAMES,
        value_name="value",
        sep="|",
        dropna=True,
    )
    long_df["individualid"] = long_df["individualid"].astype(str)
    # The pipeline lowercases headers and converts isoform hyphens to underscores; upper-case
    # and restore the hyphen to recover the canonical accession (e.g. q8c8r3_2 -> Q8C8R3-2).
    long_df["uniprotid"] = (
        long_df["uniprotid"].str.upper().str.replace("_", "-", regex=False)
    )
    return long_df[["individualid", "uniprotid", "value"]]


def _normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Map the JAX tissue name to Hemibrain and default missing values to Hemibrain.

    Args:
        tissue: Series of tissue strings from the harmonized metadata.

    Returns:
        Series with "right cerebral hemisphere" (any case) mapped to "Hemibrain" and
        null/empty values defaulted to "Hemibrain".
    """
    mapped = tissue.str.replace(
        r"(?i)^\s*right cerebral hemisphere\s*$", "Hemibrain", regex=True
    )
    mapped = mapped.fillna("Hemibrain")
    return mapped.replace("", "Hemibrain")


def _build_output(
    combined_long: pd.DataFrame,
    harmonized_df: pd.DataFrame,
    uniprot_to_ensembl: Dict[str, str],
    gene_metadata_dict: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Join metadata, derive fields, and nest individual records into output entries.

    Args:
        combined_long: Long proteomics data with columns individualid, uniprotid, value.
        harmonized_df: De-duplicated harmonized metadata keyed on individualid.
        uniprot_to_ensembl: UniProt accession to mouse Ensembl gene id lookup.
        gene_metadata_dict: Ensembl gene id to gene symbol lookup.
        genotype_label_map_df: Genotype label map with result_order cast to int.

    Returns:
        List of output entry dictionaries, one per (unique_id, tissue, model_group, age).

    Raises:
        ValueError: If no rows remain after filtering to mapped genes and genotypes.
    """
    # Attach per-animal metadata. An inner join drops proteomics animals absent from the
    # harmonized metadata (the missing 24-month animals, which are all wildtype and would be
    # dropped by the genotype filter anyway).
    df = combined_long.merge(harmonized_df, on="individualid", how="inner")

    # Map UniProt accession to Ensembl gene id. Isoform accessions (e.g. Q8C8R3-2) share the
    # base accession's mapping, so map on the base while keeping the full accession for the
    # output uniprotid so distinct proteoforms stay distinct.
    df["uniprot_base"] = df["uniprotid"].str.split("-").str[0]
    df["ensembl_gene_id"] = df["uniprot_base"].map(uniprot_to_ensembl)
    df = df.dropna(subset=["ensembl_gene_id"])
    df = filter_to_mouse_genes(df)

    # Assign the study's single model and enrich with genotype display labels. Rows whose
    # (model, genotype) is absent from the label map get NA result_order and are dropped,
    # which excludes the wildtype and heterozygous animals.
    df["model"] = MODEL
    df = df.merge(
        genotype_label_map_df,
        on=["model", "genotype"],
        how="left",
        validate="many_to_one",
    )
    df = df.dropna(subset=["result_order"])

    if df.empty:
        raise ValueError(
            "No rows remained after filtering to mapped genes and genotypes — check the "
            "UniProt/Ensembl mapping and that genotypes are present in the label map."
        )

    # result_order and matched_control are constant across the single LOAD2 model_group.
    result_order_list = _determine_result_order(df)
    matched_control = result_order_list[0] if result_order_list else ""

    # Derive output fields.
    df["age"] = bucket_age(df["agedeath"], AGE_BINS, AGE_LABELS)
    df["tissue"] = _normalize_tissue(df["tissue"])
    df["sex"] = df["sex"].str.title()
    df["gene_symbol"] = df["ensembl_gene_id"].map(gene_metadata_dict).fillna("")
    df["unique_id"] = df["ensembl_gene_id"] + df["uniprotid"]
    df["display_symbol"] = (
        df["gene_symbol"] + " (" + df["uniprotid"] + ")"
    ).str.strip()
    df["value"] = df["value"].astype(float).round(5)
    df["individual_id"] = df["individualid"].astype(str)

    # Drop the raw genotype before renaming display_label so there is no duplicate column.
    df = df.drop(columns=["genotype"]).rename(columns={"display_label": "genotype"})

    # Nest individual records by (unique_id, tissue, model_group, age). ensembl_gene_id,
    # uniprotid, gene_symbol, and display_symbol are functionally determined by unique_id, so
    # including them in the grouping keeps them as top-level columns without creating extra
    # groups.
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
    entries["matched_control"] = matched_control
    # All rows share the same result_order list; the multiplication creates references to a
    # single list object, which is safe because it is only read afterwards.
    entries["result_order"] = [result_order_list] * len(entries)

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

    Processing Steps:
        1. Validates required datasets and columns (check_required_datasets_and_columns)
        2. Validates static column values (check_column_rules on COLUMN_RULES)
        3. Prepares metadata (label map result_order to int; gene symbol lookup; UniProt to
           Ensembl lookup; de-duplicated harmonized metadata)
        4. Melts every wide proteomics data file to long form and concatenates them
        5. Joins harmonized metadata, maps proteins to mouse Ensembl genes, assigns the LOAD2
           model, applies the genotype label map (dropping wildtype/heterozygous animals),
           buckets age, and builds the proteomics-specific fields
        6. Nests individual records into one entry per (unique_id, tissue, model_group, age)

    Args:
        datasets: Dictionary mapping dataset names to DataFrames. Must include the datasets
            listed in REQUIRED_INPUT plus one or more wide proteomics data files whose keys
            are not in required_input.
        required_input: Dictionary mapping required dataset names to their required columns.
            Defaults to REQUIRED_INPUT.
        data_file_required_columns: Required id columns for each wide data file. Defaults to
            DATA_FILE_REQUIRED_COLUMNS.
        column_rules: Per-column content rules for static datasets. Defaults to COLUMN_RULES.

    Returns:
        List of dictionaries, each representing a unique combination of unique_id, tissue,
        model_group, and age. Each entry contains ensembl_gene_id, gene_symbol, uniprotid,
        unique_id, display_symbol, tissue, name, model_group, matched_control, units, age,
        age_numeric, result_order, and a data list of individual records (genotype, sex,
        individual_id, value).

    Raises:
        ValueError: If required datasets or columns are missing, a static column rule is
            violated, no data files are provided, a data file is empty or missing id columns,
            or no rows remain after filtering to mapped genes and genotypes.
    """
    # Step 1-2: Validate inputs
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    # Step 3: Prepare metadata
    genotype_label_map_df = datasets["genotype_label_map"].copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    validate_model_group_consistency(genotype_label_map_df)

    gene_metadata_dict = create_gene_metadata_dict(datasets["mouse_gene_metadata"])
    uniprot_to_ensembl = _build_uniprot_to_ensembl(datasets["uniprot_ensembl_map"])
    harmonized_df = _prepare_harmonized_metadata(datasets["load2_harmonized_metadata"])

    # Step 4: Melt every wide proteomics data file and concatenate
    file_list = [k for k in datasets.keys() if k not in required_input]
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
        long_frames.append(_melt_proteomics_file(data_file))

    combined_long = (
        pd.concat(long_frames, ignore_index=True)
        if len(long_frames) > 1
        else long_frames[0]
    )

    # Step 5-6: Join metadata, derive fields, and nest
    output = _build_output(
        combined_long,
        harmonized_df,
        uniprot_to_ensembl,
        gene_metadata_dict,
        genotype_label_map_df,
    )

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
