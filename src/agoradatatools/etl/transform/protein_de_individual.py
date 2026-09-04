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

The harmonized metadata is study-scoped, so a second study arrives as its own file rather
than as extra rows. The config lists them in harmonized_metadata and they are concatenated;
see _build_harmonized_metadata for why that is sufficient.

Required inputs are declared in REQUIRED_INPUT, plus the per-animal metadata files named in
harmonized_metadata and one or more wide proteomics data files whose model is declared in
model_map. Any dataset in none of those roles is taken to be a proteomics data file.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    nest_fields,
    normalize_zero,
    ColumnRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
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

# Continuous ageDeath (months) is bucketed into the nominal age group. Thresholds were
# confirmed with JAX on MG-985; intervals are closed on the right, so ageDeath 6 belongs to
# the 4-month group and the real 14.2-month animals belong to the 12-month group. The outer
# edges are infinite, so only a missing ageDeath falls outside every bucket.
# The label is derived from the number rather than the reverse, so the two output fields
# cannot disagree.
# ponytail: these buckets encode LOAD2's 4/8/12/18/24-month design. A model on a different
# timepoint schedule would land in the wrong bucket; make the bins a model_map-style config
# parameter when one arrives.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = [4, 8, 12, 18, 24]

# MG-985 asked whether the tissue value had to be hard-coded because the study's own
# metadata carried none. It does not: the harmonized metadata populates tissue on every row,
# so this only normalizes the JAX spelling and a populated non-JAX tissue passes through
# unchanged. A blank tissue raises rather than defaulting to Hemibrain, so a non-JAX study
# arriving without one cannot be silently mislabeled.
TISSUE_ALIASES = {"right cerebral hemisphere": "Hemibrain"}

# MG-985: syn75965714 omits 15 of the 64 24-month animals, so partial coverage is expected
# and cannot be an error. A file losing most of its animals instead means the two sources
# stopped sharing an individualID vocabulary -- most likely an upstream dtype change turning
# 51503 into "51503.0" -- which would otherwise shrink the output with no failure.
MIN_METADATA_COVERAGE = 0.5

# Required of every file named in harmonized_metadata. The metadata is study-scoped, so
# these are per-file requirements rather than REQUIRED_INPUT entries under a fixed key.
HARMONIZED_COLUMNS = ["individualid", "sex", "agedeath", "genotype", "tissue"]
HARMONIZED_COLUMN_RULES: Dict[str, List[ColumnRule]] = {
    "individualid": [NotEmptyRule()],
    "genotype": [NotEmptyRule()],
}

REQUIRED_INPUT = {
    "genotype_label_map": [
        "model",
        "model_group",
        "display_label",
        "genotype",
        "result_order",
    ],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
    # The mapping file names its Ensembl column resource_identifier; the config's
    # column_rename renames it so this transform speaks one vocabulary throughout.
    "uniprot_ensembl_map": ["uniprotkb_accession", "ensembl_gene_id"],
}

# The only column a wide proteomics file must carry. Protein columns are found by the pipe
# in their header rather than by excluding known id columns, so specimenid needs no entry
# here and a new metadata column upstream cannot become a phantom protein.
DATA_FILE_REQUIRED_COLUMNS = ["individualid"]

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
        "ensembl_gene_id": [NotEmptyRule()],
    },
}


def _build_uniprot_candidates(mapping_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Map each UniProt accession to its candidate mouse Ensembl gene ids.

    Human (ENSG) rows are dropped. 105 accessions match several mouse genes, so the
    mapping alone cannot pick one; see _resolve_gene_ids.
    """
    mouse = mapping_df[
        mapping_df["ensembl_gene_id"].astype(str).str.startswith("ENSMUSG")
    ]
    return {
        accession: sorted(genes)
        for accession, genes in mouse.groupby("uniprotkb_accession")["ensembl_gene_id"]
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
    cannot resolve keep the smallest id.

    Attaching each protein to exactly one gene was chosen over repeating identical
    measurements across every candidate or dropping the protein from the output.

    MG-985 comment 340902 answers this question and supports two readings, so both are
    recorded here. It opens with

        Use the uniprot mapping file, don't rely on gene symbols embedded in the results
        file. results.uniprot_id -> uniprot mapping file ensembl_gene_id(s) [-> pick
        lowest ensembl_gene_id if multiples] -> resolve gene_symbol for selected
        ensembl_gene_id from gene_metadata

    which describes a pipeline with no header-symbol step at all. But every one of the four
    bullets beneath it answers only the accessions that already reach the fallback, and the
    bullet covering three of them reads "go with the matching ensembl_gene_id, then pick the
    lowest ENS value if there are multiples" -- "the matching ensembl_gene_id" presupposes
    that symbols are being matched.

    Of the 60 measured accessions with more than one candidate, 53 resolve on the header
    symbol, 4 on an alias, and 3 fall back to the smallest id (P10853, Q8BR63, Q8R092).
    Comment 340898 reported 6 falling back; Ptp4a1 and H3-3a/H3-3b now resolve because ties
    among named genes stay within the named genes, and Adat3 resolves because
    ENSMUSG00000113640 has since been added to mouse_gene_metadata.

    This function implements the narrow reading: the header symbol selects among the
    candidates, and the smallest id breaks ties the header cannot. The strict reading,
    dropping the header step entirely, was measured against the current inputs and moves 19
    of 8,774 measured accessions and 2,299 data points, including Cycs to Gm10053, Uba52 to
    the retrogene Uba52rt, Eno1 to Eno1b, Rpl36a to Rpl36al and Psme2 to Psme2b. MG-985
    comment 340898 did not show that cost when the question was answered, so it has been
    raised on the ticket; if the strict reading is confirmed, this function, along with
    _build_gene_aliases and _observed_gene_names, collapses to a groupby-min over the
    mapping file.

    Candidates only ever come from the UniProt mapping file. A header symbol naming a gene
    the mapping file does not offer for that accession does not pull that gene in: P10853
    is headed H2bc15, which mouse_gene_metadata knows as ENSMUSG00000095217, but the
    mapping file pairs P10853 with three other histone genes, so the fallback picks among
    those three. Trusting the header over the mapping would attach a protein to a gene the
    mapping file says it does not come from.
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
        # candidates arrive sorted, so matches[0] is the smallest matching id. Falling back
        # to min(genes) when several candidates match would pick a gene the header never
        # named, which no current accession hits but which the sort order would hide.
        resolved[accession] = matches[0] if matches else min(genes)
    return resolved


def _melt_proteomics_file(
    file_name: str, data_file: pd.DataFrame, model: str
) -> pd.DataFrame:
    """Melt one wide proteomics file into individualid, model, uniprotid, value rows.

    Protein columns are identified by the pipe in their gene_symbol|uniprotid header. The
    alternative, treating every column that is not a known id column as a protein, silently
    turns a metadata column added upstream into a protein named after it.

    The file's model comes from the caller because the proteomics files carry no model
    column; it is set here so the returned frame needs no further copies.
    """
    protein_columns = [column for column in data_file.columns if "|" in column]
    if not protein_columns:
        raise ValueError(
            f"Proteomics data file '{file_name}' has no protein columns. Protein columns "
            "are named gene_symbol|uniprotid; columns found: "
            f"{', '.join(map(str, data_file.columns))}."
        )

    long_df = data_file.melt(
        id_vars=["individualid"],
        value_vars=protein_columns,
        var_name="header",
        value_name="value",
    )
    # Coerced here, rather than at output time, so a non-numeric cell can name its file.
    # Unmeasured proteins are already null and are dropped, not reported.
    value = pd.to_numeric(long_df["value"], errors="coerce")
    unparseable = value.isna() & long_df["value"].notna()
    if unparseable.any():
        raise ValueError(
            f"Non-numeric abundance values in proteomics data file '{file_name}': "
            f"{long_df.loc[unparseable, 'value'].unique()[:5].tolist()}"
        )
    long_df["value"] = value
    long_df = long_df.dropna(subset=["value"])
    if long_df.empty:
        raise ValueError(
            f"Every abundance value in proteomics data file '{file_name}' is missing, so "
            f"it contributes nothing to the output."
        )

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
    # Required, not cosmetic: individualID arrives as int64 from one source file and as
    # object from the other, and the harmonized metadata is cast to match. Without this the
    # merge on individualid would silently match nothing for one of the files.
    long_df["individualid"] = long_df["individualid"].astype(str)
    long_df["model"] = model
    return long_df[["individualid", "model", "uniprotid", "header_symbol", "value"]]


def _check_metadata_coverage(
    file_name: str, individuals: pd.Series, known_individuals: set
) -> None:
    """Log how many of a file's animals have harmonized metadata; raise if most do not."""
    unique = set(individuals.unique())
    matched = unique & known_individuals
    coverage = len(matched) / len(unique)
    logger.info(
        f"Transform protein_de_individual: {file_name}: {len(matched)}/{len(unique)} "
        f"animals have harmonized metadata ({coverage:.0%})"
    )
    if coverage < MIN_METADATA_COVERAGE:
        raise ValueError(
            f"Only {len(matched)} of {len(unique)} animals in proteomics data file "
            f"'{file_name}' were found in the harmonized metadata, below the "
            f"{MIN_METADATA_COVERAGE:.0%} expected. The individualID values in the two "
            f"sources are probably no longer comparable. Unmatched (first 10): "
            f"{sorted(unique - matched)[:10]}"
        )


def _normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Apply TISSUE_ALIASES case-insensitively, leaving any other tissue unchanged."""
    normalized = tissue.astype("string").str.strip()
    return normalized.str.casefold().map(TISSUE_ALIASES).fillna(normalized)


def _log_stage(stage: str, df: pd.DataFrame) -> None:
    """Record how much data survived a filtering stage.

    Rows are dropped at three points in _build_output and only the all-or-nothing case
    raises, so a partial failure -- one source file whose join key stopped matching -- would
    otherwise shrink the output with nothing in the log to show it.
    """
    logger.info(
        f"Transform protein_de_individual: {stage}: {len(df)} measurements, "
        f"{df['individualid'].nunique()} animals"
    )


def _build_output(
    long_df: pd.DataFrame,
    harmonized_df: pd.DataFrame,
    uniprot_to_ensembl: Dict[str, str],
    gene_symbols: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Join metadata onto the long proteomics data, derive output fields, and nest records.

    long_df carries a model column set from model_map, so it may hold several models. Every
    per-model field is resolved by model_group rather than assumed constant.

    Raises:
        ValueError: If an animal has an unbucketable ageDeath or no tissue, or if no rows
            remain after filtering to mapped genes and genotypes.
    """
    _log_stage("melted", long_df)

    # An inner join drops proteomics animals absent from the harmonized metadata. Per
    # MG-985 those are all 24-month wildtypes, which the genotype filter below would drop.
    # validate rejects a harmonized metadata that disagrees with itself about an animal;
    # the caller de-duplicates whole rows, so only a genuine conflict reaches this.
    df = long_df.merge(
        harmonized_df, on="individualid", how="inner", validate="many_to_one"
    )
    _log_stage("after harmonized metadata join", df)

    # Isoform accessions (Q8C8R3-2) inherit the base accession's gene mapping, but the full
    # accession stays in the output so distinct proteoforms stay distinct.
    df["ensembl_gene_id"] = (
        df["uniprotid"].str.split("-").str[0].map(uniprot_to_ensembl)
    )
    df = df.drop(columns=["header_symbol"]).dropna(subset=["ensembl_gene_id"])
    _log_stage("after gene mapping", df)

    # Rows whose (model, genotype) is absent from the label map get NA result_order after
    # the left merge. Dropping them excludes the wildtype and heterozygous animals, which
    # MG-985 confirmed should not be shown. The model comes from model_map via the melt.
    df = df.merge(
        genotype_label_map_df,
        on=["model", "genotype"],
        how="left",
        validate="many_to_one",
    ).dropna(subset=["result_order"])
    _log_stage("after genotype labeling", df)

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

    # age is a nest_fields grouping key and groupby drops null keys, so an unbucketable
    # ageDeath would delete those animals with no error.
    age_numeric = pd.cut(df["agedeath"], bins=AGE_BINS, labels=AGE_LABELS)
    if age_numeric.isna().any():
        raise ValueError(
            "Missing or unbucketable ageDeath for individualID(s): "
            f"{sorted(df.loc[age_numeric.isna(), 'individualid'].unique())}"
        )
    # Cast out of the categorical pd.cut returns: grouping on a categorical would emit an
    # entry for every unused age label. int, not Int64, so the value serializes as a plain
    # JSON number.
    df["age_numeric"] = age_numeric.astype(int)
    df["age"] = df["age_numeric"].astype(str) + " months"

    df["tissue"] = _normalize_tissue(df["tissue"])
    missing_tissue = df["tissue"].isna() | (df["tissue"] == "")
    if missing_tissue.any():
        raise ValueError(
            "Missing tissue for individualID(s): "
            f"{sorted(df.loc[missing_tissue, 'individualid'].unique())}"
        )

    df["sex"] = remap_sex_labels(df["sex"].astype("string").str.title())
    df["gene_symbol"] = df["ensembl_gene_id"].map(gene_symbols).fillna("")
    df["unique_id"] = df["ensembl_gene_id"] + df["uniprotid"]
    # MG-985: display_symbol falls back to the Ensembl gene id when no symbol is known.
    df["display_symbol"] = (
        df["gene_symbol"].where(df["gene_symbol"] != "", df["ensembl_gene_id"])
        + " ("
        + df["uniprotid"]
        + ")"
    )
    # normalize_zero because the abundances are centred on zero, so small negatives round to
    # -0.0 and json.dumps keeps the sign.
    df["value"] = df["value"].round(5).apply(normalize_zero)

    # Drop the raw genotype before renaming display_label so there is no duplicate column.
    df = df.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype", "individualid": "individual_id"}
    )

    # ensembl_gene_id, uniprotid, gene_symbol and display_symbol are functionally determined
    # by unique_id, and age_numeric by age, so grouping on them keeps them as top-level
    # columns without creating extra groups.
    group_cols = [
        "unique_id",
        "ensembl_gene_id",
        "uniprotid",
        "gene_symbol",
        "display_symbol",
        "tissue",
        "model_group",
        "age",
        "age_numeric",
    ]
    data_cols = ["genotype", "sex", "individual_id", "value"]
    entries = nest_fields(
        df[group_cols + data_cols],
        grouping=group_cols,
        new_column="data",
        drop_columns=group_cols,
    )

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


def _build_harmonized_metadata(
    datasets: Dict[str, pd.DataFrame], harmonized_metadata: List[str]
) -> pd.DataFrame:
    """Combine the declared per-animal metadata files into one frame keyed on individualid.

    The metadata is study-scoped, so a second study arrives as its own file rather than as
    extra rows. Concatenating them is enough because individualID is a study-independent
    Synapse identifier: today's Model AD studies number their animals in non-overlapping
    ranges (UCI 298-11428, JAX 32043-111090), and nothing here assumes which file an animal
    came from. If two studies ever do issue the same individualID for different animals, the
    validate on the join in _build_output rejects it rather than picking one silently.
    """
    combined = pd.concat(
        [datasets[name][HARMONIZED_COLUMNS] for name in harmonized_metadata],
        ignore_index=True,
    )
    # Cast before de-duplicating, not after: two metadata files can type individualid
    # differently, and 51503 and "51503" are one animal but two rows.
    combined["individualid"] = combined["individualid"].astype(str)
    return combined.drop_duplicates()


def _validate_harmonized_metadata(
    harmonized_metadata: Optional[List[str]], dataset_names: set
) -> None:
    """Check that the declared per-animal metadata files exist.

    A file left out of the declaration is treated as a proteomics data file, so it would
    otherwise surface as the unrelated "No model declared" error.
    """
    if not harmonized_metadata:
        raise ValueError(
            "No harmonized_metadata provided. List the per-animal metadata dataset(s) "
            "under custom_transformations in the config:\n"
            "  custom_transformations:\n"
            "    transform_protein_de_individual:\n"
            "      harmonized_metadata:\n"
            "        - load2_harmonized_metadata"
        )

    unknown = sorted(set(harmonized_metadata) - dataset_names)
    if unknown:
        raise ValueError(
            f"harmonized_metadata names dataset(s) {unknown} that are not files in this "
            "dataset. Check the config for a typo."
        )


def _validate_model_map(
    model_map: Optional[Dict[str, str]], file_list: List[str], known_models: set
) -> None:
    """Check that every data file has a declared model and every model can be labeled.

    A model absent from the label map is caught here rather than left to the label map
    merge, which would drop every one of its rows and report the unrelated "No rows
    remained" error.
    """
    if not model_map:
        raise ValueError(
            "No model_map provided. Declare one under custom_transformations in the "
            "config, mapping each proteomics data file's dataset name to its model:\n"
            "  custom_transformations:\n"
            "    transform_protein_de_individual:\n"
            "      model_map:\n"
            "        jax_load2_proteomics: LOAD2"
        )

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
    model_map: Optional[Dict[str, str]] = None,
    harmonized_metadata: Optional[List[str]] = None,
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
            every model must exist in the genotype label map. Defaulted so that a config
            that omits it gets an actionable error rather than a TypeError.
        harmonized_metadata: Dataset names of the per-animal metadata files, declared in the
            config alongside model_map. The metadata is study-scoped, so a second study
            arrives as an additional file; they are concatenated and joined on individualID.
            Any dataset that is neither listed here nor in required_input is taken to be a
            proteomics data file.
        required_input: Required dataset names mapped to their required columns.
        data_file_required_columns: Required columns for each wide data file.
        column_rules: Per-column content rules for the static datasets.

    Returns:
        List of dictionaries, one per (unique_id, tissue, model_group, age), with the fields
        listed in _build_output's output_cols.

    Raises:
        ValueError: If any input is missing, empty, violates a column rule, or is
            unjoinable; see the individual validators for the specific conditions.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    _validate_harmonized_metadata(harmonized_metadata, set(datasets))
    check_required_datasets_and_columns(
        datasets, {name: HARMONIZED_COLUMNS for name in harmonized_metadata}
    )
    check_column_rules(
        datasets, {name: HARMONIZED_COLUMN_RULES for name in harmonized_metadata}
    )

    genotype_label_map_df = datasets["genotype_label_map"].copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    validate_model_group_consistency(genotype_label_map_df)

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])

    # individualID repeats in the harmonized metadata for animals with more than one
    # specimen. De-duplicating whole rows rather than the key means an animal whose rows
    # genuinely disagree survives as two rows, and the validate on the join in _build_output
    # rejects it; de-duplicating on individualid alone would keep whichever row came first.
    harmonized_df = _build_harmonized_metadata(datasets, harmonized_metadata)
    known_individuals = set(harmonized_df["individualid"])

    metadata_names = set(required_input) | set(harmonized_metadata)
    file_list = [key for key in datasets if key not in metadata_names]
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

    for file_name in file_list:
        validate_data_file_not_empty(file_name, datasets[file_name])
    check_required_datasets_and_columns(
        {name: datasets[name] for name in file_list},
        {name: data_file_required_columns for name in file_list},
    )

    long_frames = []
    for file_name in file_list:
        # ponytail: one model per file, matching the RNA transform. The proteomics files are
        # split by study and timepoint rather than by model, so a study delivering several
        # models in one file would need model_map values widened from a string to a list.
        long_df = _melt_proteomics_file(
            file_name, datasets[file_name], model_map[file_name]
        )
        _check_metadata_coverage(file_name, long_df["individualid"], known_individuals)
        long_frames.append(long_df)

    long_df = pd.concat(long_frames, ignore_index=True)
    uniprot_to_ensembl = _resolve_gene_ids(
        long_df,
        _build_uniprot_candidates(datasets["uniprot_ensembl_map"]),
        gene_symbols,
        _build_gene_aliases(datasets["mouse_gene_metadata"]),
    )

    output = _build_output(
        long_df,
        harmonized_df,
        uniprot_to_ensembl,
        gene_symbols,
        genotype_label_map_df,
    )

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
