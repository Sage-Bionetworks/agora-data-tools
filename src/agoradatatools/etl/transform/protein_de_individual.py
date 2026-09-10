"""
Protein Individual Expression Transform Module

Transforms individual proteomics (normalized abundance) data for Model AD into the RNA
individual transform's nested shape, plus uniprotid, unique_id, and display_symbol.

The source files are wide (one column per protein, header gene_symbol|uniprotid), carry no
biology metadata, and have no model column, so each file's model is declared in the config
as model_map. Inputs whose name is neither a required dataset nor a model_map key are taken
to be per-animal metadata. That metadata is study-scoped, so a second study arrives as its
own file rather than as extra rows.
"""

import gc
import logging
from collections import defaultdict
from typing import Any, Dict, List

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
    build_model_to_model_group,
    create_gene_metadata_dict,
    determine_result_order,
    label_genotypes,
    normalize_tissue,
    prepare_genotype_label_map,
    validate_data_file_not_empty,
    GENOTYPE_LABEL_MAP_COLUMNS,
    GENOTYPE_LABEL_MAP_RULES,
)

logger = logging.getLogger(__name__)

UNITS = "Log2 Counts per Million"

AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = [4, 8, 12, 18, 24]

# MG-985: syn75965714 omits 15 of the 64 24-month animals, so partial coverage is expected
# and cannot be an error. A file losing most of its animals instead means the two sources
# stopped sharing an individualID vocabulary.
MIN_METADATA_COVERAGE = 0.5


REQUIRED_INPUT = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_COLUMNS,
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
    "uniprot_ensembl_map": ["uniprotkb_accession", "ensembl_gene_id"],
}

COLUMN_RULES = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_RULES,
    "uniprot_ensembl_map": {
        "uniprotkb_accession": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule()],
    },
}

MODEL_METADATA_REQUIRED_COLUMNS = [
    "individualid",
    "sex",
    "agedeath",
    "genotype",
    "tissue",
]
MODEL_METADATA_COLUMN_RULES = {
    "individualid": [NotEmptyRule()],
    "genotype": [NotEmptyRule()],
}

DATAFILE_REQUIRED_COLUMNS = ["individualid"]
DATAFILE_COLUMN_RULES = {
    "individualid": [NotEmptyRule()],
}


def _build_uniprot_candidates(mapping_df: pd.DataFrame) -> Dict[str, List[str]]:
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


def _canonical_accession(headers: pd.Series) -> pd.Series:
    """Recover the canonical UniProt accession from gene_symbol|uniprotid headers.

    The pipeline lowercases headers and converts isoform hyphens to underscores;
    upper-casing and restoring the hyphen recovers the accession losslessly
    (ank2|q8c8r3_2 -> Q8C8R3-2). UniProt accessions never contain an underscore.
    """
    return (
        headers.str.rsplit("|", n=1)
        .str[-1]
        .str.upper()
        .str.replace("_", "-", regex=False)
    )


def _measured_header_pairs(
    datasets: Dict[str, pd.DataFrame], datafile_list: List[str]
) -> pd.DataFrame:
    """Collect the accession and header symbol of every protein column that holds data.

    Gene resolution has to see all the data files at once, because one accession may be
    headed with different symbols in different files and _observed_gene_names unions them.
    Taking the pairs from the column headers rather than from melted rows keeps that global
    step off the measurements, which is what lets the melt run one model_group at a time.
    """
    headers = pd.Series(
        [
            column
            for file_name in datafile_list
            for column in datasets[file_name].columns
            if "|" in column and datasets[file_name][column].notna().any()
        ],
        dtype="object",
    ).drop_duplicates()
    return pd.DataFrame(
        {
            "uniprotid": _canonical_accession(headers),
            "header_symbol": headers.str.rsplit("|", n=1).str[0],
        }
    )


def _observed_gene_names(header_pairs: pd.DataFrame) -> Dict[str, set]:
    """Collect the case-folded gene names each accession is labeled with in the data files.

    Names are unioned per accession rather than resolved per file: two files sharing a
    model_group can head one accession differently, and resolving per file would split one
    protein's age trajectory across two unique_ids.

    Underscores are restored to hyphens because the pipeline mangles hyphenated symbols the
    same way it mangles isoform accessions (h3_3b -> h3-3b), and mangles the separator
    between several genes too, so "H4c1; H4c2" arrives as "h4c1;_h4c2". The leading
    underscore is stripped before the interior ones are converted, otherwise every name
    after the first would read as "-h4c2" and match no gene.

    Isoform accessions contribute to their base accession, which carries the gene mapping.
    """
    names: Dict[str, set] = {}
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


def _resolve_gene_ids(
    header_pairs: pd.DataFrame,
    candidates: Dict[str, List[str]],
    gene_symbols: Dict[str, str],
    gene_aliases: Dict[str, set],
) -> Dict[str, str]:
    """Pick one Ensembl gene per accession, preferring the gene the data file names.

    Candidates come only from the UniProt mapping file; a header symbol naming a gene that
    file does not offer for the accession cannot pull that gene in. Among the candidates,
    the header symbol decides, because Ensembl ids carry no annotation-quality signal and
    retrogenes often have lower ids than the parent gene, so the smallest id alone would
    label cytochrome c as Gm10053. Aliases catch nomenclature drift, where the file still
    says Srp54 and mouse_gene_metadata says Srp54a. The smallest id breaks what neither can.

    Attaching each protein to exactly one gene was chosen over repeating identical
    measurements across every candidate or dropping the protein from the output.

    MG-985 comment 340902 supports two readings of this and the narrow one is implemented
    here. PR #370 records the disagreement and the measured cost of the strict reading,
    under which this function, _build_gene_aliases and _observed_gene_names all collapse to
    a groupby-min over the mapping file.
    """
    names = _observed_gene_names(header_pairs)
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

    long_df["uniprotid"] = _canonical_accession(long_df["header"])
    # Required, not cosmetic: individualID arrives as int64 from one source file and as
    # object from the other, and the harmonized metadata is cast to match. Without this the
    # merge on individualid would silently match nothing for one of the files.
    long_df["individualid"] = long_df["individualid"].astype(str)
    long_df["model"] = model
    return long_df[["individualid", "model", "uniprotid", "value"]]


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


def _log_stage(model_group: str, stage: str, df: pd.DataFrame) -> None:
    logger.info(
        f"Transform protein_de_individual: {model_group}: {stage}: {len(df)} measurements, "
        f"{df['individualid'].nunique()} animals"
    )


def _build_output(
    model_group: str,
    long_df: pd.DataFrame,
    harmonized_model_metadata_df: pd.DataFrame,
    uniprot_to_ensembl: Dict[str, str],
    gene_symbols: Dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Join metadata onto the long proteomics data, derive output fields, and nest records.

    Called once per model_group, so result_order and matched_control are constant across
    the frame and are resolved as scalars. long_df may still hold several models when a
    model_group covers more than one, which is why name comes from the group rather than
    from the model column.
    """
    # Rows are dropped at three points below and only the all-or-nothing case raises, so a
    # partial failure -- one source file whose join key stopped matching -- would otherwise
    # shrink the output with nothing in the log to show it.
    _log_stage(model_group, "melted", long_df)

    # An inner join drops proteomics animals absent from the harmonized metadata. Per
    # MG-985 those are all 24-month wildtypes, which the genotype filter below would drop.
    # validate rejects a harmonized metadata that disagrees with itself about an animal;
    # the caller de-duplicates whole rows, so only a genuine conflict reaches this.
    df = long_df.merge(
        harmonized_model_metadata_df,
        on="individualid",
        how="inner",
        validate="many_to_one",
    )
    _log_stage(model_group, "after harmonized metadata join", df)

    # Isoform accessions (Q8C8R3-2) inherit the base accession's gene mapping, but the full
    # accession stays in the output so distinct proteoforms stay distinct.
    df["ensembl_gene_id"] = (
        df["uniprotid"].str.split("-").str[0].map(uniprot_to_ensembl)
    )
    df = df.dropna(subset=["ensembl_gene_id"])
    _log_stage(model_group, "after gene mapping", df)
    if df.empty:
        raise ValueError(
            f"No rows remained for model_group '{model_group}' after mapping proteins to "
            "genes — check the UniProt to Ensembl mapping file."
        )

    # MG-985 confirmed the wildtype and heterozygous animals dropped here should not be
    # shown. The model this labels on comes from model_map via the melt.
    df = label_genotypes(df, genotype_label_map_df, f"model_group '{model_group}'")
    _log_stage(model_group, "after genotype labeling", df)

    # determine_result_order expects rows from one model_group, which is what this frame is.
    result_order = determine_result_order(df)

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

    df["tissue"] = normalize_tissue(df["tissue"])
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
    entries["matched_control"] = result_order[0]
    # Every row shares one list object. Safe because nothing mutates it after this point;
    # to_dict and json.dump only read it.
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
    # Unsorted: the caller sorts the accumulated output once, so ordering does not depend
    # on how the data files were split across model_groups.
    return entries[output_cols].to_dict(orient="records")


def transform_protein_de_individual(
    datasets: Dict[str, pd.DataFrame],
    model_map: Dict[str, str],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
    column_rules: Dict[str, Dict[str, List[ColumnRule]]] = COLUMN_RULES,
) -> List[Dict[str, Any]]:
    """
    Main transformation function for Model AD individual proteomics data.

    Args:
        datasets: The datasets in REQUIRED_INPUT, one or more wide proteomics data files
            named in model_map, and one or more per-animal metadata files.
        model_map: Model name per proteomics data file, keyed on dataset name and declared
            in the config under custom_transformations. Every entry must name an input and
            every model must exist in the genotype label map.
        required_input: Required dataset names mapped to their required columns.
        column_rules: Per-column content rules for the static datasets.

    Returns:
        One dictionary per (unique_id, tissue, model_group, age), with the fields listed in
        _build_output's output_cols.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])

    # model_map decides which inputs are data files and which are metadata, so a model_map
    # that disagrees with the inputs is checked before it is used. Without these, a config
    # naming no usable data file returns an empty output and a config with one typo'd key
    # returns an output silently missing that file, both reported as a successful run.
    # An absent model_map arrives as None from a YAML key with no value beneath it.
    if not model_map:
        raise ValueError(
            "No model_map provided. The proteomics data files carry no model column, so "
            "each one's model has to be declared in the config under "
            "custom_transformations. Inputs available: "
            f"{', '.join(sorted(set(datasets) - set(required_input)))}."
        )
    if unknown_files := sorted(set(model_map) - set(datasets)):
        raise ValueError(
            f"model_map names {unknown_files}, which are not proteomics data files in "
            "this dataset. Correct the name in the config or add the file to the "
            "dataset's files. Inputs available: "
            f"{', '.join(sorted(set(datasets) - set(required_input)))}."
        )

    datafile_list = [key for key in datasets if key in model_map]
    for file_name in datafile_list:
        validate_data_file_not_empty(file_name, datasets[file_name])
    check_required_datasets_and_columns(
        {name: datasets[name] for name in datafile_list},
        {name: DATAFILE_REQUIRED_COLUMNS for name in datafile_list},
    )
    check_column_rules(
        {name: datasets[name] for name in datafile_list},
        {name: DATAFILE_COLUMN_RULES for name in datafile_list},
    )
    if unknown_models := sorted(
        set(model_map.values()) - set(genotype_label_map_df["model"])
    ):
        raise ValueError(
            f"model_map refers to model(s) {unknown_models} that are absent from the "
            "genotype label map, so none of their rows could be labeled. Add the model to "
            "the label map or correct the config."
        )

    model_metadata_file_names = [
        key for key in datasets if key not in model_map and key not in required_input
    ]
    model_metadata_files = {name: datasets[name] for name in model_metadata_file_names}
    check_required_datasets_and_columns(
        model_metadata_files,
        {name: MODEL_METADATA_REQUIRED_COLUMNS for name in model_metadata_file_names},
    )
    check_column_rules(
        model_metadata_files,
        {name: MODEL_METADATA_COLUMN_RULES for name in model_metadata_file_names},
    )
    harmonized_model_metadata_df = pd.concat(
        [df[MODEL_METADATA_REQUIRED_COLUMNS] for df in model_metadata_files.values()],
        ignore_index=True,
    )
    # Cast before de-duplicating: two studies' metadata files can disagree on the dtype of
    # the join key, so 51503 and "51503" have to collapse to one row rather than survive as
    # two and fan the merge out.
    harmonized_model_metadata_df["individualid"] = harmonized_model_metadata_df[
        "individualid"
    ].astype(str)
    harmonized_model_metadata_df = harmonized_model_metadata_df.drop_duplicates()

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])

    # Gene resolution is the one step that needs every data file at once, and reading it
    # from the column headers keeps it off the measurements. See _measured_header_pairs.
    uniprot_to_ensembl = _resolve_gene_ids(
        header_pairs=_measured_header_pairs(datasets, datafile_list),
        candidates=_build_uniprot_candidates(datasets["uniprot_ensembl_map"]),
        gene_symbols=gene_symbols,
        gene_aliases=_build_gene_aliases(datasets["mouse_gene_metadata"]),
    )

    # Files sharing a model_group have to be built together, because entries are keyed on
    # (unique_id, tissue, model_group, age) and splitting a group would emit two entries
    # for one key. Files in different groups share no key, so building one group at a time
    # keeps peak memory proportional to the largest group instead of to the whole run.
    model_to_model_group = build_model_to_model_group(genotype_label_map_df)
    files_by_model_group: Dict[str, List[str]] = defaultdict(list)
    for file_name in datafile_list:
        files_by_model_group[model_to_model_group[model_map[file_name]]].append(
            file_name
        )
    logger.info(
        "Transform protein_de_individual: data files by model_group: "
        + ", ".join(f"{group}={files}" for group, files in files_by_model_group.items())
    )

    known_individuals = set(harmonized_model_metadata_df["individualid"])

    output = []
    for model_group, file_names in files_by_model_group.items():
        long_frames = []
        for file_name in file_names:
            long_df = _melt_proteomics_file(
                file_name, datasets[file_name], model_map[file_name]
            )
            _check_metadata_coverage(
                file_name, long_df["individualid"], known_individuals
            )
            long_frames.append(long_df)

        combined = (
            pd.concat(long_frames, ignore_index=True)
            if len(long_frames) > 1
            else long_frames[0]
        )
        output.extend(
            _build_output(
                model_group,
                combined,
                harmonized_model_metadata_df,
                uniprot_to_ensembl,
                gene_symbols,
                genotype_label_map_df,
            )
        )
        del long_frames, combined
        gc.collect()

    # Sorted here rather than per group so the ordering is the same whichever model_group a
    # protein came from.
    output.sort(key=lambda entry: (entry["unique_id"], entry["age_numeric"]))

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
