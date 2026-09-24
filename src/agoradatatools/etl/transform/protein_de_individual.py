"""
Transforms wide Model AD proteomics files into the RNA individual nested shape,
plus uniprotid, unique_id, and display_symbol.
"""

import gc
import logging
from collections import defaultdict
from typing import Any

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    ColumnRule,
    NotEmptyRule,
    normalize_zero,
)
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    build_model_to_model_group_lookup,
    create_gene_metadata_dict,
    filter_to_mouse_genes,
    label_genotypes,
    nest_individual_records,
    normalize_tissue,
    prepare_genotype_label_map,
    validate_data_file_not_empty,
    GENOTYPE_LABEL_MAP_COLUMNS,
    GENOTYPE_LABEL_MAP_RULES,
)

logger = logging.getLogger(__name__)

UNITS = "Log2 Relative Abundance"

# JAX-confirmed right-closed ageDeath buckets: <=6 -> 4 months, then 8, 12, 18, 24.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = [4, 8, 12, 18, 24]

REQUIRED_INPUT = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_COLUMNS,
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol"],
    "uniprot_ensembl_map": ["uniprot_id", "ensembl_gene_id"],
}

COLUMN_RULES = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_RULES,
    "uniprot_ensembl_map": {
        "uniprot_id": [NotEmptyRule()],
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


def _build_uniprot_candidates(mapping_df: pd.DataFrame) -> dict[str, list[str]]:
    """Map each UniProt accession to its mouse Ensembl gene ids, smallest first."""
    mouse = filter_to_mouse_genes(mapping_df)
    return {
        accession: sorted(genes)
        for accession, genes in mouse.groupby("uniprot_id")["ensembl_gene_id"]
        .unique()
        .items()
    }


def _canonical_accession(headers: pd.Series) -> pd.Series:
    """Recover the canonical UniProt accession from gene_symbol|uniprotid headers."""
    # Extract lowercases headers and turns hyphens into underscores.
    return (
        headers.str.rsplit("|", n=1)
        .str[-1]
        .str.upper()
        .str.replace("_", "-", regex=False)
    )


def _measured_header_pairs(
    datasets: dict[str, pd.DataFrame], datafile_list: list[str]
) -> pd.DataFrame:
    """Collect accession and header-symbol pairs from protein columns that hold data."""
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


def _observed_gene_names(header_pairs: pd.DataFrame) -> dict[str, set[str]]:
    """Collect the case-folded gene names each accession is labeled with in the data files."""
    # Extract mangles "; " to ";_". Isoform headers also feed the base accession.
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
                names.setdefault(accession, set()).add(name)
                names.setdefault(base, set()).add(name)
    return names


def _resolve_gene_ids(
    header_pairs: pd.DataFrame,
    candidates: dict[str, list[str]],
    gene_symbols: dict[str, str],
) -> dict[str, str]:
    """Pick one Ensembl gene per mapped accession, using the header symbol when it matches."""
    names = _observed_gene_names(header_pairs)
    resolved = {}
    for accession, genes in candidates.items():
        wanted = names.get(accession, set())
        matches = [
            gene for gene in genes if gene_symbols.get(gene, "").casefold() in wanted
        ]
        resolved[accession] = matches[0] if matches else genes[0]
    return resolved


def _lookup_ensembl(
    accessions: pd.Series, uniprot_to_ensembl: dict[str, str]
) -> pd.Series:
    """Map each accession to an Ensembl id, trying the full accession then the base."""
    mapped = accessions.map(uniprot_to_ensembl)
    base_mapped = accessions.str.split("-").str[0].map(uniprot_to_ensembl)
    return mapped.fillna(base_mapped)


def _melt_proteomics_file(
    file_name: str, data_file: pd.DataFrame, model: str
) -> pd.DataFrame:
    """Melt one wide proteomics file into individualid, model, uniprotid, value rows."""
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
    # Cast the join key to string so it matches the metadata key.
    long_df["individualid"] = long_df["individualid"].astype(str)
    long_df["model"] = model
    return long_df[["individualid", "model", "uniprotid", "value"]]


def _check_metadata_coverage(
    file_name: str, individuals: pd.Series, known_individuals: set
) -> None:
    """Log how many of a file's animals have harmonized metadata; raise if none do."""
    unique = set(individuals.unique())
    matched = unique & known_individuals
    logger.info(
        f"Transform protein_de_individual: {file_name}: {len(matched)}/{len(unique)} "
        f"animals have harmonized metadata"
    )
    if not matched:
        raise ValueError(
            f"None of the {len(unique)} animals in proteomics data file "
            f"'{file_name}' were found in the harmonized metadata. The "
            f"individualID values in the two sources are probably no longer "
            f"comparable. Unmatched (first 10): {sorted(unique)[:10]}"
        )


def _log_stage(model_group: str, stage: str, df: pd.DataFrame) -> None:
    """Log measurement and animal counts for one processing stage of a model_group."""
    logger.info(
        f"Transform protein_de_individual: {model_group}: {stage}: {len(df)} measurements, "
        f"{df['individualid'].nunique()} animals"
    )


def _build_output(
    model_group: str,
    long_df: pd.DataFrame,
    harmonized_model_metadata_df: pd.DataFrame,
    uniprot_to_ensembl: dict[str, str],
    gene_symbols: dict[str, str],
    genotype_label_map_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Join metadata onto long proteomics data, derive output fields, and nest records."""
    _log_stage(model_group, "melted", long_df)

    df = long_df.merge(
        harmonized_model_metadata_df,
        on="individualid",
        how="inner",
        validate="many_to_one",
    )
    _log_stage(model_group, "after harmonized metadata join", df)

    df["ensembl_gene_id"] = _lookup_ensembl(df["uniprotid"], uniprot_to_ensembl)
    df = df.dropna(subset=["ensembl_gene_id"])
    _log_stage(model_group, "after gene mapping", df)
    if df.empty:
        raise ValueError(
            f"No rows remained for model_group '{model_group}' after mapping proteins to "
            "genes — check the UniProt to Ensembl mapping file."
        )

    df = label_genotypes(df, genotype_label_map_df, f"model_group '{model_group}'")
    _log_stage(model_group, "after genotype labeling", df)

    # age is a grouping key and groupby drops null keys, so an unbucketable ageDeath
    # would delete those animals with no error.
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

    df["sex"] = remap_sex_labels(df["sex"])
    df["gene_symbol"] = df["ensembl_gene_id"].map(gene_symbols).fillna("")
    df["unique_id"] = df["ensembl_gene_id"] + df["uniprotid"]
    df["display_symbol"] = (
        df["gene_symbol"].where(df["gene_symbol"] != "", df["ensembl_gene_id"])
        + " ("
        + df["uniprotid"]
        + ")"
    )
    df["value"] = df["value"].round(5).map(normalize_zero)

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
    entries = nest_individual_records(df, group_columns=group_cols, units=UNITS)

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
    return entries[output_cols].to_dict(orient="records")


def transform_protein_de_individual(
    datasets: dict[str, pd.DataFrame],
    model_map: dict[str, str],
    required_input: dict[str, list[str]] = REQUIRED_INPUT,
    column_rules: dict[str, dict[str, list[ColumnRule]]] = COLUMN_RULES,
) -> list[dict[str, Any]]:
    """Transform Model AD individual proteomics data into nested per-protein records."""
    # model_map is required because the proteomics files have no model column.
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])

    if not model_map:
        raise ValueError(
            "No model_map provided. Each proteomics file's model has to be declared "
            "in the config under custom_transformations. Inputs available: "
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

    leftover_names = [
        key for key in datasets if key not in model_map and key not in required_input
    ]
    unmapped_proteomics = [
        name
        for name in leftover_names
        if any("|" in str(column) for column in datasets[name].columns)
    ]
    if unmapped_proteomics:
        raise ValueError(
            f"{unmapped_proteomics} look like proteomics data files (columns named "
            "gene_symbol|uniprotid) but are not in model_map. Add them to model_map "
            "or remove them from this dataset's files."
        )
    model_metadata_files = {name: datasets[name] for name in leftover_names}
    check_required_datasets_and_columns(
        model_metadata_files,
        {name: MODEL_METADATA_REQUIRED_COLUMNS for name in leftover_names},
    )
    check_column_rules(
        model_metadata_files,
        {name: MODEL_METADATA_COLUMN_RULES for name in leftover_names},
    )
    harmonized_model_metadata_df = pd.concat(
        [df[MODEL_METADATA_REQUIRED_COLUMNS] for df in model_metadata_files.values()],
        ignore_index=True,
    )
    # Cast the join key to string before de-duplicating so 51503 and "51503" collapse
    # to one row rather than surviving as two and fanning the merge out.
    harmonized_model_metadata_df["individualid"] = harmonized_model_metadata_df[
        "individualid"
    ].astype(str)
    harmonized_model_metadata_df = harmonized_model_metadata_df.drop_duplicates()

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])

    header_pairs = _measured_header_pairs(datasets, datafile_list)
    uniprot_to_ensembl = _resolve_gene_ids(
        header_pairs=header_pairs,
        candidates=_build_uniprot_candidates(datasets["uniprot_ensembl_map"]),
        gene_symbols=gene_symbols,
    )
    unmapped = sorted(
        {
            accession
            for accession in header_pairs["uniprotid"]
            if accession not in uniprot_to_ensembl
            and accession.split("-")[0] not in uniprot_to_ensembl
        }
    )
    if unmapped:
        logger.info(
            f"Transform protein_de_individual: {len(unmapped)} UniProt IDs absent "
            f"from the map (dropped): {unmapped[:10]}"
        )

    model_to_model_group = build_model_to_model_group_lookup(genotype_label_map_df)
    files_by_model_group: dict[str, list[str]] = defaultdict(list)
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

    output.sort(key=lambda entry: (entry["unique_id"], entry["age_numeric"]))

    logger.info(f"Transform protein_de_individual total output entries: {len(output)}")
    return output
