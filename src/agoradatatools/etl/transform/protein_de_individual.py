"""
Transforms wide Model AD proteomics files into the RNA individual nested shape,
plus uniprotid, unique_id, and display_symbol.
"""

import gc
import logging
from collections import defaultdict
from collections.abc import Collection
from typing import Any

import pandas as pd

from agoradatatools.etl.utils import normalize_zero
from agoradatatools.etl.validation_utils import raise_on_duplicate_keys
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    build_model_to_model_group_lookup,
    create_gene_metadata_dict,
    label_genotypes,
    nest_individual_records,
    normalize_tissue,
    prepare_genotype_label_map,
)
from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    add_protein_identity_columns,
    build_uniprot_to_ensembl,
    canonical_accession,
    log_unmapped_accessions,
    map_accessions_to_genes,
)

logger = logging.getLogger(__name__)

UNITS = "Log2 Relative Abundance"

# JAX-confirmed right-closed ageDeath buckets (confirmed with Annat):
# <=6 -> 4 months, >6 and <=10 -> 8, >10 and <=16 -> 12, >16 and <=20 -> 18, >20 -> 24.
# 14.2 months is a 12-month animal. Jax studies use wide ranges for each age group.
AGE_BINS = [float("-inf"), 6, 10, 16, 20, float("inf")]
AGE_LABELS = [4, 8, 12, 18, 24]

STATIC_INPUTS = frozenset(
    {"genotype_label_map", "mouse_gene_metadata", "uniprot_ensembl_map"}
)

# Columns copied from each harmonized-metadata file onto the measurements.
METADATA_COLUMNS = [
    "individualid",
    "sex",
    "agedeath",
    "genotype",
    "tissue",
]


def _measured_accessions(
    datasets: dict[str, pd.DataFrame], datafile_list: list[str]
) -> pd.Series:
    """Collect the canonical UniProt accessions of protein columns that hold data. Column headers look like "abc|q123",
    where everything before the "|" is the lower-cased gene symbol and everything after the "|" is the lower-cased
    Uniprot accession. The Uniprot IDs are extracted from the headers and returned to their original upper-case values.
    """
    headers = pd.Series(
        [
            column
            for file_name in datafile_list
            for column in datasets[file_name].columns
            if "|" in column and datasets[file_name][column].notna().any()
        ],
        dtype="object",
    )
    return canonical_accession(headers).drop_duplicates()


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
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["value"])
    if long_df.empty:
        raise ValueError(
            f"Either every abundance value in proteomics data file '{file_name}' is "
            "missing or non-numeric values were found, so it contributes nothing "
            "to the output."
        )

    long_df["uniprotid"] = canonical_accession(long_df["header"])
    # Cast the join key to string so it matches the metadata key.
    long_df["individualid"] = long_df["individualid"].astype(str)
    long_df["model"] = model
    return long_df[["individualid", "model", "uniprotid", "value"]]


def _check_metadata_coverage(
    file_name: str, individuals: pd.Series, model: str, known_individuals: set
) -> None:
    """Log how many of a file's animals have harmonized metadata; raise if none do."""
    unique = set(individuals.unique())
    matched = {
        individual for individual in unique if (individual, model) in known_individuals
    }
    # Counted per file, so the same animal is counted again in every file that measures it.
    logger.info(
        f"{file_name}: {len(matched)}/{len(unique)} of this file's animals have harmonized metadata"
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
        f"{model_group}: {stage}: {len(df)} measurements, {df['individualid'].nunique()} animals"
    )


def _log_cross_file_animals(
    model_group: str, animals_by_file: dict[str, set[str]]
) -> None:
    """Log animals measured in more than one of a model_group's data files.

    Coverage is reported per file, so an animal in two files is counted in both. Without this
    the per-file counts do not add up to the model_group's count.
    """
    seen: set[str] = set()
    shared: set[str] = set()
    for animals in animals_by_file.values():
        shared |= seen & animals
        seen |= animals
    if shared:
        logger.info(
            f"{model_group}: {len(shared)} of {len(seen)} animals are measured in more than one "
            f"data file, so the per-file counts above sum to more than this model_group's total. "
            f"Animals (first 10): {sorted(shared)[:10]}"
        )


def _collapse_duplicate_measurements(
    model_group: str, df: pd.DataFrame
) -> pd.DataFrame:
    """Collapse a protein measured for one animal in more than one file, raising if they disagree.

    An animal measured in two data files reaches the same output entry from both, because every
    grouping key is derived from its metadata or the protein header. nest_individual_records does
    not de-duplicate, so without this its individual_id would appear twice in one protein's data
    list.

    Runs on the rows that survive to the output, so repeats among animals that are dropped for
    their genotype are not reported.
    """
    keys = ["individualid", "uniprotid"]
    collapsed = df.drop_duplicates(subset=keys + ["value"])
    raise_on_duplicate_keys(
        collapsed,
        keys,
        f"Model_group '{model_group}': the data files disagree about the abundance of a "
        "protein, so which value belongs on the page cannot be decided here.",
    )
    if len(collapsed) < len(df):
        shared = sorted(
            df.loc[df.duplicated(subset=keys, keep=False), "individualid"].unique()
        )
        logger.info(
            f"{model_group}: {len(df) - len(collapsed)} repeated measurements collapsed for "
            f"{len(shared)} animals measured in more than one data file. Animals (first 10): "
            f"{shared[:10]}"
        )
    return collapsed


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
        on=["individualid", "model"],
        how="inner",
        validate="many_to_one",
    )
    _log_stage(model_group, "after harmonized metadata join", df)

    df = map_accessions_to_genes(
        df, uniprot_to_ensembl, f"for model_group '{model_group}'"
    )
    _log_stage(model_group, "after gene mapping", df)

    df = label_genotypes(df, genotype_label_map_df, f"model_group '{model_group}'")
    _log_stage(model_group, "after genotype labeling", df)

    df = _collapse_duplicate_measurements(model_group, df)

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
    df = add_protein_identity_columns(df, gene_symbols)
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


def _validate_file_maps(
    datasets: dict[str, pd.DataFrame],
    model_map: dict[str, str],
    metadata_map: dict[str, str],
    static_inputs: Collection[str],
    known_models: set[str],
) -> None:
    """Raise if model_map or metadata_map is empty, overlaps, leftover, or names unknown files or models."""
    available = ", ".join(sorted(set(datasets) - set(static_inputs)))
    for map_name, file_map, label in (
        ("model_map", model_map, "proteomics data"),
        ("metadata_map", metadata_map, "metadata"),
    ):
        if not file_map:
            raise ValueError(
                f"No {map_name} provided. Each {label} file's model has to be declared "
                f"in the config under custom_transformations. Inputs available: {available}."
            )
        if unknown_files := sorted(set(file_map) - set(datasets)):
            raise ValueError(
                f"{map_name} names {unknown_files}, which are not {label} files in this "
                f"dataset. Correct the name in the config or add the file to the dataset's "
                f"files. Inputs available: {available}."
            )
        if unknown_models := sorted(set(file_map.values()) - known_models):
            raise ValueError(
                f"{map_name} refers to model(s) {unknown_models} that are absent "
                "from the genotype label map, so none of their rows could be labeled. "
                "Add the model to the label map or correct the config."
            )
    if overlap := sorted(set(model_map) & set(metadata_map)):
        raise ValueError(
            f"{overlap} appear in both model_map and metadata_map. Each file can "
            "only be in one map."
        )
    leftover_names = sorted(
        set(datasets) - set(static_inputs) - set(model_map) - set(metadata_map)
    )
    if leftover_names:
        raise ValueError(
            f"{leftover_names} are not in model_map or metadata_map. Add them to the "
            "appropriate map or remove them from this dataset's files."
        )


def transform_protein_de_individual(
    datasets: dict[str, pd.DataFrame],
    model_map: dict[str, str],
    metadata_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Transform Model AD individual proteomics data into nested per-protein records."""
    # model_map and metadata_map are required because the proteomics files have no model column.
    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])
    _validate_file_maps(
        datasets,
        model_map,
        metadata_map,
        STATIC_INPUTS,
        set(genotype_label_map_df["model"]),
    )

    datafile_list = [key for key in datasets if key in model_map]

    metadata_names = list(metadata_map)

    stamped_metadata = []
    for name in metadata_names:
        frame = datasets[name][METADATA_COLUMNS].copy()
        frame["model"] = metadata_map[name]
        stamped_metadata.append(frame)
    harmonized_model_metadata_df = pd.concat(stamped_metadata, ignore_index=True)
    # Cast the join key to string before de-duplicating so 51503 and "51503" collapse
    # to one row rather than surviving as two and fanning the merge out.
    harmonized_model_metadata_df["individualid"] = harmonized_model_metadata_df[
        "individualid"
    ].astype(str)
    harmonized_model_metadata_df = harmonized_model_metadata_df.drop_duplicates()

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])

    uniprot_to_ensembl = build_uniprot_to_ensembl(datasets["uniprot_ensembl_map"])
    log_unmapped_accessions(
        _measured_accessions(datasets, datafile_list), uniprot_to_ensembl
    )

    model_to_model_group = build_model_to_model_group_lookup(genotype_label_map_df)
    files_by_model_group: dict[str, list[str]] = defaultdict(list)
    for file_name in datafile_list:
        files_by_model_group[model_to_model_group[model_map[file_name]]].append(
            file_name
        )
    logger.info(
        "Data files by model_group: "
        + ", ".join(f"{group}={files}" for group, files in files_by_model_group.items())
    )

    known_individuals = set(
        zip(
            harmonized_model_metadata_df["individualid"],
            harmonized_model_metadata_df["model"],
        )
    )

    output = []
    for model_group, file_names in files_by_model_group.items():
        long_frames = []
        animals_by_file: dict[str, set[str]] = {}
        for file_name in file_names:
            long_df = _melt_proteomics_file(
                file_name, datasets[file_name], model_map[file_name]
            )
            _check_metadata_coverage(
                file_name,
                long_df["individualid"],
                model_map[file_name],
                known_individuals,
            )
            animals_by_file[file_name] = set(long_df["individualid"])
            long_frames.append(long_df)
        _log_cross_file_animals(model_group, animals_by_file)

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
