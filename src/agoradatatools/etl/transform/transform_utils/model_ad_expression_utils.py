"""
Shared utilities for the Model AD expression transforms.

Used by rna_de_individual, protein_de_individual, and rna_de_aggregate. The genotype
label map pieces in particular are a shared contract: MG-980 renders the datasets on
the same page, so a genotype label, an ordering, or a tissue name that differs between
them is a visible product bug. Changing anything here changes all three datasets.

filter_to_mouse_genes and preprocess_data_file are still RNA-only.
"""

import logging
from typing import Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    nest_fields,
    ColumnRule,
    NotEmptyRule,
)

from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)

logger = logging.getLogger(__name__)

GENOTYPE_LABEL_MAP_COLUMNS = [
    "model",
    "model_group",
    "display_label",
    "genotype",
    "result_order",
]

# Every column is required to be populated: a blank display_label or result_order silently
# unlabels or reorders a genotype rather than failing.
GENOTYPE_LABEL_MAP_RULES: Dict[str, List[ColumnRule]] = {
    column: [NotEmptyRule()] for column in GENOTYPE_LABEL_MAP_COLUMNS
}

# Keyed case-folded, because tissue arrives in whatever case the source study used.
TISSUE_ALIASES = {"right cerebral hemisphere": "Hemibrain"}

# The per-animal fields nested into each entry's data list. Shared rather than per-transform
# because MG-980 renders both datasets from the same component, so a field present in one
# data list and absent from the other is a visible product bug.
INDIVIDUAL_DATA_COLUMNS = ["genotype", "sex", "individual_id", "value"]


def filter_to_mouse_genes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to keep only mouse genes (ENSMUSG*), excluding human genes (ENSG*).

    Args:
        df: DataFrame with an 'ensembl_gene_id' column

    Returns:
        Filtered DataFrame containing only mouse genes
    """
    return df[df["ensembl_gene_id"].str.startswith("ENSMUSG")].copy()


def determine_result_order(data_file: pd.DataFrame) -> List[str]:
    """
    Determines the result_order (ordering of display labels) for genotypes in a data file.

    Operates on a data_file that has already been merged with the genotype label map
    and filtered to a single model_group, so every display_label present is guaranteed
    to exist in the actual data. Empty display_label values are guaranteed not to be
    present — check_column_rules validates the label map before processing begins.

    Args:
        data_file: DataFrame already merged with the genotype label map and filtered to
            one model_group. Must have columns: display_label, result_order.

    Returns:
        List of display labels in the correct order based on result_order values.
    """
    unique_labels = data_file[["display_label", "result_order"]].drop_duplicates()
    return unique_labels.sort_values("result_order")["display_label"].tolist()


def prepare_genotype_label_map(genotype_label_map_df: pd.DataFrame) -> pd.DataFrame:
    """Copy the label map, make result_order sortable, and reject an inconsistent one."""
    genotype_label_map_df = genotype_label_map_df.copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    validate_model_group_consistency(genotype_label_map_df)
    return genotype_label_map_df


def label_genotypes(
    data_file: pd.DataFrame,
    genotype_label_map_df: pd.DataFrame,
    context: str = "",
) -> pd.DataFrame:
    """Attach display labels to data_file and drop rows whose (model, genotype) has no label.

    Unmatched rows carry NA result_order out of the left merge, which is what identifies
    them. Dropping them is how wildtype and heterozygous animals are kept out of the output,
    so an empty result means the data and the label map no longer share a genotype
    vocabulary rather than that everything was correctly filtered.

    display_label is deliberately left un-renamed: determine_result_order reads it, so the
    caller renames only once it is finished with it.

    Args:
        data_file: Rows with model and genotype columns.
        genotype_label_map_df: Label map, already through prepare_genotype_label_map.
        context: Named in the error message when nothing survives, e.g. a model_group.
    """
    data_file = data_file.merge(
        genotype_label_map_df,
        on=["model", "genotype"],
        how="left",
        validate="many_to_one",
    ).dropna(subset=["result_order"])

    if data_file.empty:
        where = f" for {context}" if context else ""
        raise ValueError(
            f"No rows remained{where} after filtering to mapped genotypes — none of the "
            "genotypes present were found in the genotype label map."
        )
    return data_file


def nest_individual_records(
    df: pd.DataFrame,
    group_columns: List[str],
    units: str,
    name_from_model: bool = False,
) -> pd.DataFrame:
    """Nest the per-animal records of one model_group and attach its per-group fields.

    The shared tail of the rna_de_individual and protein_de_individual transforms. Each
    caller derives its own columns before calling and selects its own output columns after,
    which is where the two datasets genuinely differ; everything between is identical and
    lives here so the two cannot drift apart on the same page.

    A DataFrame is returned rather than records because rna_de_individual still adds
    columns and sorts afterwards, and protein_de_individual does not.

    Called once per model_group, so name, matched_control and result_order are constant
    across the frame and are resolved as scalars.

    Preconditions, none of which are re-checked here:
      - df is already through label_genotypes, so display_label and result_order exist and
        every unmapped genotype is gone. Labeling stays with the callers because each one
        interleaves it with its own validations, which run on the surviving rows.
      - the per-animal values already use their output names: value, not expression, and
        individualid, which is renamed to individual_id here.
      - group_columns contains model_group, which is a grouping key for both datasets and
        the fallback for name.

    Args:
        df: Labeled per-animal rows carrying model, genotype, display_label, result_order,
            individualid, value, and every column named in group_columns.
        group_columns: Columns that key one output entry. Kept as top-level columns.
        units: Value units, passed in per transform rather than shared, so correcting one
            dataset's units does not silently change the other's.
        name_from_model: When True, name is the model itself for a group that has exactly
            one, which is rna_de_individual's behavior. protein_de_individual always names
            the model_group.

    Returns:
        One row per group_columns combination, with the per-animal fields nested into a
        data list plus units, name, matched_control, and result_order.
    """
    result_order = determine_result_order(df)

    # name identifies the group in the UI. Read from the labeled frame, because a model
    # whose genotypes are all unmapped is gone by this point and must not make a
    # single-model group look like several. model_group is the fallback for both.
    models = df["model"].unique() if name_from_model else []
    name = models[0] if len(models) == 1 else None

    # Drop the raw genotype before renaming display_label so there is no duplicate column.
    df = df.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype", "individualid": "individual_id"}
    )
    entries = nest_fields(
        df[group_columns + INDIVIDUAL_DATA_COLUMNS],
        grouping=group_columns,
        new_column="data",
        drop_columns=group_columns,
    )

    entries["units"] = units
    entries["name"] = name if name is not None else entries["model_group"]
    # result_order is non-empty: label_genotypes raises on an empty frame, so there is
    # always at least one label, and the lowest result_order among them is the control.
    #
    # Limitation for 4-genotype UCI studies: some DE analyses pair each case genotype with
    # a different control (e.g. Trem2-R47H_NSS.5xFAD vs Trem2-R47H_NSS, not vs C57BL/6J).
    # A single matched_control is a simplification there — it reflects the group's overall
    # reference genotype rather than the per-case-genotype DE pairing.
    entries["matched_control"] = result_order[0]
    # Every row shares one list object. Safe because nothing mutates it after this point;
    # to_dict and json.dump only read it.
    entries["result_order"] = [result_order] * len(entries)
    return entries


def normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Apply TISSUE_ALIASES case-insensitively, leaving any other tissue unchanged.

    The cast is required, not cosmetic: an entirely unpopulated tissue column is read as
    float and has no usable str accessor.
    """
    normalized = tissue.astype("string").str.strip()
    return normalized.str.casefold().map(TISSUE_ALIASES).fillna(normalized)


def validate_model_group_consistency(
    genotype_label_map_df: pd.DataFrame,
) -> None:
    """
    Validate that each model has consistent model_group values.
    Each model should map to exactly one unique model_group; having multiple
    different model_group values for the same model indicates a data quality issue.

    None/NaN values are counted as a single distinct value (i.e. "no group assigned")
    rather than being excluded from the uniqueness check.

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Raises:
        ValueError: If any model has inconsistent model_group values
    """
    inconsistent_models = (
        genotype_label_map_df.groupby("model")["model_group"]
        .nunique(dropna=False)
        .pipe(lambda x: x[x > 1].index.tolist())
    )
    if inconsistent_models:
        raise ValueError(
            f"Each model must have a consistent model_group value in genotype_label_map. "
            f"Models with inconsistent model_group values: {inconsistent_models}"
        )


def build_model_to_model_group(
    genotype_label_map_df: pd.DataFrame,
) -> Dict[str, str]:
    """
    Build a lookup mapping each model to its model_group.

    Taking the first row per model is safe because validate_model_group_consistency
    rejects a label map that gives one model more than one model_group.

    An all-missing model_group is stored as None rather than NaN so it matches
    groupby().first() and serializes as JSON null. The individual transforms reject
    empty model_group values before this is called; rna_de_aggregate still allows them.

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Returns:
        Dictionary mapping model to model_group
    """
    return {
        model: None if pd.isna(group) else group
        for model, group in (
            genotype_label_map_df.drop_duplicates("model")
            .set_index("model")["model_group"]
            .items()
        )
    }


def create_gene_metadata_dict(mouse_gene_metadata_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create a lookup dictionary mapping Ensembl gene IDs to gene symbols.

    Missing gene_symbol values are dropped so a caller that reads with dict.get
    (rna_de_aggregate) does not leak a NaN into the output. Callers that map and
    fill missing values with an empty string (the individual transforms) are unaffected.

    Args:
        mouse_gene_metadata_df: DataFrame with 'ensembl_gene_id' and 'gene_symbol' columns

    Returns:
        Dictionary mapping ensembl_gene_id to gene_symbol
    """
    return (
        mouse_gene_metadata_df.set_index("ensembl_gene_id")["gene_symbol"]
        .dropna()
        .to_dict()
    )


def log_file_processing_info(
    file_name: str,
    file_index: int,
    total_files: int,
    data_file: pd.DataFrame,
) -> None:
    """
    Log information about a file being processed.

    Args:
        file_name: Name of the file being processed
        file_index: Current file index (0-based)
        total_files: Total number of files to process
        data_file: DataFrame being processed
    """
    logger.info(
        f"Processing {file_name} ({file_index+1}/{total_files}): {len(data_file)} rows, "
        f"{len(data_file.columns)} columns, "
        f"{data_file.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
    )


def validate_data_file_not_empty(file_name: str, data_file: pd.DataFrame) -> None:
    """
    Validate that a data file is not empty.

    Args:
        file_name: Name of the file being validated
        data_file: DataFrame to validate

    Raises:
        ValueError: If the data file is empty
    """
    if len(data_file) == 0:
        raise ValueError(f"Data file {file_name} is empty")


def preprocess_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    file_index: int,
    total_files: int,
    data_file_required_columns: List[str],
    data_file_column_rules: Dict[str, List[ColumnRule]],
) -> pd.DataFrame:
    """
    Preprocess a single data file with common validation and transformation steps.

    Returns the preprocessed DataFrame so callers can accumulate and concatenate
    results across multiple files before further processing.

    Args:
        file_name: Name of the file being processed
        data_file: DataFrame to preprocess
        file_index: Index of this file in the processing sequence (0-based)
        total_files: Total number of files being processed
        data_file_required_columns: List of column names that must be present
        data_file_column_rules: Per-column content rules to validate via
            check_column_rules. Keys are column names; values are lists of ColumnRule
            objects. Rules are checked after required-column validation.

    Returns:
        Preprocessed DataFrame with mouse genes only, tissue names mapped and
        plural sex values mapped to singular display labels, and
        numeric values rounded to 5 decimal places.

    Raises:
        ValueError: If the file is empty, missing required columns, or any column
            value rule is violated.
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)
    validate_data_file_not_empty(file_name, data_file)
    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )
    check_column_rules({file_name: data_file}, {file_name: data_file_column_rules})
    data_file = filter_to_mouse_genes(data_file)
    data_file["tissue"] = normalize_tissue(data_file["tissue"])
    # Map plural source sex values to the singular display form
    data_file["sex"] = remap_sex_labels(data_file["sex"])
    data_file["expression"] = data_file["expression"].astype(float)
    data_file = data_file.round(decimals=5)
    data_file["individualid"] = data_file["individualid"].astype(str)
    return data_file
