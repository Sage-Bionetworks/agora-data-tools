"""
Shared utilities for the Model AD expression transforms.

Used by rna_de_individual, protein_de_individual, rna_de_aggregate, and
protein_de_aggregate. The genotype label map pieces in particular are a shared
contract: these transforms use the same file and need to perform the same
operations on that data. Changing anything here changes all of them.
"""

import logging
import pandas as pd

from agoradatatools.etl.utils import (
    extract_age_numeric,
    nest_fields,
    normalize_zero,
)
from agoradatatools.etl.validation_utils import (
    ColumnRule,
    check_column_rules,
    check_required_datasets_and_columns,
)

from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)

logger = logging.getLogger(__name__)

# Key is lower-case but the matching will be case-insensitive, because tissue arrives in whateve
# case the source study used.
TISSUE_ALIASES = {"right cerebral hemisphere": "Hemibrain"}

# The per-animal fields nested into each entry's data list. This same set of columns is required in
# all three transforms.
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


def determine_result_order(data_file: pd.DataFrame) -> list[str]:
    """
    Determines the result_order (ordering of display labels) for genotypes in a data file.

    Operates on a data_file that has already been merged with the genotype label map
    and filtered to a single model_group, so every display_label present is guaranteed
    to exist in the actual data. Empty display_label values are guaranteed not to be
    present — the Input Validation stage validates the label map before the transform runs.

    Args:
        data_file: DataFrame already merged with the genotype label map and filtered to
            one model_group. Must have columns: display_label, result_order.

    Returns:
        List of display labels in the correct order based on result_order values.
    """
    unique_labels = data_file[["display_label", "result_order"]].drop_duplicates()
    return unique_labels.sort_values("result_order")["display_label"].tolist()


def prepare_genotype_label_map(genotype_label_map_df: pd.DataFrame) -> pd.DataFrame:
    """Copy the label map and make result_order sortable.

    The Input Validation stage already requires model to map to one model_group
    and one model_type, so this function does not re-check that.
    """
    genotype_label_map_df = genotype_label_map_df.copy()
    genotype_label_map_df["result_order"] = genotype_label_map_df[
        "result_order"
    ].astype(int)
    return genotype_label_map_df


def label_genotypes(
    data_file: pd.DataFrame,
    genotype_label_map_df: pd.DataFrame,
    context: str = "",
) -> pd.DataFrame:
    """Attach display labels to data_file to map each genotype in data_file to a label, and
    drop rows whose (model, genotype) has no label.

    Unmatched rows carry NA result_order out of the left merge, which is what identifies
    them. Dropping them is how heterozygous animals or other non-relevant genotypes are kept
    out of the output. An empty result means the data and the label map no longer share a
    genotype vocabulary rather than that everything was correctly filtered, and this case should
    raise an error.

    Args:
        data_file: Rows with model and genotype columns.
        genotype_label_map_df: Label map, already through prepare_genotype_label_map.
        context: Named in the error message when nothing survives, e.g. a model_group.

    Returns:
        DataFrame with display labels attached and rows with unmapped genotypes removed.

    Raises:
        ValueError: If no rows remain after filtering to mapped genotypes.
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
    group_columns: list[str],
    units: str,
    name_from_model: bool = False,
) -> pd.DataFrame:
    """Nest the per-animal records of one model_group and attach its per-group fields.

    This function is the shared tail of the rna_de_individual and protein_de_individual transforms.
    Each caller derives its own columns before calling and selects its own output columns after,
    which is where the two datasets genuinely differ; everything between is identical and
    lives here so the two cannot drift apart.

    Called once per model_group, so name, matched_control and result_order are constant
    across the frame and are resolved as scalars.

    Example input:

        model  genotype  display_label  result_order  individualid  value  model_group
    0     M1     G1        L1             1             I1            10     MG1
    1     M1     G2        L2             2             I2            20     MG1
    ...

    Example output:

        model_group  name  result_order  data
    0     MG1         M1     1            list of dicts, see below

    `data` is a list of dictionaries of all the individuals belonging to this model:
    [
        {'individual_id': 'I1', 'genotype': 'G1', 'display_label': 'L1', 'value': 10},
        {'individual_id': 'I2', 'genotype': 'G2', 'display_label': 'L2', 'value': 20}
    ]

    Preconditions, none of which are re-checked here:
      - df is already through label_genotypes, so display_label and result_order exist and
        every unmapped genotype is gone. Labeling stays with the callers because each one
        interleaves it with its own validations, which run on the surviving rows.
      - the per-animal values already use their output names: value, not expression, and
        individualid, which is renamed to individual_id here.
      - group_columns contains model_group, which is a grouping key for both datasets and
        the fallback for name.

    Args:
        df: data frame with one row per animal, with the following columns: model, genotype,
            display_label, result_order, individualid, value, and every column named in
            group_columns.
        group_columns: Columns that key one output entry. Kept as top-level columns.
        units: Value units, which differ between transforms.
        name_from_model: Whether to use the "model" column for the output name. When True, the
            output name comes from the "model" column (rna_de_individual behavior). When False,
            the name comes from the "model_group" column (protein_de_individual behavior). If
            `name_from_model` is True but there is more than one unique value in the "model"
            column (as happens with some UCI studies), then the name will come from the
            "model_group" column instead.

    Returns:
        One row per group_columns combination, with the per-animal fields nested into a
        data list plus units, name, matched_control, and result_order.
    """
    ordered_genotypes = determine_result_order(df)

    # name identifies the group in the UI. The name either comes from "model" or "model_group".
    name = df["model" if name_from_model else "model_group"].unique()

    # Fall back to "model_group" if there is more than one unique value in the "model" column. We
    # are guaranteed to have one unique value in the model_group field from the pre-validation steps.
    if name_from_model and len(name) > 1:
        name = df["model_group"].unique()

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
    entries["name"] = name[0]

    # The genotype with the lowest result order in the group is the matched control. This should be
    # the first entry in ordered_genotypes.
    entries["matched_control"] = ordered_genotypes[0]

    # Add the ordered_genotypes list to each row in the entries DataFrame so it is included with
    # each entry in the JSON output
    entries["result_order"] = [ordered_genotypes] * len(entries)
    return entries


def normalize_tissue(tissue: pd.Series) -> pd.Series:
    """Apply TISSUE_ALIASES case-insensitively, leaving any other tissue unchanged.

    The cast is required, not cosmetic: an entirely unpopulated tissue column is read as
    float and has no usable str accessor.
    """
    normalized = tissue.astype("string").str.strip()

    # Any values not in TISSUE_ALIASES are assigned NaN by map(), so we fill NaN values with their
    # original value
    return normalized.str.casefold().map(TISSUE_ALIASES).fillna(normalized)


def build_model_to_model_group_lookup(
    genotype_label_map_df: pd.DataFrame,
) -> dict[str, str]:
    """
    Build a lookup mapping each model to its model_group.

    This function assumes the following have been validated before calling:
        * Each model maps to exactly one unique model_group
        * There are no missing model or model_group values

    Args:
        genotype_label_map_df: DataFrame with 'model' and 'model_group' columns

    Returns:
        Dictionary mapping model to model_group
    """
    return (
        genotype_label_map_df.drop_duplicates("model")
        .set_index("model")["model_group"]
        .to_dict()
    )


def build_model_type_lookup(genotype_label_map_df: pd.DataFrame) -> dict[str, str]:
    """Map each model to its model_type.

    Assumes the Input Validation stage has already rejected a model with more
    than one model_type, so one row per model is enough.
    """
    return (
        genotype_label_map_df.drop_duplicates("model")
        .set_index("model")["model_type"]
        .to_dict()
    )


def build_biodomain_lookup(biodom_genes_mm_df: pd.DataFrame) -> dict[str, list[str]]:
    """Map each ensembl id to its biodomain names.

    Rows with no ensembl id are dropped: they cannot be joined to a gene.
    """
    present = biodom_genes_mm_df.dropna(axis="index", subset=["ensembl_id"])
    return (
        present[["ensembl_id", "biodomain"]]
        .drop_duplicates()
        .groupby("ensembl_id")["biodomain"]
        .apply(list)
        .to_dict()
    )


def create_gene_metadata_dict(mouse_gene_metadata_df: pd.DataFrame) -> dict[str, str]:
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


def validate_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    file_index: int,
    total_files: int,
    data_file_required_columns: list[str],
    data_file_column_rules: dict[str, list[ColumnRule]] | None = None,
) -> None:
    """Shared validation preamble for an expression data file.

    The empty-file check runs before the required-column check so an empty file
    is not reported as a missing-column error. Column value rules are checked
    only when supplied.

    Raises:
        ValueError: If the file is empty, a required column is missing, or a
            supplied column value rule is violated.
    """
    log_file_processing_info(file_name, file_index, total_files, data_file)
    validate_data_file_not_empty(file_name, data_file)
    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: data_file_required_columns}
    )
    if data_file_column_rules:
        check_column_rules({file_name: data_file}, {file_name: data_file_column_rules})


def preprocess_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    file_index: int,
    total_files: int,
    data_file_required_columns: list[str],
    data_file_column_rules: dict[str, list[ColumnRule]],
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
    validate_data_file(
        file_name,
        data_file,
        file_index,
        total_files,
        data_file_required_columns,
        data_file_column_rules,
    )
    data_file = filter_to_mouse_genes(data_file)
    data_file["tissue"] = normalize_tissue(data_file["tissue"])
    # Map plural source sex values to the singular display form
    data_file["sex"] = remap_sex_labels(data_file["sex"])
    data_file["expression"] = data_file["expression"].astype(float)
    data_file = data_file.round(decimals=5)
    data_file["individualid"] = data_file["individualid"].astype(str)
    return data_file


def create_age_entries_from_group(
    group: pd.DataFrame,
    ensembl_gene_id: str,
    model: str,
    tissue: str,
    sex: str,
) -> dict[str, dict[str, float]]:
    """Build age -> log2_fc / adj_p_val from a group that already has log2foldchange.

    Entries are in numeric age order. The caller has already required age to match
    N months, so extract_age_numeric always finds the integer.

    Missing padj becomes 1.0: a missing p-value is not evidence of significance
    and must not present as 0.0. Negative padj raises.
    """
    ordered = sorted(
        group.itertuples(index=False), key=lambda row: extract_age_numeric(row.age)
    )
    age_entries = {}
    for row in ordered:
        if not pd.isna(row.padj) and float(row.padj) < 0.0:
            raise ValueError(
                f"Negative adjusted p-value found in data for gene "
                f"'{ensembl_gene_id}', model '{model}', tissue '{tissue}', "
                f"sex '{sex}'. Expected positive adjusted p-value but found: "
                f"'{row.padj}'"
            )
        age_entries[str(row.age)] = {
            "log2_fc": normalize_zero(float(row.log2foldchange)),
            "adj_p_val": 1.0 if pd.isna(row.padj) else float(row.padj),
        }
    return age_entries
