"""
This module contains the transformation logic for the marmo_details dataset.
This is for the Model AD project (marmoset details pages).
"""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    MatchesRegexRule,
    NonNegativeRule,
    NotEmptyRule,
    NumericRule,
    OneOfRule,
    UniqueRule,
    check_column_rules,
    check_required_datasets_and_columns,
    nest_fields,
    normalize_null_values,
    round_y_axis_max,
    standardize_column_name,
    validate_one_to_one_mapping,
    validate_references_exist,
)


REQUIRED_INPUT = {
    "marmo_model_metadata": [
        "model",
        "model_type",
        "study_synid",
        "modified_gene",
        "ensembl_gene_id",
        "allele_type",
    ],
    "marmo_genotype_label_map": [
        "model",
        "genotype",
        "display_label",
    ],
    "marmo_biomarker_measure_info": [
        "result_column",
        "evidence_type",
        "units",
        "display_order",
    ],
    "marmo_individual_metadata": [
        "individualid",
        "genotype",
        "sex",
    ],
    "marmo_biomaterial_metadata": [
        "biomaterialid",
        "collectionage",
        "collectionageunits",
    ],
    "marmo_results": [
        "biomaterialid",
        "individualid",
        "qc_ab",
        "qc_neuro",
    ],
}

COLUMN_RULES = {
    "marmo_model_metadata": {
        "model": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(r"^ENSCJAG\d+$")],
    },
    "marmo_genotype_label_map": {
        "model": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "display_label": [NotEmptyRule()],
    },
    "marmo_biomarker_measure_info": {
        "result_column": [NotEmptyRule()],
        "evidence_type": [NotEmptyRule()],
        "display_order": [NotEmptyRule(), NumericRule(), NonNegativeRule()],
    },
    "marmo_individual_metadata": {
        "individualid": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "sex": [NotEmptyRule()],
    },
    "marmo_biomaterial_metadata": {
        "biomaterialid": [UniqueRule()],
        "collectionage": [NumericRule(), NonNegativeRule()],
    },
    "marmo_results": {
        "biomaterialid": [NotEmptyRule()],
        "individualid": [NotEmptyRule()],
    },
}

# Ages are bucketed as months, so any other unit must fail. OneOfRule counts nulls as violations
# and the no-age assays leave the unit blank, so this can only run on the consumed subset.
REFERENCED_BIOMATERIAL_RULES = {
    "marmo_biomaterial_metadata": {
        "collectionageunits": [OneOfRule({"months"})],
    },
}

MONTHS_PER_YEAR = 12

# Each QC flag in marmo_results gates a group of assays: a measurement is dropped when its group's QC
# flag is not "PASS". The two QC flags are independent, so a row can pass one group and fail the other.
QC_MEASURE_GROUPS = {
    "qc_ab": ["ab40_pg_ml", "ab42_pg_ml", "ab_ratio"],
    "qc_neuro": ["gfap_pg_ml", "nfl_pg_ml", "ttau_fg_ml"],
}


def _validate_and_prepare_model_metadata(
    genotype_map: pd.DataFrame, raw_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Validate the label map and model metadata together, and normalize metadata blanks.

    Returns marmo_model_metadata with blanks normalized to None so taking iloc[0] later is
    not an arbitrary pick of conflicting model_type or study_synid values.

    Args:
        genotype_map (pd.DataFrame): marmo_genotype_label_map.
        raw_metadata (pd.DataFrame): marmo_model_metadata before null normalization.

    Returns:
        pd.DataFrame: Normalized marmo_model_metadata.

    Raises:
        ValueError: If the label map has duplicate (model, genotype) rows, a model has
            inconsistent model_type or study_synid values, or the label map names a model
            absent from marmo_model_metadata.
    """
    # (model, genotype) must be unique: a duplicate pair would multiply points within a model.
    duplicate_keys = genotype_map.duplicated(subset=["model", "genotype"], keep=False)
    if duplicate_keys.any():
        dupes = (
            genotype_map.loc[duplicate_keys, ["model", "genotype"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        raise ValueError(
            "marmo_genotype_label_map has duplicate (model, genotype) rows, which would "
            f"multiply measurements within a model: {dupes}"
        )

    # Blanks in model_type, study_synid, modified_gene, and allele_type are allowed and become
    # None in the JSON, matching transform_model_details. ensembl_gene_id stays NotEmptyRule.
    metadata = normalize_null_values(raw_metadata)
    # A model has one row per modified gene. model_type and study_synid must be the same on
    # every row so taking the first row of each model later is not an arbitrary pick.
    validate_one_to_one_mapping(metadata, "model", "model_type")
    validate_one_to_one_mapping(metadata, "model", "study_synid")

    # Hand-maintained files: all models in genotype_map must exist in marmo_model_metadata.
    # Extra or typo'd models in genotype_map would have their rows silently removed and would
    # not get a page on the explorer.
    validate_references_exist(
        genotype_map["model"],
        metadata["model"],
        source_name="marmo_genotype_label_map",
        target_name="marmo_model_metadata",
        item_name="models",
    )
    return metadata


def _prepare_measure_info(raw_measure_info: pd.DataFrame) -> pd.DataFrame:
    """Return measure info with the derived columns _build_measurements needs.

    Adds a standardized result_column_std, a numeric display_order, and empty-string units.

    Args:
        raw_measure_info (pd.DataFrame): marmo_biomarker_measure_info.

    Returns:
        pd.DataFrame: Measure info ready for _build_measurements.
    """
    measure_info = raw_measure_info.copy()
    measure_info["result_column_std"] = measure_info["result_column"].apply(
        standardize_column_name
    )
    measure_info["display_order"] = pd.to_numeric(
        measure_info["display_order"], errors="coerce"
    )
    # The A-beta ratio has no units; empty string rather than null.
    return normalize_null_values(measure_info, empty_string_columns=["units"])


def _apply_qc_masks(results: pd.DataFrame) -> pd.DataFrame:
    """Null out measurements for an assay-group if that group's QC flag is not PASS.

    For each QC flag in QC_MEASURE_GROUPS, sets that group's measure columns to NaN on rows where the
    flag is not "PASS" (case-insensitive, ignoring leading/trailing whitespace).

    Args:
        results (pd.DataFrame): marmo_results

    Returns:
        pd.DataFrame: A copy of results with QC-failing measure values set to NaN.
    """
    masked = results.copy()
    for qc_column, measure_columns in QC_MEASURE_GROUPS.items():
        present = [column for column in measure_columns if column in masked.columns]
        if not present:
            continue
        # astype(str) renders blank/NaN flags as "nan"/"none", so they compare unequal to "PASS"
        # and are treated as not-passing; it also keeps the mask a plain boolean (a nullable-string
        # comparison would yield pd.NA and break .loc indexing).
        normalized = masked[qc_column].astype(str).str.strip().str.upper()
        failing = normalized != "PASS"
        masked.loc[failing, present] = pd.NA
    return masked


def _build_measurements(
    datasets: Dict[str, pd.DataFrame],
    measure_info: pd.DataFrame,
) -> pd.DataFrame:
    """Build the per-measurement DataFrame behind the biomarkers collection.

    Melts the wide measure columns, resolves genotypes to display labels and models, joins
    collection ages, and attaches measure metadata. Measurements that did not pass QC, have
    no label-map genotype, or have no biomaterial record are dropped.

    Args:
        datasets (Dict[str, pd.DataFrame]): The input datasets.
        measure_info (pd.DataFrame): Measure info with a standardized "result_column_std" column.

    Returns:
        pd.DataFrame: One row per surfaced measurement, including its model from the label map.

    Raises:
        ValueError: If marmo_biomarker_measure_info is empty or names a result column absent
            from marmo_results, if a consumed biomaterial row violates
            REFERENCED_BIOMATERIAL_RULES, or if no measurement survives the value, genotype, or
            collection-age filters.
    """
    individual = datasets["marmo_individual_metadata"]
    biomaterial = datasets["marmo_biomaterial_metadata"]
    genotype_map = datasets["marmo_genotype_label_map"]

    if measure_info.empty:
        raise ValueError(
            "marmo_biomarker_measure_info lists no measures, so no biomarker can be plotted."
        )

    # Convert measurements whose assay-group QC flag is not PASS before melting; the null-drop below
    # will remove them along with the genuinely missing values.
    results = _apply_qc_masks(datasets["marmo_results"])

    # A typo'd result_column would otherwise drop that measure silently.
    measure_columns = list(measure_info["result_column_std"])
    validate_references_exist(
        measure_columns,
        results.columns,
        source_name="marmo_biomarker_measure_info",
        target_name="marmo_results",
        item_name="result columns",
    )

    long = results.melt(
        id_vars=["biomaterialid", "individualid"],
        value_vars=measure_columns,
        var_name="result_column_std",
        value_name="value",
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])
    if long.empty:
        raise ValueError(
            "marmo_results has no numeric values in any measurement column listed in "
            "marmo_biomarker_measure_info."
        )

    # m:1: a duplicated individual would silently duplicate all of its measurements.
    long = long.merge(
        individual[["individualid", "genotype", "sex"]],
        how="left",
        on="individualid",
        validate="m:1",
    )
    # m:m rather than m:1 because a shared control genotype (WT) is listed once per model that
    # uses it, so one measurement legitimately fans out to several models. What would wrongly
    # multiply points is a repeated (model, genotype) pair, which the caller checks for.
    long = long.merge(
        genotype_map[["model", "genotype", "display_label"]],
        how="inner",
        on="genotype",
        validate="m:m",
    )
    if long.empty:
        raise ValueError(
            "No matching genotypes found between marmo_results and marmo_genotype_label_map."
        )

    # Subset from long, not marmo_results: long has cleared the null and genotype filters, so it
    # names exactly the biomaterials behind plotted measurements.
    referenced = biomaterial[biomaterial["biomaterialid"].isin(long["biomaterialid"])]
    check_column_rules(
        {"marmo_biomaterial_metadata": referenced}, REFERENCED_BIOMATERIAL_RULES
    )

    long = long.merge(
        referenced[["biomaterialid", "collectionage"]],
        how="left",
        on="biomaterialid",
        validate="m:1",
    )
    long["collectionage"] = pd.to_numeric(long["collectionage"], errors="coerce")
    long = long.dropna(subset=["collectionage"])
    if long.empty:
        raise ValueError(
            "No matching numeric 'collectionage' values between marmo_results and "
            "marmo_biomaterial_metadata."
        )

    # Ages floor to the sample year: 9.9 months is "0-1 years", 13.0 is "1-2 years". age_start is
    # kept as the numeric sort key for the label.
    long["age_start"] = (long["collectionage"] // MONTHS_PER_YEAR).astype(int)
    year = long["age_start"]
    long["age"] = year.astype(str) + "-" + (year + 1).astype(str) + " years"
    long["sex"] = long["sex"].str.title()

    # m:1: duplicate result_column entries would multiply rows and make the y_axis_max grouping
    # ambiguous.
    long = long.merge(
        measure_info[
            [
                "result_column_std",
                "evidence_type",
                "units",
                "display_order",
            ]
        ],
        how="left",
        on="result_column_std",
        validate="m:1",
    )

    return long


def _drop_single_genotype_buckets(measurements: pd.DataFrame) -> pd.DataFrame:
    """Drop (evidence_type, age) buckets that do not compare at least two genotypes.

    A biomarker plot is only meaningful with two or more distinct display-label genotypes (e.g. a
    model and its matched control). Single-genotype buckets - common where longitudinal controls
    outlive the model animals - are removed. Filtering here, before y_axis_max is computed, keeps the
    axis scaled to the retained data.

    Args:
        measurements (pd.DataFrame): The per-measurement DataFrame, carrying display_label.

    Returns:
        pd.DataFrame: measurements limited to buckets with >= 2 unique display_label values.
    """
    genotype_counts = measurements.groupby(["evidence_type", "age"])[
        "display_label"
    ].transform("nunique")
    return measurements[genotype_counts >= 2]


def _fill_age_gaps(grouped: pd.DataFrame) -> pd.DataFrame:
    """Backfill missing age buckets with empty-data placeholders so each evidence type is contiguous.

    For each evidence_type, every whole-year bucket from "0-1 years" up to its oldest retained bucket
    should exist in the output whether it has data or not. Buckets that have no data get a placeholder
    object  (same name/units/display_order/y_axis_max, empty data). This keeps the app's per-age plots
    aligned after single-genotype buckets are dropped, so a bucket dropped from the start or middle of
    the series does not shift the ages that follow it. Trailing buckets beyond the oldest retained one
    are not added.

    Args:
        grouped (pd.DataFrame): One row per retained (evidence_type, age) bucket, with name, units,
            display_order, age_start, y_axis_max, and the nested data column.

    Returns:
        pd.DataFrame: grouped with placeholder rows appended for the missing buckets.
    """
    placeholders = []
    for evidence_type, group in grouped.groupby("evidence_type"):
        present = set(group["age_start"])
        template = group.iloc[0]
        for age_start in range(max(present) + 1):
            if age_start in present:
                continue
            placeholders.append(
                {
                    "name": template["name"],
                    "evidence_type": evidence_type,
                    "age": f"{age_start}-{age_start + 1} years",
                    "units": template["units"],
                    "display_order": template["display_order"],
                    "age_start": age_start,
                    "y_axis_max": template["y_axis_max"],
                    "data": [],
                }
            )
    if not placeholders:
        return grouped
    return pd.concat([grouped, pd.DataFrame(placeholders)], ignore_index=True)


def _build_biomarkers(
    measurements: pd.DataFrame, model_name: str
) -> List[Dict[str, Any]]:
    """Assemble one model's biomarkers collection from its measurements.

    One object per (evidence_type, age), sorted by display order then age ascending. Buckets that do
    not compare at least two genotypes are dropped, and any resulting age gap (from "0-1 years" up to
    the oldest retained bucket) is backfilled with an empty-data placeholder. y_axis_max is the
    per-evidence_type maximum across the retained ages, applied to every one of its buckets, as in the
    mouse immunohisto pipeline.

    Args:
        measurements (pd.DataFrame): The per-measurement DataFrame from _build_measurements.
        model_name (str): The model name to stamp on each biomarker object.

    Returns:
        List[Dict[str, Any]]: The sorted biomarkers collection.
    """
    # The caller passes one model's subset of the measurements, which is legitimately empty for a
    # model whose genotypes are all absent from the label map, or if all results failed QC.
    if measurements.empty:
        return []

    # Drop single-genotype buckets before computing y_axis_max so the axis fits the retained data.
    measurements = _drop_single_genotype_buckets(measurements)
    if measurements.empty:
        return []

    y_axis_max_map = {
        evidence_type: round_y_axis_max(group["value"].max())
        for evidence_type, group in measurements.groupby("evidence_type")
    }

    # Shape the data-point columns before nesting so nest_fields emits the output dicts directly.
    data_points = measurements.copy()
    data_points["individual_id"] = data_points["individualid"].astype(str)
    data_points["value"] = data_points["value"].astype(float)
    # Output genotype is the display label; drop the raw one first so the rename cannot collide.
    data_points = data_points.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype"}
    )
    # Sort on numeric individualid, not the string individual_id copy made above: as strings, animals would
    # order 1, 10, 2.
    data_points = data_points.sort_values(["individualid", "value"])

    # nest_fields emits dict keys in column order, so nest_cols order is the data-point key order.
    group_cols = ["evidence_type", "age", "units", "display_order", "age_start"]
    nest_cols = ["individual_id", "value", "sex", "genotype"]
    grouped = nest_fields(
        df=data_points[group_cols + nest_cols],
        grouping=group_cols,
        new_column="data",
        drop_columns=group_cols,
    )

    grouped["name"] = model_name
    grouped["y_axis_max"] = grouped["evidence_type"].map(y_axis_max_map).astype(float)

    # Backfill missing age buckets with metadata placeholders, then sort.
    grouped = _fill_age_gaps(grouped)
    grouped = grouped.sort_values(["display_order", "evidence_type", "age_start"])

    return grouped[
        ["name", "evidence_type", "age", "units", "y_axis_max", "data"]
    ].to_dict(orient="records")


def transform_marmo_details(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the marmoset source files into the marmo_details structured output for Model AD.

    Source files: marmo_model_metadata (syn76417166), marmo_genotype_label_map (syn76417167),
    marmo_biomarker_measure_info (syn76417168), marmo_individual_metadata (syn63926850),
    marmo_biomaterial_metadata (syn74444970), marmo_results (syn64133726).

    One output object per model in marmo_model_metadata, as transform_model_details does for mice.
    Measurements are associated with model info by matching their genotype in the
    marmo_genotype_label_map. Models with no matching genotype in the measurements data frame get
    an empty biomarkers list.

    Expected transformations:
        1. Measures that did not pass QC are converted to nulls, which are subsequently dropped.
        2. The wide marmo_results measure columns are melted long; all null measurements are dropped.
        3. Genotype and sex are joined per individual, then genotypes are mapped to display labels
           and models. Measurements with an unmapped genotype are excluded.
        4. Collection age is joined on biomaterialid; measurements with no record are dropped.
        5. Ages (months) are bucketed into whole-year ranges (e.g. "0-1 years"). Marmosets are
           sampled longitudinally and values are deliberately not averaged per animal, so one
           animal can contribute many points to a bucket - up to 15 in current data, unlike the
           mouse pipeline where an animal is one point.
        6. Only buckets up to the oldest bucket with data are emitted, per measure. Any bucket
           that contains data for only one genotype is dropped, then placeholder buckets are
           created for any missing age buckets before the oldest bucket with data. There is no
           guarantee that every measure will have the same oldest bucket with data, so each
           measure can have a different range of contiguous buckets on the same model page.
        7. Measure metadata (evidence_type, units, display_order) is attached, and y_axis_max is
           computed per model via round_y_axis_max.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        List[Dict[str, Any]]: One model detail dictionary per model in marmo_model_metadata.

    Raises:
        ValueError: If required datasets or columns are missing, if any column violates
            COLUMN_RULES, if a model has inconsistent model_type or study_synid values, if
            marmo_genotype_label_map has duplicate (model, genotype) rows or names a model
            absent from marmo_model_metadata, or if no measurement survives the value,
            genotype, or collection-age filters.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, COLUMN_RULES)

    metadata = _validate_and_prepare_model_metadata(
        datasets["marmo_genotype_label_map"], datasets["marmo_model_metadata"]
    )
    measure_info = _prepare_measure_info(datasets["marmo_biomarker_measure_info"])
    measurements = _build_measurements(datasets, measure_info)

    result = []
    for model_name in metadata["model"].unique():
        model_rows = metadata[metadata["model"] == model_name]
        # A model gets one row per modified gene, but all rows contain the same model-level information
        # (model_type, study_synid). We can safely take the first row to extract these fields.
        model_row = model_rows.iloc[0]
        model_measurements = measurements[measurements["model"] == model_name]
        biomarkers = _build_biomarkers(model_measurements, model_name)
        genetic_info = model_rows[
            ["modified_gene", "ensembl_gene_id", "allele_type"]
        ].to_dict(orient="records")

        result.append(
            {
                "name": model_name,
                "model_type": model_row["model_type"],
                "study_synid": model_row["study_synid"],
                "genetic_info": genetic_info,
                "biomarkers": biomarkers,
            }
        )

    return result
