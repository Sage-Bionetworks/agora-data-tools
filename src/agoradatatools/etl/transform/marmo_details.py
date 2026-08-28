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
    check_column_rules,
    check_required_datasets_and_columns,
    nest_fields,
    round_y_axis_max,
    standardize_column_name,
)


REQUIRED_INPUT = {
    "marmo_metadata": [
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
    ],
}

# marmo_biomaterial_metadata describes every biomaterial collected for the study, not just the
# MSD plasma samples this transform consumes, so its rules here are limited to ones that hold
# file-wide. NumericRule and NonNegativeRule both skip nulls, so they tolerate the assays that
# record no age. NonNegativeRule belongs here rather than in REFERENCED_BIOMATERIAL_RULES because
# a negative age is impossible for any assay, not only for the rows this transform reads.
# A NotEmptyRule on biomaterialid would not hold file-wide: the file carries a row with only
# modelSystemType populated. That blank id is harmless because it can never match a results id,
# and the meaningful presence check already lives on marmo_results.biomaterialid, which drives
# the join.

COLUMN_RULES = {
    "marmo_metadata": {
        "model": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(r"^ENSCJAG\d+$")],
    },
    "marmo_genotype_label_map": {
        "model": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "display_label": [NotEmptyRule()],
    },
    # result_column feeds standardize_column_name, where a null raises a bare TypeError from
    # re.sub instead of a ValueError naming the file. evidence_type and display_order are
    # nest_fields grouping keys, and pandas groupby drops null keys, so an unvalidated bad value
    # in either deletes that entire measure from every model page with no error at all.
    # display_order additionally needs NumericRule because to_numeric coerces an unparseable
    # value to NaN, which NotEmptyRule cannot see. units is deliberately left unvalidated: it is
    # legitimately blank for the A-beta ratio and is filled with an empty string before grouping.
    "marmo_biomarker_measure_info": {
        "result_column": [NotEmptyRule()],
        "evidence_type": [NotEmptyRule()],
        "display_order": [NotEmptyRule(), NumericRule()],
    },
    "marmo_individual_metadata": {
        "individualid": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "sex": [NotEmptyRule()],
    },
    "marmo_biomaterial_metadata": {
        "collectionage": [NumericRule(), NonNegativeRule()],
    },
    "marmo_results": {
        "biomaterialid": [NotEmptyRule()],
        "individualid": [NotEmptyRule()],
    },
}

# Rules checked against only the biomaterial rows that marmo_results references, applied after
# that subset is taken in _build_measurements.
#
# collectionageunits: ages are bucketed assuming months, so any other unit must fail. OneOfRule
# counts nulls as violations, and the file leaves collectionAgeUnits blank on assays that record
# no age, so this cannot be checked file-wide. Every referenced row carries "months" today.
REFERENCED_BIOMATERIAL_RULES = {
    "marmo_biomaterial_metadata": {
        "collectionageunits": [OneOfRule({"months"})],
    },
}

MONTHS_PER_YEAR = 12


def _build_measurements(
    datasets: Dict[str, pd.DataFrame],
    measure_info: pd.DataFrame,
) -> pd.DataFrame:
    """Build the per-measurement DataFrame used to assemble the biomarkers collection.

    Melts the wide marmo_results measure columns into long form, joins individual metadata,
    resolves genotypes to display labels and models, joins biomaterial collection ages, and
    attaches the measure metadata. Measurements whose genotype is absent from the label map,
    or whose biomaterial has no record, are dropped.

    Args:
        datasets (Dict[str, pd.DataFrame]): The input datasets.
        measure_info (pd.DataFrame): The measure-info DataFrame with a standardized
            "result_column_std" column added.

    Returns:
        pd.DataFrame: One row per surfaced measurement with the columns needed to build the
        biomarkers collection, including a model column from the label map.

    Raises:
        ValueError: If a referenced biomaterial row violates REFERENCED_BIOMATERIAL_RULES, if no
            measurement matches a label-map genotype, or if no measurement retains a collection
            age after the biomaterial join.
    """
    results = datasets["marmo_results"]
    individual = datasets["marmo_individual_metadata"]
    biomaterial = datasets["marmo_biomaterial_metadata"]
    genotype_map = datasets["marmo_genotype_label_map"]

    # Fail loudly on a missing or typo'd result_column instead of silently dropping that measure.
    measure_columns = list(measure_info["result_column_std"])
    missing_columns = [col for col in measure_columns if col not in results.columns]
    if missing_columns:
        raise ValueError(
            "marmo_biomarker_measure_info references result columns that are not present in "
            f"marmo_results: {missing_columns}"
        )

    long = results.melt(
        id_vars=["biomaterialid", "individualid"],
        value_vars=measure_columns,
        var_name="result_column_std",
        value_name="value",
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])

    # validate="m:1" fails loudly if an individual appears more than once, which would otherwise
    # silently duplicate every measurement for that individual.
    long = long.merge(
        individual[["individualid", "genotype", "sex"]],
        how="left",
        on="individualid",
        validate="m:1",
    )
    # Both remaining joins can empty the frame. An empty result means the files no longer share
    # a vocabulary rather than that there is genuinely nothing to plot, so each is checked the
    # same way: emptiness is only an error if there was something to surface to begin with.
    had_measurements = not long.empty

    # Join on genotype only, so a genotype (e.g. WT) may belong to more than one model and those
    # measurements are copied onto every model that lists it. validate="m:m" records that intent
    # but enforces nothing - pandas performs no check for m:m. The uniqueness check in
    # transform_marmo_details is what prevents points being multiplied within a model. This
    # assumes a genotype string identifies the same animals everywhere: if a later study reused
    # one for a different cohort, those animals would appear on every model listing it.
    long = long.merge(
        genotype_map[["model", "genotype", "display_label"]],
        how="inner",
        on="genotype",
        validate="m:m",
    )
    if had_measurements and long.empty:
        raise ValueError(
            "No marmo_results measurement matched a marmo_genotype_label_map genotype. "
            "Check that genotype values still agree between marmo_individual_metadata and "
            "marmo_genotype_label_map."
        )

    # marmo_results and marmo_biomaterial_metadata share the biomaterialID vocabulary directly,
    # so the age join is a plain key join. The biomaterial file also describes assays this
    # transform never surfaces (ddPCR, rnaSeq, and others), so narrow it to the referenced rows
    # before validating the rules that only hold for what is consumed.
    referenced = biomaterial[
        biomaterial["biomaterialid"].isin(results["biomaterialid"])
    ]
    check_column_rules(
        {"marmo_biomaterial_metadata": referenced}, REFERENCED_BIOMATERIAL_RULES
    )

    # validate="m:1": one collection age per biomaterial, so duplicate biomaterial rows cannot
    # duplicate measurements.
    long = long.merge(
        referenced[["biomaterialid", "collectionage"]],
        how="left",
        on="biomaterialid",
        validate="m:1",
    )
    long["collectionage"] = pd.to_numeric(long["collectionage"], errors="coerce")
    retained = long.dropna(subset=["collectionage"])
    # An empty result here means the two files no longer share an id vocabulary rather than that
    # every sample is genuinely unrecorded. Without this guard that produces empty biomarkers
    # collections instead of an error, which is how the previous id mismatch went unnoticed.
    if had_measurements and retained.empty:
        raise ValueError(
            "No marmo_results measurement matched a marmo_biomaterial_metadata record with a "
            "collection age. Check that biomaterialid values still agree between the two files."
        )
    long = retained

    # Ages are floored to the year the sample was taken, so 9.9 months is "0-1 years" and 13.0
    # is "1-2 years". age_start is kept as the numeric sort key for the label.
    long["age_start"] = (long["collectionage"] // MONTHS_PER_YEAR).astype(int)
    year = long["age_start"]
    long["age"] = year.astype(str) + "-" + (year + 1).astype(str) + " years"
    long["sex"] = long["sex"].str.title()

    # validate="m:1": one metadata row per result column, so duplicate result_column entries
    # can't multiply rows or make the y_axis_max grouping ambiguous.
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


def _build_biomarkers(
    measurements: pd.DataFrame, model_name: str
) -> List[Dict[str, Any]]:
    """Assemble the biomarkers collection for a model from the per-measurement DataFrame.

    Produces one object per (evidence_type, age) combination, each containing the data points
    for that plot. Objects are sorted by the measure display order, then by age ascending.

    The y_axis_max for an evidence_type is derived from its maximum surfaced value across all
    ages and applied to every one of its age buckets, mirroring how the mouse immunohisto
    pipeline derives its plot axis maxima.

    Args:
        measurements (pd.DataFrame): The per-measurement DataFrame from _build_measurements.
        model_name (str): The model name to stamp on each biomarker object.

    Returns:
        List[Dict[str, Any]]: The sorted biomarkers collection.
    """
    if measurements.empty:
        return []

    y_axis_max_map = {
        evidence_type: round_y_axis_max(group["value"].max())
        for evidence_type, group in measurements.groupby("evidence_type")
    }

    # Shape the data-point columns into their final form before nesting so nest_fields emits the
    # output dicts directly, with no per-row rebuild needed.
    data_points = measurements.copy()
    data_points["individual_id"] = data_points["individualid"].astype(str)
    data_points["value"] = data_points["value"].astype(float)
    # The output "genotype" is the display label; drop the raw genotype (from the individual join)
    # first so the rename does not collide with it.
    data_points = data_points.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype"}
    )
    # Sort on the numeric source column rather than the stringified individual_id, which would
    # order animals 1, 10, 2 instead of 1, 2, 10.
    data_points = data_points.sort_values(["individualid", "value"])

    keep_columns = {"individual_id", "value", "sex", "genotype"}
    grouped = nest_fields(
        df=data_points,
        grouping=[
            "evidence_type",
            "age",
            "units",
            "display_order",
            "age_start",
        ],
        new_column="data",
        drop_columns=set(data_points.columns) - keep_columns,
    )

    grouped = grouped.sort_values(["display_order", "age_start"])

    biomarkers = []
    for _, row in grouped.iterrows():
        biomarkers.append(
            {
                "name": model_name,
                "evidence_type": row["evidence_type"],
                "age": row["age"],
                "units": row["units"],
                "y_axis_max": float(y_axis_max_map[row["evidence_type"]]),
                "data": row["data"],
            }
        )

    return biomarkers


def transform_marmo_details(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the marmoset source files into the marmo_details structured output for Model AD.

    Source files: marmo_metadata (syn76417166), marmo_genotype_label_map (syn76417167),
    marmo_biomarker_measure_info (syn76417168), marmo_individual_metadata (syn63926850),
    marmo_biomaterial_metadata (syn74444970), marmo_results (syn64133726).

    One output object is produced per distinct model in marmo_metadata, matching how
    transform_model_details loops mouse models. A measurement is associated with a model
    by joining its genotype to marmo_genotype_label_map (which has a model column, like
    the mouse genotype_label_map). Shared control genotypes listed under multiple models
    appear on each of those model pages. A metadata model with no matching label-map rows
    gets an empty biomarkers list. Label-map models absent from metadata produce no output
    entry.

    Expected transformations:
        1. The wide marmo_results measure columns are melted into long form; null measurements
           are dropped.
        2. Each measurement is joined to its individual's genotype and sex; genotypes are mapped
           to display labels and models via marmo_genotype_label_map. Measurements whose
           genotype is not in the label map are excluded.
        3. Each measurement's collection age is joined from the biomaterial record on
           biomaterialid. Measurements with no biomaterial record are dropped.
        4. Collection ages (months) are bucketed into whole-year ranges (e.g. "0-1 years").
           Marmosets are sampled longitudinally, so values are deliberately not averaged per
           animal: one animal contributes one point per collection falling in a bucket, and in
           current data can account for as many as 15 points in one plot. This is where the
           transform diverges from the mouse immunohisto pipeline, where each animal is one
           point.
        5. A bucket is only emitted where that measure has data. Unlike the mouse pipeline,
           which calls _add_missing_age_entries to give every measure every age, no placeholder
           entries are added here, so measures with different coverage produce different bucket
           sets on the same model page.
        6. Measure metadata (evidence_type, units, display_order) is attached from
           marmo_biomarker_measure_info. y_axis_max is computed per model from the data
           (per-measure maximum rounded up via round_y_axis_max). Each model's biomarkers
           collection contains one object per (evidence_type, age), sorted by display order
           then age ascending.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        List[Dict[str, Any]]: A list of model detail dictionaries (one per model in
        marmo_metadata).

    Raises:
        ValueError: If required datasets or columns are missing, if any column violates
            COLUMN_RULES, if marmo_genotype_label_map has duplicate (model, genotype) rows, or
            if no measurement survives either the marmo_genotype_label_map join or the
            marmo_biomaterial_metadata join.
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, COLUMN_RULES)

    genotype_map = datasets["marmo_genotype_label_map"]
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

    metadata = datasets["marmo_metadata"]

    measure_info = datasets["marmo_biomarker_measure_info"].copy()
    measure_info["result_column_std"] = measure_info["result_column"].apply(
        standardize_column_name
    )
    measure_info["display_order"] = pd.to_numeric(
        measure_info["display_order"], errors="coerce"
    )
    # The A-beta ratio has no units; represent missing units as an empty string, not null
    measure_info["units"] = measure_info["units"].fillna("")

    measurements = _build_measurements(datasets, measure_info)

    # Loop each model in metadata the same way transform_model_details does for mice.
    result = []
    for model_name in metadata["model"].unique():
        model_rows = metadata[metadata["model"] == model_name]
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
