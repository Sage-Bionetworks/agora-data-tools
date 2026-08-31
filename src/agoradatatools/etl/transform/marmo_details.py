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
    require_survivors,
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

# marmo_biomaterial_metadata covers every biomaterial in the study, not just the MSD plasma rows
# this transform reads, so only file-wide rules live here - nulls pass NumericRule and
# NonNegativeRule, which is what lets the no-age assays through. biomaterialid gets no
# NotEmptyRule: one row carries only modelSystemType, and a blank id can never match a results id.

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
    # evidence_type and display_order are nest_fields grouping keys and pandas drops null keys, so
    # a bad value silently deletes that measure from every model page. display_order also needs
    # NumericRule: to_numeric coerces junk to NaN, which NotEmptyRule cannot see. units is
    # unvalidated because it is legitimately blank for the A-beta ratio.
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

# Ages are bucketed as months, so any other unit must fail. OneOfRule counts nulls as violations
# and the no-age assays leave the unit blank, so this can only run on the consumed subset.
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
    """Build the per-measurement DataFrame behind the biomarkers collection.

    Melts the wide measure columns, resolves genotypes to display labels and models, joins
    collection ages, and attaches measure metadata. Measurements with no label-map genotype or
    no biomaterial record are dropped.

    Args:
        datasets (Dict[str, pd.DataFrame]): The input datasets.
        measure_info (pd.DataFrame): Measure info with a standardized "result_column_std" column.

    Returns:
        pd.DataFrame: One row per surfaced measurement, including its model from the label map.

    Raises:
        ValueError: If marmo_biomarker_measure_info names a result column absent from
            marmo_results, if a consumed biomaterial row violates REFERENCED_BIOMATERIAL_RULES,
            or if no measurement survives the value, genotype, or collection-age filters.
    """
    results = datasets["marmo_results"]
    individual = datasets["marmo_individual_metadata"]
    biomaterial = datasets["marmo_biomaterial_metadata"]
    genotype_map = datasets["marmo_genotype_label_map"]

    # A typo'd result_column would otherwise drop that measure silently.
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
    # An empty marmo_results and a measure_info listing no measures both leave long empty, so
    # require_survivors passes them while still rejecting an all-blank measure column.
    long = require_survivors(
        long,
        long.dropna(subset=["value"]),
        "No marmo_results row carries a numeric value in any column listed in "
        "marmo_biomarker_measure_info. Check that the measure columns still hold data.",
    )

    # m:1: a duplicated individual would silently duplicate all of its measurements.
    long = long.merge(
        individual[["individualid", "genotype", "sex"]],
        how="left",
        on="individualid",
        validate="m:1",
    )
    # Joining on genotype alone fans a shared genotype (WT) out to every model listing it. pandas
    # enforces nothing for m:m, so the (model, genotype) uniqueness check in the caller is what
    # keeps points from multiplying within a model. Assumes a genotype string means the same
    # animals everywhere.
    long = require_survivors(
        long,
        long.merge(
            genotype_map[["model", "genotype", "display_label"]],
            how="inner",
            on="genotype",
            validate="m:m",
        ),
        "No marmo_results measurement matched a marmo_genotype_label_map genotype. "
        "Check that genotype values still agree between marmo_individual_metadata and "
        "marmo_genotype_label_map.",
    )

    # Subset from long, not marmo_results: long has cleared the null and genotype filters, so it
    # names exactly the biomaterials behind plotted measurements.
    referenced = biomaterial[biomaterial["biomaterialid"].isin(long["biomaterialid"])]
    check_column_rules(
        {"marmo_biomaterial_metadata": referenced}, REFERENCED_BIOMATERIAL_RULES
    )

    # m:1: duplicate biomaterial rows must not duplicate measurements.
    long = long.merge(
        referenced[["biomaterialid", "collectionage"]],
        how="left",
        on="biomaterialid",
        validate="m:1",
    )
    long["collectionage"] = pd.to_numeric(long["collectionage"], errors="coerce")
    long = require_survivors(
        long,
        long.dropna(subset=["collectionage"]),
        "No marmo_results measurement matched a marmo_biomaterial_metadata record with a "
        "collection age. Check that biomaterialid values still agree between the two files.",
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


def _build_biomarkers(
    measurements: pd.DataFrame, model_name: str
) -> List[Dict[str, Any]]:
    """Assemble one model's biomarkers collection from its measurements.

    One object per (evidence_type, age), sorted by display order then age ascending. y_axis_max is
    the per-evidence_type maximum across all ages, applied to every one of its buckets, as in the
    mouse immunohisto pipeline.

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

    # Shape the data-point columns before nesting so nest_fields emits the output dicts directly.
    data_points = measurements.copy()
    data_points["individual_id"] = data_points["individualid"].astype(str)
    data_points["value"] = data_points["value"].astype(float)
    # Output genotype is the display label; drop the raw one first so the rename cannot collide.
    data_points = data_points.drop(columns=["genotype"]).rename(
        columns={"display_label": "genotype"}
    )
    # Sort on the numeric id, not the stringified one, which would order animals 1, 10, 2.
    data_points = data_points.sort_values(["individualid", "value"])

    # nest_fields emits dict keys in column order. Deriving other_columns by subtraction keeps any
    # column added upstream out of the data points by default.
    keep_columns = ["individual_id", "value", "sex", "genotype"]
    other_columns = [col for col in data_points.columns if col not in keep_columns]
    data_points = data_points[keep_columns + other_columns]

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
        drop_columns=other_columns,
    )

    # evidence_type is only a tiebreaker: it matters when two measures share a display_order,
    # where sorting on age alone would interleave them.
    grouped = grouped.sort_values(["display_order", "evidence_type", "age_start"])

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

    One output object per model in marmo_metadata, as transform_model_details does for mice.
    Measurements reach a model through their genotype in marmo_genotype_label_map, so a shared
    control genotype appears on every model listing it, and a model with no mapped genotypes gets
    an empty biomarkers list.

    Expected transformations:
        1. The wide marmo_results measure columns are melted long; null measurements are dropped.
        2. Genotype and sex are joined per individual, then genotypes are mapped to display labels
           and models. Measurements with an unmapped genotype are excluded.
        3. Collection age is joined on biomaterialid; measurements with no record are dropped.
        4. Ages (months) are bucketed into whole-year ranges (e.g. "0-1 years"). Marmosets are
           sampled longitudinally and values are deliberately not averaged per animal, so one
           animal can contribute many points to a bucket - up to 15 in current data, unlike the
           mouse pipeline where an animal is one point.
        5. Only buckets with data are emitted. There is no _add_missing_age_entries equivalent, so
           measures with different coverage produce different bucket sets on one model page.
        6. Measure metadata (evidence_type, units, display_order) is attached, and y_axis_max is
           computed per model via round_y_axis_max.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        List[Dict[str, Any]]: One model detail dictionary per model in marmo_metadata.

    Raises:
        ValueError: If required datasets or columns are missing, if any column violates
            COLUMN_RULES, if marmo_genotype_label_map has duplicate (model, genotype) rows or
            names a model absent from marmo_metadata, or if no measurement survives the value,
            genotype, or collection-age filters.
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

    # Hand-maintained files: a label-map model absent from marmo_metadata is a typo that would
    # silently empty the real model page. The inverse is fine - a model can precede its genotypes.
    unknown_models = sorted(set(genotype_map["model"]) - set(metadata["model"]))
    if unknown_models:
        raise ValueError(
            "marmo_genotype_label_map references models that are not present in "
            f"marmo_metadata: {unknown_models}"
        )

    measure_info = datasets["marmo_biomarker_measure_info"].copy()
    measure_info["result_column_std"] = measure_info["result_column"].apply(
        standardize_column_name
    )
    measure_info["display_order"] = pd.to_numeric(
        measure_info["display_order"], errors="coerce"
    )
    # The A-beta ratio has no units; empty string rather than null.
    measure_info["units"] = measure_info["units"].fillna("")

    measurements = _build_measurements(datasets, measure_info)

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
