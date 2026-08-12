"""
This module contains the transformation logic for the marmo_details dataset.
This is for the Model AD project (marmoset details pages).
"""

from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.utils import (
    MatchesRegexRule,
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
    "marmo_biospecimen_metadata": [
        "specimenid",
        "samplingage",
        "samplingageunits",
    ],
    "marmo_results": [
        "biomaterialid",
        "individualid",
    ],
}

# Per-column content rules validated up front so bad source data fails loudly rather than
# silently dropping or miscomputing rows downstream.
#   - samplingageunits: ages are bucketed assuming months, so any other unit must fail.
#   - samplingage: must be numeric so age bucketing is well-defined.
#   - ensembl_gene_id: marmoset (Callithrix jacchus) Ensembl gene ids are ENSCJAG-prefixed.
#   - NotEmpty rules guard the id/label/order columns whose absence would corrupt joins or output.
COLUMN_RULES = {
    "marmo_metadata": {
        "model": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(r"^ENSCJAG\d+$")],
    },
    "marmo_genotype_label_map": {
        "genotype": [NotEmptyRule()],
        "display_label": [NotEmptyRule()],
    },
    "marmo_biomarker_measure_info": {
        "display_order": [NotEmptyRule()],
    },
    "marmo_individual_metadata": {
        "individualid": [NotEmptyRule()],
        "genotype": [NotEmptyRule()],
        "sex": [NotEmptyRule()],
    },
    "marmo_biospecimen_metadata": {
        "specimenid": [NotEmptyRule()],
        "samplingage": [NumericRule()],
        "samplingageunits": [OneOfRule({"months"})],
    },
    "marmo_results": {
        "biomaterialid": [NotEmptyRule()],
        "individualid": [NotEmptyRule()],
    },
}

# Number of months used to bucket sampling ages into whole-year ranges.
MONTHS_PER_YEAR = 12


def _convert_to_year(sampling_age_months: float) -> int:
    """Floor a sampling age in months to the whole year in which the sample was taken.

    Shared by the bucket label and the numeric sort key so the flooring logic lives in one place.

    Args:
        sampling_age_months (float): The animal's age in months at the time of sampling.

    Returns:
        int: The floored whole-year value, e.g. 9.9 -> 0, 13.0 -> 1.
    """
    return int(sampling_age_months // MONTHS_PER_YEAR)


def _age_to_year_bucket(sampling_age_months: float) -> str:
    """Convert a sampling age in months into a whole-year bucket label.

    Ages are floored to the year in which the sample was taken, e.g. 9.9 months -> "0-1 years",
    13.0 months -> "1-2 years".

    Args:
        sampling_age_months (float): The animal's age in months at the time of sampling.

    Returns:
        str: The bucket label, e.g. "2-3 years".
    """
    bucket_start = _convert_to_year(sampling_age_months)
    return f"{bucket_start}-{bucket_start + 1} years"


def _build_measurements(
    datasets: Dict[str, pd.DataFrame],
    measure_info: pd.DataFrame,
) -> pd.DataFrame:
    """Build the per-measurement DataFrame used to assemble the biomarkers collection.

    Melts the wide marmo_results measure columns into long form, joins individual metadata
    (genotype, sex), maps genotypes to their display labels (dropping any genotype not present
    in the label map), joins biospecimen sampling ages (dropping rows without a biospecimen
    record), and attaches the measure metadata (evidence_type, units, display_order).

    Args:
        datasets (Dict[str, pd.DataFrame]): The input datasets.
        measure_info (pd.DataFrame): The measure-info DataFrame with a standardized
            "result_column_std" column added.

    Returns:
        pd.DataFrame: One row per surfaced measurement with the columns needed to build the
        biomarkers collection.
    """
    results = datasets["marmo_results"]
    individual = datasets["marmo_individual_metadata"]
    biospecimen = datasets["marmo_biospecimen_metadata"]
    genotype_map = datasets["marmo_genotype_label_map"]

    # Every result_column listed in the measure-info mapping must exist in marmo_results.
    # Fail loudly on a missing/typo'd column instead of silently dropping that measure.
    missing_columns = [
        col for col in measure_info["result_column_std"] if col not in results.columns
    ]
    if missing_columns:
        raise ValueError(
            "marmo_biomarker_measure_info references result columns that are not present in "
            f"marmo_results: {missing_columns}"
        )

    measure_columns = list(measure_info["result_column_std"])

    long = results.melt(
        id_vars=["biomaterialid", "individualid"],
        value_vars=measure_columns,
        var_name="result_column_std",
        value_name="value",
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])

    # Attach genotype + sex, then resolve genotype display label (unmapped genotypes are dropped).
    # validate="m:1" fails loudly if an individual appears more than once, which would otherwise
    # silently duplicate every measurement for that individual.
    long = long.merge(
        individual[["individualid", "genotype", "sex"]],
        how="left",
        on="individualid",
        validate="m:1",
    )
    # validate="m:1": the label map must have one display label per genotype; duplicate genotype
    # keys would multiply rows.
    long = long.merge(
        genotype_map[["genotype", "display_label"]],
        how="inner",
        on="genotype",
        validate="m:1",
    )

    # Attach sampling age from the biospecimen record; drop measurements with no biospecimen match.
    # validate="m:1": one sampling age per specimen; duplicate specimen rows would duplicate
    # measurements.
    long = long.merge(
        biospecimen[["specimenid", "samplingage"]].rename(
            columns={"specimenid": "biomaterialid"}
        ),
        how="left",
        on="biomaterialid",
        validate="m:1",
    )
    long["samplingage"] = pd.to_numeric(long["samplingage"], errors="coerce")
    long = long.dropna(subset=["samplingage"])

    long["age"] = long["samplingage"].apply(_age_to_year_bucket)
    long["age_start"] = long["samplingage"].apply(_convert_to_year)
    long["sex"] = long["sex"].str.title()

    # Attach measure metadata. validate="m:1": one metadata row per result column, so duplicate
    # result_column entries can't multiply rows or make the y_axis_max grouping ambiguous.
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

    The y_axis_max for each measure is computed from the data: the maximum surfaced value for
    an evidence_type (across all ages) is rounded up to a "nice" number via round_y_axis_max,
    mirroring how the mouse immunohisto pipeline derives its plot axis maxima. The same
    y_axis_max is applied to every age bucket of a given evidence_type.

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
    # output dicts directly (individual_id, value, sex, genotype) with no per-row rebuild needed.
    data_points = measurements.copy()
    data_points["individual_id"] = data_points["individualid"].astype(str)
    data_points["value"] = data_points["value"].astype(float)
    # The output "genotype" is the display label; drop the raw genotype (from the individual join)
    # first so the rename does not collide with it. errors="ignore" keeps this a no-op when the
    # caller already supplies a display_label-only frame.
    data_points = data_points.drop(columns=["genotype"], errors="ignore").rename(
        columns={"display_label": "genotype"}
    )
    data_points = data_points.sort_values(["individual_id", "value"])

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
    marmo_biospecimen_metadata (syn63927118), marmo_results (syn64133726).

    Expected transformations:
        1. The wide marmo_results measure columns are melted into long form; null measurements
           are dropped.
        2. Each measurement is joined to its individual's genotype and sex; genotypes are mapped
           to display labels via marmo_genotype_label_map. Measurements whose genotype is not in
           the label map are excluded.
        3. Each measurement's sampling age is joined from the biospecimen record on
           biomaterialid == specimenid. Measurements with no biospecimen record are dropped.
        4. Sampling ages (months) are bucketed into whole-year ranges (e.g. "0-1 years").
        5. Measure metadata (evidence_type, units, display_order) is attached from
           marmo_biomarker_measure_info. y_axis_max is computed from the data (per-measure
           maximum rounded up via round_y_axis_max). The biomarkers collection contains one
           object per (evidence_type, age), sorted by display order then age ascending.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary of dataset names mapped to their DataFrame.
        required_input (Dict[str, List[str]]): Dictionary of required input datasets and columns.

    Returns:
        List[Dict[str, Any]]: A list of model detail dictionaries (one per model).

    Raises:
        ValueError: If required datasets or columns are missing, or if more than one model is
            present in marmo_metadata (multi-model output is not yet supported).
    """
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, COLUMN_RULES)

    metadata = datasets["marmo_metadata"]

    # TODO: generalize to multiple models. The blocker is data linkage: outside marmo_metadata
    # there is no model column on the label map, individual, biospecimen, or results files, so a
    # measurement is tied to a model only implicitly via the genotype label map. Generalizing
    # requires the data team to decide how each genotype/individual/measurement maps to a model
    # (e.g. a model column on marmo_genotype_label_map). Until then, fail loudly on multi-model
    # input rather than silently attributing all measurements to one model.
    model_names = metadata["model"].unique().tolist()
    if len(model_names) != 1:
        raise ValueError(
            "marmo_details currently supports exactly one model in marmo_metadata; "
            f"found {len(model_names)}: {model_names}"
        )
    model_name = model_names[0]

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
    biomarkers = _build_biomarkers(measurements, model_name)

    model_row = metadata[metadata["model"] == model_name].iloc[0]
    genetic_info = metadata[metadata["model"] == model_name][
        ["modified_gene", "ensembl_gene_id", "allele_type"]
    ].to_dict(orient="records")

    model_entry = {
        "name": model_name,
        "model_type": model_row["model_type"],
        "study_synid": model_row["study_synid"],
        "genetic_info": genetic_info,
        "biomarkers": biomarkers,
    }

    return [model_entry]
