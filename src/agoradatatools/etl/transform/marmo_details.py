"""
This module contains the transformation logic for the marmo_details dataset.
This is for the Model AD project (marmoset details pages).
"""

import re
from typing import Any, Dict, List

import pandas as pd

from agoradatatools.etl.transform.immunohisto_transform import round_y_axis_max
from agoradatatools.etl.utils import (
    check_required_datasets_and_columns,
    nest_fields,
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
    ],
    "marmo_results": [
        "biomaterialid",
        "individualid",
    ],
}

# Number of months used to bucket sampling ages into whole-year ranges.
MONTHS_PER_YEAR = 12


def _standardize_result_column(name: str) -> str:
    """Normalize a measure result column name the same way the ETL harness standardizes
    dataframe column headers, so measure-info result_column values match the standardized
    columns of the marmo_results dataset.

    Mirrors agoradatatools.etl.utils.standardize_column_names: strip a set of special
    characters, replace spaces / hyphens / periods with underscores, and lowercase.

    Args:
        name (str): A raw result column name, e.g. "Ab40_pg.ml".

    Returns:
        str: The standardized column name, e.g. "ab40_pg_ml".
    """
    name = re.sub(r"[#@&*^?()%$!/]", "", name)
    name = re.sub(r"[ \-.]", "_", name)
    return name.lower()


def _age_to_year_bucket(sampling_age_months: float) -> str:
    """Convert a sampling age in months into a whole-year bucket label.

    Ages are floored to the year in which the sample was taken, e.g. 9.9 months -> "0-1 years",
    13.0 months -> "1-2 years".

    Args:
        sampling_age_months (float): The animal's age in months at the time of sampling.

    Returns:
        str: The bucket label, e.g. "2-3 years".
    """
    bucket_start = int(sampling_age_months // MONTHS_PER_YEAR)
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

    measure_columns = [
        col for col in measure_info["result_column_std"] if col in results.columns
    ]

    long = results.melt(
        id_vars=["biomaterialid", "individualid"],
        value_vars=measure_columns,
        var_name="result_column_std",
        value_name="value",
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])

    # Attach genotype + sex, then resolve genotype display label (unmapped genotypes are dropped)
    long = long.merge(
        individual[["individualid", "genotype", "sex"]],
        how="left",
        on="individualid",
    )
    long = long.merge(
        genotype_map[["genotype", "display_label"]],
        how="inner",
        on="genotype",
    )

    # Attach sampling age from the biospecimen record; drop measurements with no biospecimen match
    long = long.merge(
        biospecimen[["specimenid", "samplingage"]].rename(
            columns={"specimenid": "biomaterialid"}
        ),
        how="left",
        on="biomaterialid",
    )
    long["samplingage"] = pd.to_numeric(long["samplingage"], errors="coerce")
    long = long.dropna(subset=["samplingage"])

    long["age"] = long["samplingage"].apply(_age_to_year_bucket)
    long["age_start"] = (long["samplingage"] // MONTHS_PER_YEAR).astype(int)
    long["sex"] = long["sex"].str.title()

    # Attach measure metadata
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

    data_points = measurements.copy()
    data_points["individual_id"] = data_points["individualid"].astype(str)
    data_points = data_points.sort_values(["individual_id", "value"])

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
        drop_columns=[
            col
            for col in data_points.columns
            if col not in ["individual_id", "value", "sex", "display_label"]
        ],
    )

    grouped = grouped.sort_values(["display_order", "age_start"])

    biomarkers = []
    for _, row in grouped.iterrows():
        data = [
            {
                "individual_id": point["individual_id"],
                "value": float(point["value"]),
                "sex": point["sex"],
                "genotype": point["display_label"],
            }
            for point in row["data"]
        ]
        biomarkers.append(
            {
                "name": model_name,
                "evidence_type": row["evidence_type"],
                "age": row["age"],
                "units": row["units"],
                "y_axis_max": float(y_axis_max_map[row["evidence_type"]]),
                "data": data,
            }
        )

    return biomarkers


def transform_marmo_details(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> List[Dict[str, Any]]:
    """
    Transforms the marmoset source files into the marmo_details structured output for Model AD.

    Source files: marmo_metadata, marmo_genotype_label_map, marmo_biomarker_measure_info,
    marmo_individual_metadata (syn63926850), marmo_biospecimen_metadata (syn63927118),
    marmo_results (syn64133726).

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

    metadata = datasets["marmo_metadata"]

    model_names = metadata["model"].unique().tolist()
    if len(model_names) != 1:
        raise ValueError(
            "marmo_details currently supports exactly one model in marmo_metadata; "
            f"found {len(model_names)}: {model_names}"
        )
    model_name = model_names[0]

    measure_info = datasets["marmo_biomarker_measure_info"].copy()
    measure_info["result_column_std"] = measure_info["result_column"].apply(
        _standardize_result_column
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
