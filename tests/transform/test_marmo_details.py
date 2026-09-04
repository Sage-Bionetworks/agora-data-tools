import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import (
    _build_biomarkers,
    _build_measurements,
    transform_marmo_details,
)


# The measurement columns carried by the marmo_results fixture.
MEASURE_COLUMNS = ["ab40_pg_ml", "ab_ratio", "gfap_pg_ml"]


# Each of these helper functions creates a dataset that causes _build_measurements to produce an
# empty data frame in different ways.
def _blank_measure_columns(datasets, columns):
    for column in columns:
        datasets["marmo_results"][column] = None


def _unmatch_label_map_genotypes(datasets):
    label_map = datasets["marmo_genotype_label_map"]
    datasets["marmo_genotype_label_map"] = label_map.assign(
        genotype=label_map["genotype"] + "_unmatched"
    )


def _unmatch_biomaterial_ids(datasets):
    biomaterial = datasets["marmo_biomaterial_metadata"]
    datasets["marmo_biomaterial_metadata"] = biomaterial.assign(
        biomaterialid="unmatched-" + biomaterial["biomaterialid"]
    )


class TestTransformMarmoDetails:
    data_files_path = "tests/test_assets/marmo_details"

    # Input files shared across the pass case and the fail cases.
    good_input_files = {
        "marmo_model_metadata": "marmo_model_metadata_good_input.csv",
        "marmo_genotype_label_map": "marmo_genotype_label_map_good_input.csv",
        "marmo_biomarker_measure_info": "marmo_biomarker_measure_info_good_input.csv",
        "marmo_individual_metadata": "marmo_individual_metadata_good_input.csv",
        "marmo_biomaterial_metadata": "marmo_biomaterial_metadata_good_input.csv",
        "marmo_results": "marmo_results_good_input.csv",
    }

    def _load_datasets(self, overrides=None):
        input_files = {**self.good_input_files, **(overrides or {})}
        return {
            dataset_name: pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )
            for dataset_name, file_name in input_files.items()
        }

    @pytest.mark.parametrize(
        "input_overrides,expected_output_file",
        [
            ({}, "marmo_details_transform_good_test_output.json"),
            (
                {
                    "marmo_model_metadata": "marmo_model_metadata_multi_model_input.csv",
                    "marmo_genotype_label_map": "marmo_genotype_label_map_multi_model_input.csv",
                },
                "marmo_details_transform_multi_model_output.json",
            ),
        ],
        ids=["one model", "two models sharing WT controls"],
    )
    def test_marmo_details_transform_should_pass(
        self, input_overrides, expected_output_file
    ):
        """The golden files are the contract for everything observable in the output: melt,
        genotype mapping (including exclusion of an unmapped genotype), dropping rows with no
        biomaterial match, dropping null measurements, title-cased sex, empty-string ratio units,
        measure/age sort order, and a y_axis_max that is the per-model, per-evidence_type maximum
        applied to every age bucket. The unit tests below only cover what a dict comparison
        against these files cannot distinguish."""
        datasets = self._load_datasets(input_overrides)

        output_data = transform_marmo_details(datasets=datasets)

        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

        assert output_data == expected_data

    def test_marmo_details_missing_dataset_should_fail(self):
        datasets = self._load_datasets()
        del datasets["marmo_results"]

        with pytest.raises(ValueError, match="Missing required datasets"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_missing_column_should_fail(self):
        """The label map's model column is the case worth pinning: it is what ties a measurement
        to a model page, so its absence must fail up front rather than reaching the genotype
        join."""
        datasets = self._load_datasets()
        datasets["marmo_genotype_label_map"] = datasets[
            "marmo_genotype_label_map"
        ].drop(columns=["model"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_marmo_details(datasets=datasets)

    def _set_bad_value(self, datasets, dataset, column, bad_value):
        """Overwrite the first row of a column, which every rule below scans in full."""
        frame = datasets[dataset]
        # Cast first: writing a string into a numeric column is deprecated in pandas.
        frame[column] = frame[column].astype(object)
        frame.loc[0, column] = bad_value

    def test_marmo_details_rejects_invalid_collection_age_units(self):
        """Ages are bucketed as months, so any other unit on a plotted row must fail."""
        datasets = self._load_datasets()
        self._set_bad_value(
            datasets, "marmo_biomaterial_metadata", "collectionageunits", "days"
        )

        with pytest.raises(
            ValueError, match=r"column 'collectionageunits'.*rule 'one_of'"
        ):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "dataset,column,bad_value,expected_message",
        [
            (
                "marmo_biomaterial_metadata",
                "collectionage",
                "eighteen",
                r"column 'collectionage'.*rule 'numeric'",
            ),
            # Negative ages are not allowed.
            (
                "marmo_biomaterial_metadata",
                "collectionage",
                -6,
                r"column 'collectionage'.*rule 'non_negative'",
            ),
            (
                "marmo_biomarker_measure_info",
                "display_order",
                "third",
                r"column 'display_order'.*rule 'numeric'",
            ),
            (
                "marmo_biomarker_measure_info",
                "display_order",
                -1,
                r"column 'display_order'.*rule 'non_negative'",
            ),
        ],
        ids=[
            "non-numeric collection age",
            "negative collection age",
            "non-numeric display order",
            "negative display order",
        ],
    )
    def test_marmo_details_rejects_invalid_numeric_values(
        self, dataset, column, bad_value, expected_message
    ):
        """A single bad cell raises a ValueError that names the file, column, and rule."""
        datasets = self._load_datasets()
        self._set_bad_value(datasets, dataset, column, bad_value)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "column,expected_message",
        [
            # display_order and evidence_type are nest_fields grouping keys: unvalidated, a null
            # silently deletes that measure from every model page.
            ("evidence_type", r"column 'evidence_type'.*rule 'not_empty'"),
            # A null result_column would otherwise reach standardize_column_name and raise a bare
            # TypeError naming neither file nor column.
            ("result_column", r"column 'result_column'.*rule 'not_empty'"),
        ],
        ids=["null evidence type", "null result column"],
    )
    def test_marmo_details_rejects_none_values(self, column, expected_message):
        """A null in a measure-info column raises rather than dropping a measure silently."""
        datasets = self._load_datasets()
        self._set_bad_value(datasets, "marmo_biomarker_measure_info", column, None)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "dataset,column,bad_value,expected_message",
        [
            # A result_column typo names a measure that marmo_results does not carry.
            (
                "marmo_biomarker_measure_info",
                "result_column",
                "GFAP_typo",
                "not present in marmo_results",
            ),
            # A label-map model absent from marmo_model_metadata is silent when unguarded: the
            # measurements are attributed to a model with no output entry, and the real model
            # page emits an empty biomarkers list.
            (
                "marmo_genotype_label_map",
                "model",
                "Presenilin-1",
                "not present in marmo_model_metadata",
            ),
        ],
        ids=["typo'd result column", "label-map model absent from model metadata"],
    )
    def test_marmo_details_fails_on_mismatches(
        self, dataset, column, bad_value, expected_message
    ):
        """A value that no longer matches across two hand-maintained files raises."""
        datasets = self._load_datasets()
        self._set_bad_value(datasets, dataset, column, bad_value)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "dataset,expected_message",
        [
            ("marmo_genotype_label_map", r"duplicate \(model, genotype\)"),
            # Matched on the full message because pandas raises its own ValueError mentioning
            # uniqueness from the m:1 merge, which would let a looser pattern pass either way.
            ("marmo_biomaterial_metadata", r"column 'biomaterialid'.*rule 'unique'"),
        ],
        ids=["duplicate (model, genotype)", "duplicate biomaterialid"],
    )
    def test_marmo_details_duplicate_key_should_fail(self, dataset, expected_message):
        """A duplicated key row would multiply a model's measurements. Both keys are checked up
        front rather than left to merge validation, which catches a duplicate only when it
        happens to back a plotted measurement."""
        datasets = self._load_datasets()
        frame = datasets[dataset]
        datasets[dataset] = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "break_source,expected_message",
        [
            (
                lambda datasets: _blank_measure_columns(datasets, MEASURE_COLUMNS),
                "marmo_results has no numeric values",
            ),
            (
                _unmatch_label_map_genotypes,
                "No matching genotypes found between marmo_results and "
                "marmo_genotype_label_map",
            ),
            (
                _unmatch_biomaterial_ids,
                "No matching numeric 'collectionage' values",
            ),
        ],
        ids=["no numeric values", "no matching genotype", "no matching biomaterialid"],
    )
    def test_marmo_details_source_mismatch_should_fail(
        self, break_source, expected_message
    ):
        """Every step that can discard all measurements raises instead of emitting empty
        biomarkers collections. This is the failure mode that let an earlier id-scheme mismatch
        between two files go unnoticed."""
        datasets = self._load_datasets()
        break_source(datasets)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "unplotted_id,bad_units",
        [("GT20-19233", None), ("7017_1", "days")],
        ids=[
            "biomaterial absent from marmo_results",
            "biomaterial whose measurements are not plotted",
        ],
    )
    def test_marmo_details_units_rule_skips_unplotted_rows_should_pass(
        self, unplotted_id, bad_units
    ):
        """The collectionAgeUnits rule applies only to the biomaterial rows behind plotted
        measurements. A bad unit on a plotted row fails instead, which
        test_marmo_details_rejects_invalid_collection_age_units covers."""
        datasets = self._load_datasets()
        # GT20-19233 is absent from marmo_results. 7017_1 is present, but it belongs to
        # individual 3, whose NOTCH3 genotype is absent from the label map, so its measurements
        # are dropped before the rule runs.
        datasets["marmo_biomaterial_metadata"] = pd.DataFrame(
            {
                "biomaterialid": ["7015_1", "7016_1", unplotted_id],
                "collectionage": [6, 18, 10],
                "collectionageunits": ["months", "months", bad_units],
            }
        )

        transform_marmo_details(datasets=datasets)

    def test_marmo_details_model_without_label_map_rows_gets_empty_biomarkers(self):
        """A model in marmo_model_metadata with no matching label-map rows still gets an output
        entry, with an empty biomarkers list rather than being dropped."""
        datasets = self._load_datasets()
        metadata = datasets["marmo_model_metadata"]
        datasets["marmo_model_metadata"] = pd.concat(
            [
                metadata,
                metadata.assign(model="Orphan", ensembl_gene_id="ENSCJAG00000000001"),
            ],
            ignore_index=True,
        )

        output_data = transform_marmo_details(datasets=datasets)

        biomarkers = {model["name"]: model["biomarkers"] for model in output_data}
        assert biomarkers["Orphan"] == []
        assert biomarkers["Presenilin1"]


def _measurement_inputs():
    """Inputs for the two behaviors the golden files cannot cover: a measurement belonging to an
    individual with no metadata row, and an age that sits either side of a bucket boundary."""
    datasets = {
        # Individual 9 has no row in marmo_individual_metadata. Individual 1 is sampled
        # longitudinally at 6, 11.9 and 12 months so the year-bucket boundary is covered.
        "marmo_results": pd.DataFrame(
            {
                "biomaterialid": ["7015_1", "7019_1", "7016_1", "7017_1"],
                "individualid": [1, 9, 1, 1],
                "ab40_pg_ml": [100.0, 900.0, 110.0, 120.0],
            }
        ),
        "marmo_individual_metadata": pd.DataFrame(
            {"individualid": [1], "genotype": ["WT"], "sex": ["male"]}
        ),
        "marmo_biomaterial_metadata": pd.DataFrame(
            {
                "biomaterialid": ["7015_1", "7019_1", "7016_1", "7017_1"],
                "collectionage": [6, 9, 11.9, 12],
                "collectionageunits": ["months"] * 4,
            }
        ),
        "marmo_genotype_label_map": pd.DataFrame(
            {
                "model": ["Presenilin1"],
                "genotype": ["WT"],
                "display_label": ["Matched Control"],
            }
        ),
    }
    measure_info = pd.DataFrame(
        {
            "result_column_std": ["ab40_pg_ml"],
            "evidence_type": ["A&beta;40"],
            "units": ["pg/mL"],
            "display_order": [1],
        }
    )
    return datasets, measure_info


def test_measurements_drop_unknown_individuals():
    """A measurement whose individualid is absent from marmo_individual_metadata is dropped: the
    left join yields a null genotype, which the inner genotype-map merge excludes."""
    datasets, measure_info = _measurement_inputs()

    measurements = _build_measurements(datasets, measure_info)

    assert set(measurements["individualid"]) == {1}


def test_measurements_floor_ages_to_whole_years():
    """Ages floor rather than round, so 11.9 months is still the first bucket and 12.0 opens the
    second."""
    datasets, measure_info = _measurement_inputs()

    measurements = _build_measurements(datasets, measure_info).sort_values(
        "collectionage"
    )

    assert list(measurements["age"]) == ["0-1 years", "0-1 years", "1-2 years"]


class TestBuildBiomarkers:
    def _measurements(self):
        """Mirrors what _build_measurements emits, including the raw genotype column carried
        over from the individual join. That column must be dropped before display_label is
        renamed to genotype, so the fixture has to carry it for the collision path to be
        exercised at all. Individuals 10 and 2 share the first bucket so that data-point
        ordering within a bucket is observable."""
        return pd.DataFrame(
            {
                "individualid": [10, 2, 2, 1],
                "value": [100.0, 150.0, 200.0, 0.1],
                "sex": ["Male", "Female", "Female", "Male"],
                "genotype": [
                    "WT",
                    "PSEN1-C410Y_Y410/Y410",
                    "PSEN1-C410Y_Y410/Y410",
                    "WT",
                ],
                "display_label": [
                    "Matched Control",
                    "Presenilin-1",
                    "Presenilin-1",
                    "Matched Control",
                ],
                "evidence_type": [
                    "A&beta;40",
                    "A&beta;40",
                    "A&beta;40",
                    "A&beta;42/A&beta;40",
                ],
                "age": ["0-1 years", "0-1 years", "1-2 years", "0-1 years"],
                "units": ["pg/mL", "pg/mL", "pg/mL", ""],
                "display_order": [1, 1, 1, 2],
                "age_start": [0, 0, 1, 0],
            }
        )

    @pytest.mark.parametrize(
        "display_orders,expected_order",
        [
            (
                [2, 2, 2, 1],
                [
                    ("A&beta;42/A&beta;40", "0-1 years"),
                    ("A&beta;40", "0-1 years"),
                    ("A&beta;40", "1-2 years"),
                ],
            ),
            (
                [1, 1, 1, 1],
                [
                    ("A&beta;40", "0-1 years"),
                    ("A&beta;40", "1-2 years"),
                    ("A&beta;42/A&beta;40", "0-1 years"),
                ],
            ),
        ],
        ids=["display_order outranks evidence_type", "tie broken by evidence_type"],
    )
    def test_sort_order(self, display_orders, expected_order):
        """display_order is the primary key; evidence_type is only a tiebreaker, and it matters
        when two measures share a display_order, where sorting on age alone would interleave them
        and break each measure's run of ascending ages."""
        measurements = self._measurements().assign(display_order=display_orders)

        biomarkers = _build_biomarkers(measurements, "Presenilin1")

        assert [(b["evidence_type"], b["age"]) for b in biomarkers] == expected_order

    def test_data_point_keys_and_order(self):
        """Key order is pinned because comparing dicts against the golden files ignores it, and
        nest_fields emits keys in column order. Points are ordered by the numeric individualid,
        so animal 2 precedes animal 10 rather than sorting lexicographically as the stringified
        individual_id would."""
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        points = biomarkers[0]["data"]
        assert list(points[0].keys()) == ["individual_id", "value", "sex", "genotype"]
        assert [point["individual_id"] for point in points] == ["2", "10"]
