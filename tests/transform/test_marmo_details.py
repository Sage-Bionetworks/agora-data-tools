import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import (
    _build_biomarkers,
    _build_measurements,
    transform_marmo_details,
)


# Each of these breaks the vocabulary one filtering step in _build_measurements joins on, so
# that step discards every measurement. They mutate whole columns, so they cannot be expressed
# as single-cell edits like the bad-value cases below.
def _blank_every_measure_column(datasets):
    for column in ["ab40_pg_ml", "ab_ratio", "gfap_pg_ml"]:
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
        "marmo_metadata": "marmo_metadata_good_input.csv",
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
                    "marmo_metadata": "marmo_metadata_multi_model_input.csv",
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

    @pytest.mark.parametrize(
        "dataset,row,column,bad_value,expected_message",
        [
            # Ages are bucketed as months, so any other unit on a plotted row must fail.
            (
                "marmo_biomaterial_metadata",
                1,
                "collectionageunits",
                "days",
                r"column 'collectionageunits'.*rule 'one_of'",
            ),
            (
                "marmo_biomaterial_metadata",
                1,
                "collectionage",
                "eighteen",
                r"column 'collectionage'.*rule 'numeric'",
            ),
            # Negative ages are rejected at the trust boundary rather than clamped during year
            # bucketing, so bad source data cannot reach the output as a wrong age bucket.
            (
                "marmo_biomaterial_metadata",
                0,
                "collectionage",
                -6,
                r"column 'collectionage'.*rule 'non_negative'",
            ),
            # display_order and evidence_type are nest_fields grouping keys: unvalidated, a NaN
            # or null silently deletes that measure from every model page.
            (
                "marmo_biomarker_measure_info",
                2,
                "display_order",
                "third",
                r"column 'display_order'.*rule 'numeric'",
            ),
            (
                "marmo_biomarker_measure_info",
                2,
                "evidence_type",
                None,
                r"column 'evidence_type'.*rule 'not_empty'",
            ),
            # A null result_column would otherwise reach standardize_column_name and raise a bare
            # TypeError naming neither file nor column.
            (
                "marmo_biomarker_measure_info",
                2,
                "result_column",
                None,
                r"column 'result_column'.*rule 'not_empty'",
            ),
            # A result_column typo names a measure that marmo_results does not carry.
            (
                "marmo_biomarker_measure_info",
                2,
                "result_column",
                "GFAP_typo",
                "not present in marmo_results",
            ),
            # A label-map model absent from marmo_metadata is a typo between two hand-maintained
            # files. Unguarded it is silent: the measurements are attributed to a model with no
            # output entry, and the real model page emits an empty biomarkers list.
            (
                "marmo_genotype_label_map",
                0,
                "model",
                "Presenilin-1",
                "not present in marmo_metadata",
            ),
        ],
    )
    def test_marmo_details_bad_source_value_should_fail(
        self, dataset, row, column, bad_value, expected_message
    ):
        """A single bad cell raises a ValueError that names the file, column, and rule."""
        datasets = self._load_datasets()
        frame = datasets[dataset]
        # Cast first: writing a string into a numeric column is deprecated in pandas.
        frame[column] = frame[column].astype(object)
        frame.loc[row, column] = bad_value

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
                _blank_every_measure_column,
                "No marmo_results row carries a numeric value",
            ),
            (
                _unmatch_label_map_genotypes,
                "No marmo_results measurement matched a marmo_genotype_label_map genotype",
            ),
            (
                _unmatch_biomaterial_ids,
                "No marmo_results measurement matched a marmo_biomaterial_metadata record",
            ),
        ],
    )
    def test_marmo_details_source_mismatch_should_fail(
        self, break_source, expected_message
    ):
        """Every step that can discard all measurements is guarded by require_survivors, so a
        source regression raises instead of emitting empty biomarkers collections. This is the
        failure mode that let an earlier id-scheme mismatch between two files go unnoticed. The
        guard's own logic is covered in tests/test_utils.py; these cases only pin that each step
        is guarded and reports which files disagree."""
        datasets = self._load_datasets()
        break_source(datasets)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "row,bad_units",
        [
            # GT20-19233 is a nanostring assay with no age, absent from marmo_results. OneOfRule
            # counts nulls as violations, so such rows must be excluded before the check runs.
            (3, None),
            # 7017_1 is referenced by marmo_results, but individual 3's NOTCH3 genotype is not in
            # the label map, so its measurements never reach the output. marmo_results carries
            # such rows for every model not yet onboarded, so a bad unit there must not fail a
            # release over data that is never plotted.
            (2, "days"),
        ],
        ids=["row absent from marmo_results", "row whose measurements are not plotted"],
    )
    def test_marmo_details_units_rule_skips_unplotted_rows_should_pass(
        self, row, bad_units
    ):
        """The collectionAgeUnits rule applies only to the biomaterial rows behind plotted
        measurements."""
        datasets = self._load_datasets()
        datasets["marmo_biomaterial_metadata"].loc[
            row, "collectionageunits"
        ] = bad_units

        transform_marmo_details(datasets=datasets)

    def test_marmo_details_model_without_label_map_rows_gets_empty_biomarkers(self):
        """A model in marmo_metadata with no matching label-map rows still gets an output entry,
        with an empty biomarkers list rather than being dropped."""
        datasets = self._load_datasets()
        metadata = datasets["marmo_metadata"]
        datasets["marmo_metadata"] = pd.concat(
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


def test_measurements_drop_unknown_individuals_and_floor_ages():
    """Two behaviors the golden files cannot cover. A measurement whose individualid is absent
    from marmo_individual_metadata is dropped silently: the left join yields a null genotype,
    which the inner genotype-map merge excludes. Ages floor rather than round, so 11.9 months is
    still the first bucket and 12.0 opens the second."""
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

    measurements = _build_measurements(datasets, measure_info).sort_values(
        "collectionage"
    )

    assert set(measurements["individualid"]) == {1}
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
