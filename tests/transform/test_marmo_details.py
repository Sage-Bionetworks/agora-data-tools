import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import (
    _build_biomarkers,
    _build_measurements,
    transform_marmo_details,
)
from agoradatatools.etl.utils import round_y_axis_max


# Each of these breaks the vocabulary one filtering step in _build_measurements joins on, so
# that step discards every measurement.
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

    # Input files shared across the "good" pass case and the fail cases.
    good_input_files = {
        "marmo_metadata": "marmo_metadata_good_input.csv",
        "marmo_genotype_label_map": "marmo_genotype_label_map_good_input.csv",
        "marmo_biomarker_measure_info": "marmo_biomarker_measure_info_good_input.csv",
        "marmo_individual_metadata": "marmo_individual_metadata_good_input.csv",
        "marmo_biomaterial_metadata": "marmo_biomaterial_metadata_good_input.csv",
        "marmo_results": "marmo_results_good_input.csv",
    }

    def _load_datasets(self, input_files):
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )
        return datasets

    def test_marmo_details_transform_should_pass(self):
        """Good data: exercises melt, genotype mapping (including exclusion of an unmapped
        genotype), dropping rows with no biomaterial match, dropping null measurements,
        empty-string ratio units, and measure/age sort order."""
        datasets = self._load_datasets(self.good_input_files)

        output_data = transform_marmo_details(datasets=datasets)

        with open(
            os.path.join(
                self.data_files_path,
                "output",
                "marmo_details_transform_good_test_output.json",
            )
        ) as f:
            expected_data = json.load(f)

        assert output_data == expected_data

    def test_marmo_details_missing_dataset_should_fail(self):
        """A missing required dataset raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        del datasets["marmo_results"]

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_missing_column_should_fail(self):
        """A required column missing from a dataset raises ValueError. The label map's model
        column is the case worth pinning: it is what ties a measurement to a model page, so its
        absence must fail up front rather than reaching the genotype join."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_genotype_label_map"] = datasets[
            "marmo_genotype_label_map"
        ].drop(columns=["model"])

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_multiple_models_should_pass(self):
        """Two models share WT controls via the label map; each model gets its own entry
        and the shared control appears under both."""
        input_files = dict(self.good_input_files)
        input_files["marmo_metadata"] = "marmo_metadata_multi_model_input.csv"
        input_files[
            "marmo_genotype_label_map"
        ] = "marmo_genotype_label_map_multi_model_input.csv"
        datasets = self._load_datasets(input_files)

        output_data = transform_marmo_details(datasets=datasets)

        with open(
            os.path.join(
                self.data_files_path,
                "output",
                "marmo_details_transform_multi_model_output.json",
            )
        ) as f:
            expected_data = json.load(f)

        assert output_data == expected_data

    def test_marmo_details_unknown_label_map_model_should_fail(self):
        """A label-map model that matches no marmo_metadata model is a typo between two
        hand-maintained files. Unguarded it is silent: the measurements are attributed to a model
        with no output entry, and the real model page emits an empty biomarkers list."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_genotype_label_map"]["model"] = "Presenilin-1"

        with pytest.raises(ValueError, match="not present in marmo_metadata"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_duplicate_model_genotype_should_fail(self):
        """A duplicate (model, genotype) pair in the label map raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        label_map = datasets["marmo_genotype_label_map"]
        datasets["marmo_genotype_label_map"] = pd.concat(
            [label_map, label_map.iloc[[0]]], ignore_index=True
        )

        with pytest.raises(ValueError, match="duplicate \\(model, genotype\\)"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_bad_collection_age_units_should_fail(self):
        """A collectionAgeUnits value other than months on a referenced biomaterial row fails
        validation and raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomaterial_metadata"].loc[1, "collectionageunits"] = "days"

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_blank_units_on_unreferenced_row_should_pass(self):
        """A blank collectionAgeUnits is tolerated on a biomaterial row that marmo_results does
        not reference. The file records assays with no age (e.g. nanostring), and OneOfRule counts
        nulls as violations, so those rows must be excluded before the unit check runs.
        """
        datasets = self._load_datasets(self.good_input_files)
        assert datasets["marmo_biomaterial_metadata"]["collectionageunits"].isna().any()

        transform_marmo_details(datasets=datasets)

    def test_marmo_details_bad_units_on_unplotted_row_should_pass(self):
        """A biomaterial row that marmo_results references but whose measurements never reach the
        output is out of scope for the units rule. Biomaterial 7017_1 belongs to individual 3,
        whose NOTCH3 genotype the label map does not list. marmo_results carries such rows for
        every model not yet onboarded, so a bad unit there must not fail a release over data
        that is never plotted.
        """
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomaterial_metadata"].loc[2, "collectionageunits"] = "days"

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
        datasets = self._load_datasets(self.good_input_files)
        break_source(datasets)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_non_numeric_collection_age_should_fail(self):
        """A non-numeric collectionage fails the NumericRule and raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        # Cast first: collectionage parses as float, and writing a string into a float column
        # is deprecated in pandas.
        biomaterial = datasets["marmo_biomaterial_metadata"].astype(
            {"collectionage": object}
        )
        biomaterial.loc[1, "collectionage"] = "eighteen"
        datasets["marmo_biomaterial_metadata"] = biomaterial

        with pytest.raises(ValueError, match="numeric"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_negative_collection_age_should_fail(self):
        """A negative collectionage fails the NonNegativeRule and raises ValueError. Negative ages
        are rejected at the trust boundary rather than clamped during year bucketing, so bad source
        data can't reach the output as a silently wrong age bucket."""
        datasets = self._load_datasets(self.good_input_files)
        biomaterial = datasets["marmo_biomaterial_metadata"]
        biomaterial.loc[0, "collectionage"] = -6

        with pytest.raises(ValueError, match="non_negative"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_duplicate_biomaterial_id_should_fail(self):
        """A repeated biomaterialid fails the UniqueRule. The m:1 merge in _build_measurements
        catches this only when the duplicate happens to back a plotted measurement, so the rule is
        what rejects a duplicated key anywhere in the file."""
        datasets = self._load_datasets(self.good_input_files)
        biomaterial = datasets["marmo_biomaterial_metadata"]
        datasets["marmo_biomaterial_metadata"] = pd.concat(
            [biomaterial, biomaterial.iloc[[0]]], ignore_index=True
        )

        # Matched on the full message because pandas raises its own ValueError mentioning
        # uniqueness from the m:1 merge, which would let a looser pattern pass either way.
        with pytest.raises(ValueError, match="column 'biomaterialid'.*rule 'unique'"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_unknown_result_column_should_fail(self):
        """A result_column in the measure-info mapping that is absent from marmo_results
        (e.g. a typo) raises ValueError rather than silently dropping the measure."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomarker_measure_info"].loc[2, "result_column"] = "GFAP_typo"

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_non_numeric_display_order_should_fail(self):
        """A non-numeric display_order fails the NumericRule. Without it the value is coerced to
        NaN and, because display_order is a nest_fields grouping key, pandas groupby drops the
        whole measure from every model page with no error."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomarker_measure_info"].loc[2, "display_order"] = "third"

        with pytest.raises(ValueError, match="numeric"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_blank_evidence_type_should_fail(self):
        """A blank evidence_type fails the NotEmptyRule. It is also a nest_fields grouping key,
        so an unvalidated null would silently delete that measure from every model page.
        """
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomarker_measure_info"].loc[2, "evidence_type"] = None

        with pytest.raises(ValueError, match="not_empty"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_blank_result_column_should_fail(self):
        """A blank result_column fails the NotEmptyRule. Without it the null reaches
        standardize_column_name and raises a bare TypeError naming neither file nor column.
        """
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_biomarker_measure_info"].loc[2, "result_column"] = None

        with pytest.raises(ValueError, match="not_empty"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_model_without_label_map_rows_gets_empty_biomarkers(self):
        """A model in marmo_metadata with no matching label-map rows still gets an output entry,
        with an empty biomarkers list rather than being dropped."""
        datasets = self._load_datasets(self.good_input_files)
        metadata = datasets["marmo_metadata"]
        datasets["marmo_metadata"] = pd.concat(
            [
                metadata,
                metadata.assign(model="Orphan", ensembl_gene_id="ENSCJAG00000000001"),
            ],
            ignore_index=True,
        )

        output_data = transform_marmo_details(datasets=datasets)

        entries = {model["name"]: model for model in output_data}
        assert set(entries) == {"Presenilin1", "Orphan"}
        assert entries["Orphan"]["biomarkers"] == []
        assert entries["Presenilin1"]["biomarkers"]


class TestBuildMeasurements:
    """Unit tests for _build_measurements covering the silent-drop behaviors."""

    def _measure_info(self):
        return pd.DataFrame(
            {
                "result_column_std": ["ab40_pg_ml"],
                "evidence_type": ["A&beta;40"],
                "units": ["pg/mL"],
                "display_order": [1],
            }
        )

    def _datasets(self):
        return {
            # individual 9 has no row in marmo_individual_metadata. Individual 1 is sampled
            # longitudinally at 6, 11.9 and 12 months so the year-bucket boundary is covered.
            "marmo_results": pd.DataFrame(
                {
                    "biomaterialid": ["7015_1", "7019_1", "7016_1", "7017_1"],
                    "individualid": [1, 9, 1, 1],
                    "ab40_pg_ml": [100.0, 900.0, 110.0, 120.0],
                }
            ),
            "marmo_individual_metadata": pd.DataFrame(
                {
                    "individualid": [1],
                    "genotype": ["WT"],
                    "sex": ["male"],
                }
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

    def test_measurement_missing_individual_metadata_is_dropped(self):
        """A measurement whose individualid is absent from marmo_individual_metadata is dropped
        silently: the left join yields a null genotype, which the inner genotype-map merge
        excludes."""
        measurements = _build_measurements(self._datasets(), self._measure_info())

        assert set(measurements["individualid"]) == {1}

    def test_measurement_columns_and_values(self):
        """A surfaced measurement carries the joined genotype label, title-cased sex, and age
        bucket."""
        measurements = _build_measurements(self._datasets(), self._measure_info())

        row = measurements.iloc[0]
        assert row["model"] == "Presenilin1"
        assert row["display_label"] == "Matched Control"
        assert row["sex"] == "Male"
        assert row["age"] == "0-1 years"
        assert row["value"] == 100.0

    def test_ages_floor_to_the_year_the_sample_was_taken(self):
        """Ages floor rather than round, so 11.9 months is still the first bucket and 12.0 opens
        the second."""
        measurements = _build_measurements(
            self._datasets(), self._measure_info()
        ).sort_values("collectionage")

        assert list(measurements["age"]) == ["0-1 years", "0-1 years", "1-2 years"]
        assert list(measurements["age_start"]) == [0, 0, 1]


class TestBuildBiomarkers:
    """Unit tests for _build_biomarkers covering sort order, y_axis_max, and empty units."""

    def _measurements(self):
        """Mirrors what _build_measurements emits, including the raw genotype column carried
        over from the individual join. That column must be dropped before display_label is
        renamed to genotype, so the fixture has to carry it for the collision path to be
        exercised at all."""
        return pd.DataFrame(
            {
                "individualid": [1, 2, 1],
                "value": [100.0, 200.0, 0.1],
                "sex": ["Male", "Female", "Male"],
                "genotype": ["WT", "PSEN1-C410Y_Y410/Y410", "WT"],
                "display_label": ["Matched Control", "Presenilin-1", "Matched Control"],
                "evidence_type": ["A&beta;40", "A&beta;40", "A&beta;42/A&beta;40"],
                "age": ["0-1 years", "1-2 years", "0-1 years"],
                "units": ["pg/mL", "pg/mL", ""],
                "display_order": [1, 1, 2],
                "age_start": [0, 1, 0],
            }
        )

    def test_empty_measurements_returns_empty_list(self):
        assert _build_biomarkers(pd.DataFrame(), "Presenilin1") == []

    def test_sort_order_by_display_order_then_age(self):
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        order = [(b["evidence_type"], b["age"]) for b in biomarkers]
        assert order == [
            ("A&beta;40", "0-1 years"),
            ("A&beta;40", "1-2 years"),
            ("A&beta;42/A&beta;40", "0-1 years"),
        ]

    def test_tied_display_order_keeps_each_measure_contiguous(self):
        """When two measures share a display_order, sorting on age alone interleaves them and
        breaks each measure's run of ascending ages. evidence_type breaks the tie so every
        measure stays in one contiguous block."""
        measurements = self._measurements()
        measurements["display_order"] = 1

        biomarkers = _build_biomarkers(measurements, "Presenilin1")

        order = [(b["evidence_type"], b["age"]) for b in biomarkers]
        assert order == [
            ("A&beta;40", "0-1 years"),
            ("A&beta;40", "1-2 years"),
            ("A&beta;42/A&beta;40", "0-1 years"),
        ]

    def test_ratio_units_are_empty_string(self):
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        ratio = [b for b in biomarkers if b["evidence_type"] == "A&beta;42/A&beta;40"]
        assert ratio
        assert all(b["units"] == "" for b in ratio)

    def test_y_axis_max_is_per_evidence_type_rounded_max(self):
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        expected = {
            "A&beta;40": float(round_y_axis_max(200.0)),
            "A&beta;42/A&beta;40": float(round_y_axis_max(0.1)),
        }
        for b in biomarkers:
            assert b["y_axis_max"] == expected[b["evidence_type"]]

    def test_data_points_have_expected_keys(self):
        """Key order is pinned, not just membership: nest_fields emits keys in column order, so
        this is what keeps the serialized output matching the golden fixtures and the structure
        the ticket defines."""
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        point = biomarkers[0]["data"][0]
        assert list(point.keys()) == ["individual_id", "value", "sex", "genotype"]

    def test_output_genotype_is_the_display_label(self):
        """The emitted genotype is the display label, not the raw genotype the measurements
        frame carries in from the individual join."""
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        genotypes = {p["genotype"] for b in biomarkers for p in b["data"]}
        assert genotypes == {"Matched Control", "Presenilin-1"}

    def test_data_points_sort_individuals_numerically(self):
        """Points are ordered by the numeric individualid, so animal 2 precedes animal 10
        rather than sorting lexicographically as the stringified individual_id would."""
        measurements = pd.DataFrame(
            {
                "individualid": [10, 2, 1],
                "value": [300.0, 200.0, 100.0],
                "sex": ["Male", "Female", "Male"],
                "genotype": ["WT", "WT", "WT"],
                "display_label": ["Matched Control"] * 3,
                "evidence_type": ["A&beta;40"] * 3,
                "age": ["0-1 years"] * 3,
                "units": ["pg/mL"] * 3,
                "display_order": [1, 1, 1],
                "age_start": [0, 0, 0],
            }
        )

        biomarkers = _build_biomarkers(measurements, "Presenilin1")

        assert [p["individual_id"] for p in biomarkers[0]["data"]] == ["1", "2", "10"]
