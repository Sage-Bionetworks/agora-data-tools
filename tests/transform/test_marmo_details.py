import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import (
    _apply_qc_masks,
    _build_biomarkers,
    _build_measurements,
    _drop_single_genotype_buckets,
    _fill_age_gaps,
    _prepare_measure_info,
    _validate_and_prepare_model_metadata,
    transform_marmo_details,
    QC_MEASURE_GROUPS,
)
from agoradatatools.etl.utils import round_y_axis_max


# The measurement columns carried by the marmo_results fixture.
MEASURE_COLUMNS = ["ab40_pg_ml", "ab_ratio", "gfap_pg_ml"]

# Assay-group measure columns from the transform
AB_COLS = QC_MEASURE_GROUPS["qc_ab"]
NEURO_COLS = QC_MEASURE_GROUPS["qc_neuro"]

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

    @pytest.mark.parametrize("qc_column", ["qc_ab", "qc_neuro"])
    def test_marmo_details_missing_qc_column_should_fail(self, qc_column):
        """qc_ab and qc_neuro are required on marmo_results; a missing one fails up front rather
        than silently disabling the QC filter."""
        datasets = self._load_datasets()
        datasets["marmo_results"] = datasets["marmo_results"].drop(columns=[qc_column])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_marmo_details(datasets=datasets)

    @pytest.mark.parametrize(
        "qc_fail_column,expected_biomarkers",
        [
            # Failing the control's Ab QC drops its ab40/ab_ratio points, leaving those 0-1 yr
            # buckets with only the Presenilin-1 animal - single-genotype, so the genotype filter
            # then removes them. Only GFAP (neuro QC still PASS) keeps both genotypes.
            ("qc_ab", {("GFAP", "0-1 years")}),
            # Symmetric: failing neuro QC drops the control's GFAP point, so GFAP goes single-genotype
            # and is removed, while the Ab plots keep both genotypes.
            (
                "qc_neuro",
                {
                    ("A&beta;40", "0-1 years"),
                    ("A&beta;42/A&beta;40", "0-1 years"),
                },
            ),
        ],
        ids=[
            "ab QC failure -> ab buckets go single-genotype and drop",
            "neuro QC failure -> gfap goes single-genotype and drops",
        ],
    )
    def test_marmo_details_qc_failure_with_genotype_filtering(
        self, qc_fail_column, expected_biomarkers
    ):
        """End-to-end interaction of the two filters: 0-1 yr has one control (7015_1) and one model
        (7020_1) animal. Failing a QC group on the control removes that group's control point, which
        drops the bucket to a single genotype, which the genotype filter then removes - while the
        other assay group (QC still PASS) retains both genotypes and survives."""
        datasets = self._load_datasets()
        results = datasets["marmo_results"]
        results.loc[results["biomaterialid"] == "7015_1", qc_fail_column] = "FAIL"

        output_data = transform_marmo_details(datasets=datasets)

        presenilin = next(m for m in output_data if m["name"] == "Presenilin1")
        biomarkers = {(b["evidence_type"], b["age"]) for b in presenilin["biomarkers"]}
        assert biomarkers == expected_biomarkers

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
            ("display_order", r"column 'display_order'.*rule 'not_empty'"),
            ("evidence_type", r"column 'evidence_type'.*rule 'not_empty'"),
            # A null result_column would otherwise reach standardize_column_name and raise a bare
            # TypeError naming neither file nor column.
            ("result_column", r"column 'result_column'.*rule 'not_empty'"),
        ],
        ids=["null display order", "null evidence type", "null result column"],
    )
    def test_marmo_details_rejects_none_values(self, column, expected_message):
        """A null in a measure-info column raises rather than dropping a measure silently."""
        datasets = self._load_datasets()
        self._set_bad_value(datasets, "marmo_biomarker_measure_info", column, None)

        with pytest.raises(ValueError, match=expected_message):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_fails_on_result_column_mismatch(self):
        """A result_column typo names a measure that marmo_results does not carry."""
        datasets = self._load_datasets()
        self._set_bad_value(
            datasets, "marmo_biomarker_measure_info", "result_column", "GFAP_typo"
        )

        with pytest.raises(ValueError, match="not present in marmo_results"):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_duplicate_biomaterialid_should_fail(self):
        """A duplicated biomaterialid would multiply measurements. UniqueRule catches it
        up front rather than leaving it to merge validation, which fires only when the
        duplicate happens to back a plotted measurement."""
        datasets = self._load_datasets()
        frame = datasets["marmo_biomaterial_metadata"]
        datasets["marmo_biomaterial_metadata"] = pd.concat(
            [frame, frame.iloc[[0]]], ignore_index=True
        )

        with pytest.raises(ValueError, match=r"column 'biomaterialid'.*rule 'unique'"):
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


class TestValidateAndPrepareModelMetadata:
    def _metadata_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Two-frame inputs for _validate_and_prepare_model_metadata."""
        genotype_map = pd.DataFrame(
            {
                "model": ["Presenilin1", "Presenilin1"],
                "genotype": ["WT", "PSEN1-C410Y_Y410/Y410"],
                "display_label": ["Matched Control", "Presenilin-1"],
            }
        )
        metadata = pd.DataFrame(
            {
                "model": ["Presenilin1"],
                "model_type": ["Familial AD"],
                "study_synid": ["syn61849889"],
                "modified_gene": ["PSEN1"],
                "ensembl_gene_id": ["ENSCJAG00000021617"],
                "allele_type": ["Endonuclease-mediated"],
            }
        )
        return genotype_map, metadata

    def test_duplicate_model_genotype_raises(self):
        """A duplicate (model, genotype) pair would multiply points within a model."""
        genotype_map, metadata = self._metadata_inputs()
        genotype_map = pd.concat(
            [genotype_map, genotype_map.iloc[[0]]], ignore_index=True
        )

        with pytest.raises(ValueError, match=r"duplicate \(model, genotype\)"):
            _validate_and_prepare_model_metadata(genotype_map, metadata)

    def test_label_map_model_absent_from_metadata_raises(self):
        """A label-map model with no metadata row would otherwise lose its explorer page."""
        genotype_map, metadata = self._metadata_inputs()
        genotype_map = genotype_map.copy()
        genotype_map.loc[0, "model"] = "MismatchedModel"

        with pytest.raises(ValueError, match="not present in marmo_model_metadata"):
            _validate_and_prepare_model_metadata(genotype_map, metadata)

    @pytest.mark.parametrize(
        "column,bad_value,expected_message",
        [
            ("model_type", "Something Else", "multiple model_type"),
            ("study_synid", "syn00000000", "multiple study_synid"),
        ],
        ids=["conflicting model_type", "conflicting study_synid"],
    )
    def test_inconsistent_model_fields_raise(self, column, bad_value, expected_message):
        """A model with more than one modified-gene row must still have a single model_type
        and study_synid; iloc[0] would otherwise pick an arbitrary value."""
        genotype_map, metadata = self._metadata_inputs()
        extra = metadata.copy()
        extra[column] = bad_value
        extra["ensembl_gene_id"] = "ENSCJAG00000000001"
        metadata = pd.concat([metadata, extra], ignore_index=True)

        with pytest.raises(ValueError, match=expected_message):
            _validate_and_prepare_model_metadata(genotype_map, metadata)

    @pytest.mark.parametrize(
        "column",
        ["model_type", "allele_type"],
        ids=["page-level model_type", "genetic_info allele_type"],
    )
    def test_blank_model_metadata_becomes_none(self, column):
        """Blank model_type, study_synid, modified_gene, and allele_type become None rather
        than NaN."""
        genotype_map, metadata = self._metadata_inputs()
        metadata = metadata.copy()
        metadata[column] = metadata[column].astype(object)
        metadata.loc[0, column] = None

        result = _validate_and_prepare_model_metadata(genotype_map, metadata)

        assert result.iloc[0][column] is None


class TestPrepareMeasureInfo:
    def _measure_info(self):
        return pd.DataFrame(
            {
                "result_column": ["Ab40_pg.ml", "Ab_ratio", "GFAP_pg.ml"],
                "evidence_type": ["A&beta;40", "A&beta;42/A&beta;40", "GFAP"],
                "units": ["pg/mL", None, "pg/mL"],
                "display_order": ["1", "2", "3"],
            }
        )

    def test_result_column_is_standardized(self):
        result = _prepare_measure_info(self._measure_info())

        assert list(result["result_column_std"]) == [
            "ab40_pg_ml",
            "ab_ratio",
            "gfap_pg_ml",
        ]

    def test_display_order_is_numeric(self):
        result = _prepare_measure_info(self._measure_info())

        assert pd.api.types.is_numeric_dtype(result["display_order"])
        assert list(result["display_order"]) == [1, 2, 3]

    def test_blank_units_become_empty_string(self):
        result = _prepare_measure_info(self._measure_info())

        assert list(result["units"]) == ["pg/mL", "", "pg/mL"]


class TestBuildMeasurements:
    def _measurement_inputs(self) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
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
                    "qc_ab": ["PASS", "PASS", "PASS", "PASS"],
                    "qc_neuro": ["PASS", "PASS", "PASS", "PASS"],
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

    def test_build_measurements_drops_unknown_individuals(self):
        """A measurement whose individualid is absent from marmo_individual_metadata is dropped: the
        left join yields a null genotype, which the inner genotype-map merge excludes."""
        datasets, measure_info = self._measurement_inputs()

        measurements = _build_measurements(datasets, measure_info)

        assert set(measurements["individualid"]) == {1}

    def test_build_measurements_floors_ages_to_whole_years(self):
        """Ages floor rather than round, so 11.9 months is still the first bucket and 12.0 opens the
        second."""
        datasets, measure_info = self._measurement_inputs()

        measurements = _build_measurements(datasets, measure_info).sort_values(
            "collectionage"
        )

        assert list(measurements["age"]) == ["0-1 years", "0-1 years", "1-2 years"]

    @pytest.mark.parametrize(
        "qc_ab,qc_neuro,expected_measures",
        [
            ("FAIL", "PASS", {"gfap_pg_ml"}),
            ("PASS", "FAIL", {"ab40_pg_ml"}),
        ],
        ids=["ab fails -> only neuro survives", "neuro fails -> only ab survives"],
    )
    def test_build_measurements_applies_qc_masks_before_melt(
        self, qc_ab, qc_neuro, expected_measures
    ):
        """A row's QC-failed group is nulled by _apply_qc_masks and then removed by the melt's
        dropna, while the other group on the same row survives."""
        datasets = {
            "marmo_results": pd.DataFrame(
                {
                    "biomaterialid": ["7015_1"],
                    "individualid": [1],
                    "ab40_pg_ml": [100.0],
                    "gfap_pg_ml": [50.0],
                    "qc_ab": [qc_ab],
                    "qc_neuro": [qc_neuro],
                }
            ),
            "marmo_individual_metadata": pd.DataFrame(
                {"individualid": [1], "genotype": ["WT"], "sex": ["male"]}
            ),
            "marmo_biomaterial_metadata": pd.DataFrame(
                {
                    "biomaterialid": ["7015_1"],
                    "collectionage": [6],
                    "collectionageunits": ["months"],
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
                "result_column_std": ["ab40_pg_ml", "gfap_pg_ml"],
                "evidence_type": ["A&beta;40", "GFAP"],
                "units": ["pg/mL", "pg/mL"],
                "display_order": [1, 3],
            }
        )

        measurements = _build_measurements(datasets, measure_info)

        assert set(measurements["result_column_std"]) == expected_measures


class TestBuildBiomarkers:
    def _measurements(self):
        """Mirrors what _build_measurements emits, including the raw genotype column carried
        over from the individual join. That column must be dropped before display_label is
        renamed to genotype, so the fixture has to carry it for the collision path to be
        exercised at all. Individuals 10 and 2 share the first bucket so that data-point
        ordering within a bucket is observable."""
        return pd.DataFrame(
            {
                "individualid": [10, 2, 2, 1, 1, 2],
                "value": [100.0, 150.0, 200.0, 180.0, 0.1, 0.15],
                "sex": ["Male", "Female", "Female", "Male", "Male", "Female"],
                "genotype": [
                    "WT",
                    "PSEN1-C410Y_Y410/Y410",
                    "PSEN1-C410Y_Y410/Y410",
                    "WT",
                    "WT",
                    "PSEN1-C410Y_Y410/Y410",
                ],
                "display_label": [
                    "Matched Control",
                    "Presenilin-1",
                    "Presenilin-1",
                    "Matched Control",
                    "Matched Control",
                    "Presenilin-1",
                ],
                "evidence_type": [
                    "A&beta;40",
                    "A&beta;40",
                    "A&beta;40",
                    "A&beta;40",
                    "A&beta;42/A&beta;40",
                    "A&beta;42/A&beta;40",
                ],
                "age": [
                    "0-1 years",
                    "0-1 years",
                    "1-2 years",
                    "1-2 years",
                    "0-1 years",
                    "0-1 years",
                ],
                "units": ["pg/mL", "pg/mL", "pg/mL", "pg/mL", "", ""],
                "display_order": [1, 1, 1, 1, 2, 2],
                "age_start": [0, 0, 1, 1, 0, 0],
            }
        )

    @pytest.mark.parametrize(
        "display_orders,expected_order",
        [
            (
                [2, 2, 2, 2, 1, 1],
                [
                    ("A&beta;42/A&beta;40", "0-1 years"),
                    ("A&beta;40", "0-1 years"),
                    ("A&beta;40", "1-2 years"),
                ],
            ),
            (
                [1, 1, 1, 1, 1, 1],
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

    def _measurement_rows(
        self, rows, units="pg/mL", display_order=1, evidence_type="A&beta;40"
    ):
        """Build a measurements frame for one evidence type from (age_start, display_label, value)
        tuples; individualid is assigned per row and raw genotype mirrors the display label."""
        return pd.DataFrame(
            {
                "individualid": list(range(1, len(rows) + 1)),
                "value": [value for _, _, value in rows],
                "sex": ["Male"] * len(rows),
                "genotype": [label for _, label, _ in rows],
                "display_label": [label for _, label, _ in rows],
                "evidence_type": [evidence_type] * len(rows),
                "age": [
                    f"{age_start}-{age_start + 1} years" for age_start, _, _ in rows
                ],
                "units": [units] * len(rows),
                "display_order": [display_order] * len(rows),
                "age_start": [age_start for age_start, _, _ in rows],
            }
        )

    def test_build_biomarkers_drops_single_genotype_bucket(self):
        """A trailing single-genotype bucket is dropped and not backfilled."""
        measurements = self._measurement_rows(
            [
                (0, "Matched Control", 10.0),
                (0, "Presenilin-1", 11.0),
                (1, "Matched Control", 20.0),
            ]
        )

        biomarkers = _build_biomarkers(measurements, "M")

        assert [b["age"] for b in biomarkers] == ["0-1 years"]

    def test_build_biomarkers_y_axis_max_uses_retained_buckets(self):
        """y_axis_max is computed after the filter, so a dropped single-genotype bucket holding the
        largest value does not inflate the retained plot's axis."""
        measurements = self._measurement_rows(
            [
                (0, "Matched Control", 100.0),
                (0, "Presenilin-1", 90.0),
                (1, "Matched Control", 500.0),  # dropped; must not set y_axis_max
            ]
        )

        biomarkers = _build_biomarkers(measurements, "M")

        assert [b["age"] for b in biomarkers] == ["0-1 years"]
        assert biomarkers[0]["y_axis_max"] == round_y_axis_max(100.0)
        assert round_y_axis_max(100.0) != round_y_axis_max(500.0)

    def test_build_biomarkers_all_single_genotype_returns_empty(self):
        """A model whose every bucket is single-genotype yields an empty biomarkers list."""
        measurements = self._measurement_rows(
            [(0, "Matched Control", 10.0), (1, "Matched Control", 20.0)]
        )

        assert _build_biomarkers(measurements, "M") == []

    def test_build_biomarkers_backfills_middle_gap(self):
        """A middle bucket dropped by the genotype filter is backfilled with an empty placeholder."""
        measurements = self._measurement_rows(
            [
                (0, "Matched Control", 10.0),
                (0, "Presenilin-1", 11.0),
                (1, "Matched Control", 20.0),  # single -> dropped -> placeholder
                (2, "Matched Control", 30.0),
                (2, "Presenilin-1", 31.0),
            ]
        )

        biomarkers = _build_biomarkers(measurements, "M")

        assert [(b["age"], b["data"] == []) for b in biomarkers] == [
            ("0-1 years", False),
            ("1-2 years", True),
            ("2-3 years", False),
        ]

    def test_build_biomarkers_backfills_leading_gap(self):
        """A dropped youngest bucket is backfilled so the series still starts at 0-1 years."""
        measurements = self._measurement_rows(
            [
                (0, "Matched Control", 10.0),  # single -> dropped -> placeholder
                (1, "Matched Control", 20.0),
                (1, "Presenilin-1", 21.0),
                (2, "Matched Control", 30.0),
                (2, "Presenilin-1", 31.0),
            ]
        )

        biomarkers = _build_biomarkers(measurements, "M")

        assert [(b["age"], b["data"] == []) for b in biomarkers] == [
            ("0-1 years", True),
            ("1-2 years", False),
            ("2-3 years", False),
        ]


class TestApplyQcMasks:
    """_apply_qc_masks nulls a group's measure values on rows whose QC flag is not PASS. qc_ab
    gates the amyloid-beta measures, qc_neuro the neuro measures; the two are independent."""

    def _results(self, qc_ab, qc_neuro):
        """A wide marmo_results frame with all six measures populated (so masking is observable).
        qc_ab and qc_neuro are per-row flag lists of equal length."""
        n = len(qc_ab)
        data = {
            "biomaterialid": [f"b{i}" for i in range(n)],
            "individualid": list(range(n)),
            "qc_ab": qc_ab,
            "qc_neuro": qc_neuro,
        }
        for offset, column in enumerate(AB_COLS + NEURO_COLS, start=1):
            data[column] = [float(offset)] * n
        return pd.DataFrame(data)

    def test_ab_fail_masks_only_ab_measures(self):
        result = _apply_qc_masks(self._results(qc_ab=["FAIL"], qc_neuro=["PASS"]))

        assert result.loc[0, AB_COLS].isna().all()
        assert result.loc[0, NEURO_COLS].notna().all()

    def test_neuro_fail_masks_only_neuro_measures(self):
        result = _apply_qc_masks(self._results(qc_ab=["PASS"], qc_neuro=["FAIL"]))

        assert result.loc[0, NEURO_COLS].isna().all()
        assert result.loc[0, AB_COLS].notna().all()

    def test_both_fail_masks_all_measures(self):
        result = _apply_qc_masks(self._results(qc_ab=["FAIL"], qc_neuro=["FAIL"]))

        assert result.loc[0, AB_COLS + NEURO_COLS].isna().all()

    def test_all_pass_masks_nothing(self):
        result = _apply_qc_masks(self._results(qc_ab=["PASS"], qc_neuro=["PASS"]))

        assert result.loc[0, AB_COLS + NEURO_COLS].notna().all()

    @pytest.mark.parametrize(
        "blank", [None, "", "   "], ids=["none", "empty", "whitespace"]
    )
    def test_blank_qc_is_treated_as_not_pass(self, blank):
        """A blank/NaN flag counts as not-passing, so its group is masked."""
        result = _apply_qc_masks(self._results(qc_ab=[blank], qc_neuro=["PASS"]))

        assert result.loc[0, AB_COLS].isna().all()
        assert result.loc[0, NEURO_COLS].notna().all()

    def test_pending_is_masked(self):
        result = _apply_qc_masks(self._results(qc_ab=["PENDING"], qc_neuro=["PASS"]))

        assert result.loc[0, AB_COLS].isna().all()

    @pytest.mark.parametrize("passing", ["PASS", "pass", "Pass", " PASS ", "pass "])
    def test_pass_is_case_and_whitespace_insensitive(self, passing):
        result = _apply_qc_masks(self._results(qc_ab=[passing], qc_neuro=[passing]))

        assert result.loc[0, AB_COLS + NEURO_COLS].notna().all()

    def test_absent_measure_columns_in_a_group_are_skipped(self):
        """Only the Ab columns actually present are masked; the missing ones raise no KeyError."""
        frame = pd.DataFrame(
            {
                "biomaterialid": ["b0"],
                "individualid": [0],
                "ab40_pg_ml": [100.0],  # ab42_pg_ml / ab_ratio absent
                "gfap_pg_ml": [10.0],
                "nfl_pg_ml": [20.0],
                "ttau_fg_ml": [30.0],
                "qc_ab": ["FAIL"],
                "qc_neuro": ["PASS"],
            }
        )

        result = _apply_qc_masks(frame)

        assert pd.isna(result.loc[0, "ab40_pg_ml"])
        assert result.loc[0, NEURO_COLS].notna().all()

    def test_group_with_no_measure_columns_needs_no_qc_column(self):
        """When a group has no measure columns present, it is skipped before its QC column is read,
        so a frame carrying only the other group's columns does not raise."""
        frame = pd.DataFrame(
            {
                "biomaterialid": ["b0"],
                "individualid": [0],
                "gfap_pg_ml": [10.0],
                "qc_neuro": ["FAIL"],  # no ab measures and no qc_ab column at all
            }
        )

        result = _apply_qc_masks(frame)

        assert pd.isna(result.loc[0, "gfap_pg_ml"])

    def test_does_not_mutate_input_and_preserves_other_columns(self):
        original = self._results(qc_ab=["FAIL"], qc_neuro=["PASS"])
        snapshot = original.copy(deep=True)

        result = _apply_qc_masks(original)

        pd.testing.assert_frame_equal(original, snapshot)
        for column in ["biomaterialid", "individualid", "qc_ab", "qc_neuro"]:
            assert list(result[column]) == list(original[column])

    def test_masks_each_row_independently(self):
        result = _apply_qc_masks(
            self._results(qc_ab=["PASS", "FAIL"], qc_neuro=["FAIL", "PASS"])
        )

        assert result.loc[0, AB_COLS].notna().all()
        assert result.loc[0, NEURO_COLS].isna().all()
        assert result.loc[1, AB_COLS].isna().all()
        assert result.loc[1, NEURO_COLS].notna().all()


class TestDropSingleGenotypeBuckets:
    """_drop_single_genotype_buckets keeps only (evidence_type, age) buckets whose display_label
    has >= 2 unique values."""

    def _measurements(self, rows):
        """rows: (evidence_type, age, display_label) tuples."""
        return pd.DataFrame(rows, columns=["evidence_type", "age", "display_label"])

    def test_two_genotype_bucket_is_kept(self):
        measurements = self._measurements(
            [
                ("A&beta;40", "0-1 years", "Matched Control"),
                ("A&beta;40", "0-1 years", "Presenilin-1"),
            ]
        )

        assert len(_drop_single_genotype_buckets(measurements)) == 2

    def test_repeated_single_genotype_bucket_is_dropped(self):
        measurements = self._measurements(
            [
                ("A&beta;40", "0-1 years", "Matched Control"),
                ("A&beta;40", "0-1 years", "Matched Control"),
            ]
        )

        assert _drop_single_genotype_buckets(measurements).empty

    def test_keeps_two_genotype_drops_single(self):
        measurements = self._measurements(
            [
                ("A&beta;40", "0-1 years", "Matched Control"),
                ("A&beta;40", "0-1 years", "Presenilin-1"),
                ("A&beta;40", "1-2 years", "Presenilin-1"),
            ]
        )

        result = _drop_single_genotype_buckets(measurements)

        assert set(zip(result["evidence_type"], result["age"])) == {
            ("A&beta;40", "0-1 years")
        }

    def test_buckets_are_keyed_by_evidence_type_and_age(self):
        """A single-genotype bucket in one evidence type does not affect a two-genotype bucket at the
        same age in another."""
        measurements = self._measurements(
            [
                ("A&beta;40", "0-1 years", "Matched Control"),  # single -> drop
                ("GFAP", "0-1 years", "Matched Control"),  # GFAP 0-1 has two -> keep
                ("GFAP", "0-1 years", "Presenilin-1"),
            ]
        )

        result = _drop_single_genotype_buckets(measurements)

        assert set(zip(result["evidence_type"], result["age"])) == {
            ("GFAP", "0-1 years")
        }

    def test_empty_input_returns_empty(self):
        measurements = pd.DataFrame(columns=["evidence_type", "age", "display_label"])

        assert _drop_single_genotype_buckets(measurements).empty

    def test_all_single_genotype_returns_empty(self):
        measurements = self._measurements(
            [
                ("A&beta;40", "0-1 years", "Matched Control"),
                ("GFAP", "1-2 years", "Presenilin-1"),
            ]
        )

        assert _drop_single_genotype_buckets(measurements).empty


class TestFillAgeGaps:
    """_fill_age_gaps backfills, per evidence type, every age bucket from 0-1 years up to the oldest
    retained one with an empty-data placeholder."""

    def _grouped(
        self,
        age_starts,
        evidence_type="A&beta;40",
        units="pg/mL",
        display_order=1,
        y_axis_max=100.0,
    ):
        """One retained row per age_start, each with a non-empty data list."""
        return pd.DataFrame(
            [
                {
                    "name": "M",
                    "evidence_type": evidence_type,
                    "age": f"{age_start}-{age_start + 1} years",
                    "units": units,
                    "display_order": display_order,
                    "age_start": age_start,
                    "y_axis_max": y_axis_max,
                    "data": [{"individual_id": "1", "value": 1.0}],
                }
                for age_start in age_starts
            ]
        )

    def _placeholder_age_starts(self, result):
        return set(result[result["data"].apply(lambda d: d == [])]["age_start"])

    def test_contiguous_from_zero_is_unchanged(self):
        result = _fill_age_gaps(self._grouped([0, 1, 2]))

        assert sorted(result["age_start"]) == [0, 1, 2]
        assert self._placeholder_age_starts(result) == set()

    def test_interior_gap_is_filled_with_placeholder(self):
        result = _fill_age_gaps(
            self._grouped([0, 1, 3], units="pg/mL", display_order=2, y_axis_max=200.0)
        )

        assert sorted(result["age_start"]) == [0, 1, 2, 3]
        placeholder = result[result["age_start"] == 2].iloc[0]
        assert placeholder["data"] == []
        assert placeholder["age"] == "2-3 years"
        assert placeholder["name"] == "M"
        assert placeholder["evidence_type"] == "A&beta;40"
        assert placeholder["units"] == "pg/mL"
        assert placeholder["display_order"] == 2
        assert placeholder["y_axis_max"] == 200.0

    def test_multiple_gaps_filled(self):
        result = _fill_age_gaps(self._grouped([0, 3]))

        assert self._placeholder_age_starts(result) == {1, 2}

    def test_leading_gap_filled_trailing_not(self):
        result = _fill_age_gaps(self._grouped([2, 3, 5]))

        assert sorted(result["age_start"]) == [0, 1, 2, 3, 4, 5]
        assert self._placeholder_age_starts(result) == {0, 1, 4}

    def test_per_evidence_type_independence_and_single_bucket_padding(self):
        grouped = pd.concat(
            [
                self._grouped([0], evidence_type="A"),
                self._grouped([2], evidence_type="B"),
            ],
            ignore_index=True,
        )

        result = _fill_age_gaps(grouped)

        a_ages = sorted(result[result["evidence_type"] == "A"]["age_start"])
        b_rows = result[result["evidence_type"] == "B"]
        assert a_ages == [0]  # single bucket already at 0 -> no fill
        assert sorted(b_rows["age_start"]) == [
            0,
            1,
            2,
        ]  # single bucket at 2 -> padded with 0, 1
        assert self._placeholder_age_starts(b_rows) == {0, 1}
