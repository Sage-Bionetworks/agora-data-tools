import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import (
    _age_to_year_bucket,
    _build_biomarkers,
    _build_measurements,
    _convert_to_year,
    transform_marmo_details,
)
from agoradatatools.etl.utils import round_y_axis_max


class TestTransformMarmoDetails:
    data_files_path = "tests/test_assets/marmo_details"

    # Input files shared across the "good" pass case and the fail cases.
    good_input_files = {
        "marmo_metadata": "marmo_metadata_good_input.csv",
        "marmo_genotype_label_map": "marmo_genotype_label_map_good_input.csv",
        "marmo_biomarker_measure_info": "marmo_biomarker_measure_info_good_input.csv",
        "marmo_individual_metadata": "marmo_individual_metadata_good_input.csv",
        "marmo_biospecimen_metadata": "marmo_biospecimen_metadata_good_input.csv",
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
        genotype), dropping rows with no biospecimen match, dropping null measurements,
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

    @pytest.mark.parametrize(
        "missing_dataset",
        [
            "marmo_metadata",
            "marmo_genotype_label_map",
            "marmo_biomarker_measure_info",
            "marmo_individual_metadata",
            "marmo_biospecimen_metadata",
            "marmo_results",
        ],
    )
    def test_marmo_details_missing_dataset_should_fail(self, missing_dataset):
        """A missing required dataset raises ValueError, whichever one is absent."""
        datasets = self._load_datasets(self.good_input_files)
        del datasets[missing_dataset]

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_missing_column_should_fail(self):
        """A required column missing from a dataset raises ValueError."""
        input_files = dict(self.good_input_files)
        input_files["marmo_results"] = "marmo_results_missing_column_input.csv"
        datasets = self._load_datasets(input_files)

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

    def test_marmo_details_missing_model_column_should_fail(self):
        """A label map without the required model column raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        datasets["marmo_genotype_label_map"] = datasets[
            "marmo_genotype_label_map"
        ].drop(columns=["model"])

        with pytest.raises(ValueError):
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

    def test_marmo_details_bad_sampling_age_units_should_fail(self):
        """A samplingAgeUnits value other than months fails validation and raises ValueError."""
        input_files = dict(self.good_input_files)
        input_files[
            "marmo_biospecimen_metadata"
        ] = "marmo_biospecimen_metadata_bad_units_input.csv"
        datasets = self._load_datasets(input_files)

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_non_numeric_sampling_age_should_fail(self):
        """A non-numeric samplingage fails the NumericRule and raises ValueError."""
        input_files = dict(self.good_input_files)
        input_files[
            "marmo_biospecimen_metadata"
        ] = "marmo_biospecimen_metadata_bad_age_input.csv"
        datasets = self._load_datasets(input_files)

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_unknown_result_column_should_fail(self):
        """A result_column in the measure-info mapping that is absent from marmo_results
        (e.g. a typo) raises ValueError rather than silently dropping the measure."""
        input_files = dict(self.good_input_files)
        input_files[
            "marmo_biomarker_measure_info"
        ] = "marmo_biomarker_measure_info_typo_input.csv"
        datasets = self._load_datasets(input_files)

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)


class TestConvertToYear:
    """Unit tests for the month-to-year helpers."""

    @pytest.mark.parametrize(
        "months, expected_year",
        [(0, 0), (9.9, 0), (12, 1), (13.0, 1), (24, 2)],
    )
    def test_convert_to_year(self, months, expected_year):
        assert _convert_to_year(months) == expected_year

    @pytest.mark.parametrize(
        "months, expected_bucket",
        [
            (0, "0-1 years"),
            (9.9, "0-1 years"),
            (12, "1-2 years"),
            (13.0, "1-2 years"),
            (24, "2-3 years"),
        ],
    )
    def test_age_to_year_bucket(self, months, expected_bucket):
        assert _age_to_year_bucket(months) == expected_bucket


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
            # individual 9 has no row in marmo_individual_metadata
            "marmo_results": pd.DataFrame(
                {
                    "biomaterialid": ["msdpl-1_A", "msdpl-9_A"],
                    "individualid": [1, 9],
                    "ab40_pg_ml": [100.0, 900.0],
                }
            ),
            "marmo_individual_metadata": pd.DataFrame(
                {
                    "individualid": [1],
                    "genotype": ["WT"],
                    "sex": ["male"],
                }
            ),
            "marmo_biospecimen_metadata": pd.DataFrame(
                {
                    "specimenid": ["msdpl-1_A", "msdpl-9_A"],
                    "samplingage": [6, 9],
                    "samplingageunits": ["months", "months"],
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


class TestBuildBiomarkers:
    """Unit tests for _build_biomarkers covering sort order, y_axis_max, and empty units."""

    def _measurements(self):
        return pd.DataFrame(
            {
                "individualid": [1, 2, 1],
                "value": [100.0, 200.0, 0.1],
                "sex": ["Male", "Female", "Male"],
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
        biomarkers = _build_biomarkers(self._measurements(), "Presenilin1")

        point = biomarkers[0]["data"][0]
        assert set(point.keys()) == {"individual_id", "value", "sex", "genotype"}
