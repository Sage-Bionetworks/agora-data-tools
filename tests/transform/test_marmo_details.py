import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.marmo_details import transform_marmo_details


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

    def test_marmo_details_excludes_unmapped_and_missing_biospecimen(self):
        """Only mapped genotypes with a biospecimen record are surfaced."""
        datasets = self._load_datasets(self.good_input_files)

        output_data = transform_marmo_details(datasets=datasets)

        data_points = [
            point
            for biomarker in output_data[0]["biomarkers"]
            for point in biomarker["data"]
        ]
        individual_ids = {point["individual_id"] for point in data_points}
        genotypes = {point["genotype"] for point in data_points}

        # individual 3 has an unmapped genotype; individual 4 has no biospecimen record
        assert individual_ids == {"1", "2"}
        assert genotypes == {"Matched Control", "Presenilin-1"}

    def test_marmo_details_ratio_units_are_empty_string(self):
        """The A-beta ratio measure has no units and must serialize as "" (not null)."""
        datasets = self._load_datasets(self.good_input_files)

        output_data = transform_marmo_details(datasets=datasets)

        ratio_objects = [
            biomarker
            for biomarker in output_data[0]["biomarkers"]
            if biomarker["evidence_type"] == "A&beta;42/A&beta;40"
        ]
        assert ratio_objects
        assert all(biomarker["units"] == "" for biomarker in ratio_objects)

    def test_marmo_details_missing_dataset_should_fail(self):
        """A missing required dataset raises ValueError."""
        datasets = self._load_datasets(self.good_input_files)
        del datasets["marmo_results"]

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_missing_column_should_fail(self):
        """A required column missing from a dataset raises ValueError."""
        input_files = dict(self.good_input_files)
        input_files["marmo_results"] = "marmo_results_missing_column_input.csv"
        datasets = self._load_datasets(input_files)

        with pytest.raises(ValueError):
            transform_marmo_details(datasets=datasets)

    def test_marmo_details_multiple_models_should_fail(self):
        """More than one model in marmo_metadata is not yet supported and raises ValueError."""
        input_files = dict(self.good_input_files)
        input_files["marmo_metadata"] = "marmo_metadata_multi_model_input.csv"
        datasets = self._load_datasets(input_files)

        with pytest.raises(ValueError):
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
