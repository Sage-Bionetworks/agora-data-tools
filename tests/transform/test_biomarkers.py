import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import biomarkers


class TestTransformBiomarkers:
    data_files_path = "tests/test_assets/biomarkers"
    pass_test_data = [
        (  # Pass with good data
            "biomarkers_good_input.csv",
            "biomarkers_good_output.json",
        )
    ]
    pass_test_ids = ["Pass with good data"]
    fail_test_data = [
        # No failure cases for this transform
    ]
    fail_test_ids = [
        # No failure cases for this transform
    ]

    @pytest.mark.parametrize(
        "biomarkers_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_biomarkers_should_pass(
        self, biomarkers_file, expected_output_file
    ):
        biomarkers_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", biomarkers_file)
        )
        output_df = pd.DataFrame(
            biomarkers.transform_biomarkers(datasets={"biomarkers": biomarkers_df})
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)
