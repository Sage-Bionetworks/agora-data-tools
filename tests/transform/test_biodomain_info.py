import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import biodomain_info


class TestTransformBiodomainInfo:
    data_files_path = "tests/test_assets/biodomain_info"
    pass_test_data = [
        (  # Pass with good data
            "biodomain_info_good_input.csv",
            "biodomain_info_good_output.json",
        ),
        (  # Pass with values missing from each column
            "biodomain_info_imperfect_input.csv",
            "biodomain_info_imperfect_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with missing values in each column",
    ]
    fail_test_data = [
        (  # Fail with missing dataset
            {},
            ValueError,
            "Missing required datasets",
        ),
        (  # Fail with missing required column
            {"genes_biodomains": "biodomain_info_missing_column_input.csv"},
            ValueError,
            "Missing required columns",
        ),
    ]
    fail_test_ids = [
        "Fail with missing dataset",
        "Fail with missing required column",
    ]

    @pytest.mark.parametrize(
        "biodomain_info_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_biodomain_info_should_pass(
        self, biodomain_info_file, expected_output_file
    ):
        biodomain_info_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", biodomain_info_file)
        )
        output_df = biodomain_info.transform_biodomain_info(
            datasets={"genes_biodomains": biodomain_info_df}
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file)
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_datasets, error_type, error_match", fail_test_data, ids=fail_test_ids
    )
    def test_transform_biodomain_info_should_fail(
        self, input_datasets, error_type, error_match
    ):
        with pytest.raises(error_type, match=error_match):
            datasets = {}
            for dataset_name, file_name in input_datasets.items():
                datasets[dataset_name] = pd.read_csv(
                    os.path.join(self.data_files_path, "input", file_name)
                )
            biodomain_info.transform_biodomain_info(datasets=datasets)
