import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import proteomics_distribution


# NOTE: This test's input is structured a little differently than the other transform
# tests because we may have up to 3 input files with specific dataset names but the
# test should work with the current 2 we support, and won't require modifying the
# test functions to add SRM data later. This structure also lets us test what happens
# when we input a file with an unsupported proteomics type.
class TestTransformProteomicsDistributionData:
    data_files_path = "tests/test_assets/proteomics_distribution_data"
    pass_test_data = [
        (  # pass with good data
            {
                "proteomics": "test_proteomics_distribution_lfq_good_input.csv",
                "proteomics_tmt": "test_proteomics_distribution_tmt_good_input.csv",
            },
            "proteomics_distribution_good_output.json",
        ),
        (  # pass with imperfect data
            {
                "proteomics": "test_proteomics_distribution_lfq_missing_input.csv",
                "proteomics_tmt": "test_proteomics_distribution_tmt_missing_input.csv",
            },
            "proteomics_distribution_missing_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data (LFQ and TMT)",
        "Pass with missing data",
    ]
    fail_test_data = [
        (  # fail with bad data type
            {
                "proteomics": "test_proteomics_distribution_lfq_wrong_data_type.csv",
                "proteomics_tmt": "test_proteomics_distribution_tmt_good_input.csv",
            },
            KeyError,
        ),
        (  # fail with all NA data
            {
                "proteomics": "test_proteomics_distribution_lfq_good_input.csv",
                "proteomics_tmt": "test_proteomics_distribution_tmt_bad_input.csv",
            },
            KeyError,
        ),
        (  # fail with bad name
            {
                "bad_name": "test_proteomics_distribution_lfq_good_input.csv",
                "proteomics_tmt": "test_proteomics_distribution_tmt_good_input.csv",
            },
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with wrong data type",
        "Fail with all NA tissue names",
        "Fail with bad data label",
    ]

    @pytest.mark.parametrize(
        "input_file_dict, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_proteomics_distribution_data_should_pass(
        self, input_file_dict, expected_output_file
    ):
        datasets = {}
        for key, filename in input_file_dict.items():
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", filename)
            )
            datasets[key] = input_df

        output_df = proteomics_distribution.transform_proteomics_distribution_data(
            datasets=datasets
        )
        output_df = output_df.reset_index(drop=True)  # Necessary so indexes match

        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_file_dict, error_type", fail_test_data, ids=fail_test_ids
    )
    def test_transform_proteomics_distribution_data_should_fail(
        self, input_file_dict, error_type
    ):
        with pytest.raises(error_type):
            datasets = {}
            for key, filename in input_file_dict.items():
                input_df = pd.read_csv(
                    os.path.join(self.data_files_path, "input", filename)
                )
                datasets[key] = input_df

            proteomics_distribution.transform_proteomics_distribution_data(
                datasets=datasets
            )
