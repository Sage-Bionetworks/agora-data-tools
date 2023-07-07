import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import proteomics_distribution


class TestTransformProteomicsDistributionData:
    data_files_path = "tests/test_assets/proteomics_distribution_data"
    pass_test_data = [
        (  # pass with good data
            "test_proteomics_distribution_lfq_good_input.csv",
            "test_proteomics_distribution_tmt_good_input.csv",
            "proteomics_distribution_good_output.json",
        ),
        (  # pass with imperfect data
            "test_proteomics_distribution_lfq_missing_input.csv",
            "test_proteomics_distribution_tmt_missing_input.csv",
            "proteomics_distribution_missing_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data (LFQ and TMT)",
        "Pass with missing data",
    ]
    fail_test_data = [
        (
            "test_proteomics_distribution_lfq_wrong_data_type.csv",  # fail with bad data
            "test_proteomics_distribution_tmt_good_input.csv",
            KeyError,
        ),
        (
            "test_proteomics_distribution_lfq_good_input.csv",  # fail with bad data
            "test_proteomics_distribution_tmt_bad_input.csv",
            KeyError,
        ),
    ]
    fail_test_ids = ["Fail with wrong data type", "Fail with all NA tissue names"]

    @pytest.mark.parametrize(
        "input_file_lfq, input_file_tmt, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_proteomics_distribution_data_should_pass(
        self, input_file_lfq, input_file_tmt, expected_output_file
    ):
        input_df_lfq = pd.read_csv(
            os.path.join(self.data_files_path, "input", input_file_lfq)
        )
        input_df_tmt = pd.read_csv(
            os.path.join(self.data_files_path, "input", input_file_tmt)
        )

        output_df = proteomics_distribution.transform_proteomics_distribution_data(
            datasets={"proteomics": input_df_lfq, "proteomics_tmt": input_df_tmt}
        )
        output_df = output_df.reset_index(drop=True) # Necessary so indexes match
        
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_file_lfq, input_file_tmt, error_type", fail_test_data, ids=fail_test_ids
    )
    def test_transform_proteomics_distribution_data_should_fail(
        self, input_file_lfq, input_file_tmt, error_type
    ):
        with pytest.raises(error_type):
            input_df_lfq = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file_lfq)
            )
            input_df_tmt = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file_tmt)
            )
            proteomics_distribution.transform_proteomics_distribution_data(
                datasets={"proteomics": input_df_lfq, "proteomics_tmt": input_df_tmt}
            )
