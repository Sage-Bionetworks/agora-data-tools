"""Integration test for the proteomics LFQ transform.
The transform should successfully filter out proteins that start with "CON__" and should remove
rows that are missing a uniqid value. The only failure case for this transform is when "uniqid"
is not a column in the data frame.
"""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import proteomics


class TestTranformProteomics:
    """Class for testing the transform.
    ADT currently ingests three proteomics data sets (LFQ, TMT, SRM) and runs the transform on each.
    Currently only LFQ data is actually modified by the transform, so the TMT and SRM test data
    should not be changed by the transform.
    """

    data_files_path = "tests/test_assets/proteomics"
    pass_test_data = [
        (  # pass with good data (LFQ)
            "proteomics_lfq_good_input.csv",
            "proteomics_lfq_good_output.json",
        ),
        (  # pass with missing data (LFQ)
            "proteomics_lfq_missing_input.csv",
            "proteomics_lfq_missing_output.json",
        ),
        (  # pass with good data (TMT)
            "proteomics_tmt_good_input.csv",
            "proteomics_tmt_good_output.json",
        ),
        (  # pass with good data (SRM)
            "proteomics_srm_good_input.csv",
            "proteomics_srm_good_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data (LFQ)",
        "Pass with missing data (LFQ)",
        "Pass with good data (TMT)",
        "Pass with good data (SRM)",
    ]
    fail_test_data = [
        "proteomics_lfq_no_uniqid_input.csv",
    ]
    fail_test_ids = [
        "Fail with missing uniqid column",
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_proteomics_should_pass(
        self, input_file: str, expected_output_file: str
    ) -> None:
        """Passing conditions: "CON__" proteins and proteins with NA uniqids are removed, all
        other rows are left intact.
        """
        input_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file))

        # reset_index is necessary because the index values need to match the expected output, but
        # if rows are removed from the output, the index values will differ.
        output_df = proteomics.transform_proteomics(df=input_df).reset_index(drop=True)
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize("input_file", fail_test_data, ids=fail_test_ids)
    def test_transform_proteomics_should_fail(self, input_file: str) -> None:
        """Failure condition: "uniqid" is not a column in the data frame.
        This should throw a KeyError.
        """
        with pytest.raises(KeyError):
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file)
            )
            proteomics.transform_proteomics(df=input_df)
