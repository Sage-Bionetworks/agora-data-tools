import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import overall_scores


class TestTransformOverallScores:
    data_files_path = "tests/test_assets/overall_scores"
    pass_test_data = [
        (  # Pass with good data
            "test_overall_scores_good_input.csv",
            "overall_scores_good_output.json",
        ),
        (  # Pass with score or isscored values missing
            "test_overall_scores_missing_input.csv",
            "overall_scores_missing_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with missing score or isscored values",
    ]
    fail_test_data = [
        # No failure cases for this transform
    ]
    fail_test_ids = [
        # No failure cases for this transform
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_overall_scores_should_pass(
        self, input_file, expected_output_file
    ):
        # Note: overall_scores data is read from a Synapse table, so the index is actually supplied
        # by Synapse instead of being numbers from 0..N. For test input, there are real index
        # values from the Synapse table written to the first column of the CSV file, and they are
        # retrieved with the 'index_col=0' argument, to try and be as close to the real data as
        # possible.
        scores_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", input_file), index_col=0
        )
        output_df = overall_scores.transform_overall_scores(df=scores_df)

        # We have to call reset_index() because the JSON file is read in and assigned indices from
        # 0..N, and assert_frame_equal() will fail if the two dataframes have different indices.
        # Even if we were to let the input have indices from 0..N, removing duplicates causes the
        # numbers to not be sequential, which is also an issue for assert_frame_equal().
        output_df = output_df.reset_index(drop=True)

        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file)
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    """
    # Leaving code stub for failure case, in case we want to add this in the future
    @pytest.mark.parametrize("input_file", fail_test_data, ids=fail_test_ids)
    def test_transform_overall_scores_should_fail(self, input_file):
        with pytest.raises(<Error type>):
            scores_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file), index_col=0)
            overall_scores.transform_overall_scores(df=scores_df)
    """
