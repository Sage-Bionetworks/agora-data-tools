import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import team_info


class TestTransformTeamInfo:
    data_files_path = "tests/test_assets/team_info"
    pass_test_data = [
        (  # Pass with good data
            "team_info_good_input.csv",
            "team_member_info_good_input.csv",
            "team_info_good_test_output.json",
        ),
        # For the following 3 cases, we do NOT expect to ever have missing data
        # in any column except for "url", because this data is hand-curated.
        # We are testing this scenario anyway, just in case.
        (  # Pass with missing values in team_info file
            "team_info_missing_input.csv",
            "team_member_info_good_input.csv",
            "team_info_missing_ti_test_output.json",
        ),
        (  # Pass with missing values in team_member_info file
            "team_info_good_input.csv",
            "team_member_info_missing_input.csv",
            "team_info_missing_tmi_test_output.json",
        ),
        (  # Pass with missing values in both input files
            "team_info_missing_input.csv",
            "team_member_info_missing_input.csv",
            "team_info_missing_both_test_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with missing values in team_info file",
        "Pass with missing values in team_member_info file",
        "Pass with missing values in both input files",
    ]
    fail_test_data = [
        # No failure cases for this transform
    ]
    fail_test_ids = [
        # No failure cases for this transform
    ]

    @pytest.mark.parametrize(
        "team_info_file, team_member_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_team_info_should_pass(
        self, team_info_file, team_member_file, expected_output_file
    ):
        team_info_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", team_info_file)
        )
        team_member_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", team_member_file)
        )
        output_df = team_info.transform_team_info(
            datasets={"team_info": team_info_df, "team_member_info": team_member_df}
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    """
    # Leaving code stub for failure case, in case we want to add this in the future
    @pytest.mark.parametrize("team_info_file, team_member_file", fail_test_data, ids=fail_test_ids)
    def test_transform_team_info_should_fail(self, team_info_file, team_member_file):
        with pytest.raises(<Error type>):
            team_info_df = pd.read_csv(os.path.join(self.data_files_path, "input", team_info_file))
            team_member_df = pd.read_csv(os.path.join(self.data_files_path, "input", team_member_file))
            team_info.transform_team_info(
                datasets={"team_info": team_info_df, "team_member_info": team_member_df}
            )
    """
