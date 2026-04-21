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
        (  # Fail with missing team_member_info dataset
            {"team_info": "team_info_good_input.csv"},
            ValueError,
            "Missing required datasets",
        ),
        (  # Fail with missing required column in team_info
            {
                "team_info": "team_info_missing_team_column_input.csv",
                "team_member_info": "team_member_info_good_input.csv",
            },
            ValueError,
            "Missing required columns",
        ),
        (  # Fail with missing required column in team_member_info
            {
                "team_info": "team_info_good_input.csv",
                "team_member_info": "team_member_info_missing_team_column_input.csv",
            },
            ValueError,
            "Missing required columns",
        ),
    ]
    fail_test_ids = [
        "Fail with missing team_member_info dataset",
        "Fail with missing required column in team_info",
        "Fail with missing required column in team_member_info",
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

    @pytest.mark.parametrize(
        "input_datasets, error_type, error_match", fail_test_data, ids=fail_test_ids
    )
    def test_transform_team_info_should_fail(
        self, input_datasets, error_type, error_match
    ):
        with pytest.raises(error_type, match=error_match):
            datasets = {}
            for dataset_name, file_name in input_datasets.items():
                datasets[dataset_name] = pd.read_csv(
                    os.path.join(self.data_files_path, "input", file_name)
                )
            team_info.transform_team_info(datasets=datasets)
