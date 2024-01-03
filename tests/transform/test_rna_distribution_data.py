import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import rna_distribution


class TestTransformRnaDistributionData:
    data_files_path = "tests/test_assets/rna_distribution_data"
    pass_test_data = [
        (  # pass with good data
            "test_rna_distribution_data_good_input.csv",
            "rna_distribution_data_good_output.json",
        ),
        (  # pass with missing data
            "test_rna_distribution_data_missing_values.csv",
            "rna_distribution_data_missing_data_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with missing data",
    ]
    fail_test_data = [
        (  # Fail with a TypeError due to string value in logfc
            "test_rna_distribution_data_bad_input_typeerror.csv",
            TypeError,
        ),
        (  # Fail with a KeyError due to too many missing values
            "test_rna_distribution_data_bad_input_keyerror.csv",
            KeyError,
        ),
    ]
    fail_test_ids = [
        "Fail with bad data type",
        "Fail with too many missing values",
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_rna_distribution_data_should_pass(
        self, input_file, expected_output_file
    ):
        input_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file))
        output_df = rna_distribution.transform_rna_distribution_data(
            datasets={"diff_exp_data": input_df}
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_file, error_type", fail_test_data, ids=fail_test_ids
    )
    def test_transform_rna_distribution_data_should_fail(self, input_file, error_type):
        with pytest.raises(error_type):
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file)
            )
            rna_distribution.transform_rna_distribution_data(
                datasets={"diff_exp_data": input_df}
            )
