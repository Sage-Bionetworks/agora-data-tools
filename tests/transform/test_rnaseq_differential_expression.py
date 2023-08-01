import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import rnaseq_differential_expression


class TestTransformRnaseqDifferentialExpression:
    data_files_path = "tests/test_assets/rnaseq_differential_expression"
    pass_test_data = [
        (  # pass with good data
            "test_rnaseq_differential_expression_good_input.csv",
            "rnaseq_differential_expression_good_output.json",
        ),
        (  # pass with missing data
            "test_rnaseq_differential_expression_missing_values.csv",
            "rnaseq_differential_expression_missing_data_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with missing data",
    ]
    fail_test_data = [
        "test_rnaseq_differential_expression_bad_input.csv",  # fail with bad data
    ]
    fail_test_ids = [
        "Fail with bad data type",
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_rnaseq_differential_expression_should_pass(
        self, input_file, expected_output_file
    ):
        input_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file))
        output_df = (
            rnaseq_differential_expression.transform_rnaseq_differential_expression(
                datasets={"diff_exp_data": input_df}
            )
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize("input_file", fail_test_data, ids=fail_test_ids)
    def test_transform_rnaseq_differential_expression_should_fail(self, input_file):
        with pytest.raises(TypeError):
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file)
            )
            rnaseq_differential_expression.transform_rnaseq_differential_expression(
                datasets={"diff_exp_data": input_df}
            )
