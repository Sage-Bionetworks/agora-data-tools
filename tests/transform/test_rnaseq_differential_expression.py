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
        (  # Fail with bad data type
            {"diff_exp_data": "test_rnaseq_differential_expression_bad_input.csv"},
            TypeError,
            None,
        ),
        (  # Fail with missing dataset
            {},
            ValueError,
            "Missing required datasets",
        ),
        (  # Fail with missing required column
            {
                "diff_exp_data": "test_rnaseq_differential_expression_missing_column_input.csv"
            },
            ValueError,
            "Missing required columns",
        ),
    ]
    fail_test_ids = [
        "Fail with bad data type",
        "Fail with missing dataset",
        "Fail with missing required column",
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

    @pytest.mark.parametrize(
        "input_datasets, error_type, error_match", fail_test_data, ids=fail_test_ids
    )
    def test_transform_rnaseq_differential_expression_should_fail(
        self, input_datasets, error_type, error_match
    ):
        with pytest.raises(error_type, match=error_match):
            datasets = {}
            for dataset_name, file_name in input_datasets.items():
                datasets[dataset_name] = pd.read_csv(
                    os.path.join(self.data_files_path, "input", file_name)
                )
            rnaseq_differential_expression.transform_rnaseq_differential_expression(
                datasets=datasets
            )
