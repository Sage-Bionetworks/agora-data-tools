import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.modelAD_general_transform import (
    modelAD_general_transform,
)


class TestTransformGeneralModelAD:
    data_files_path = "tests/test_assets/modelAD_general_transform"
    pass_test_data = [
        (
            # Pass with good fake data
            "modelAD_general_transform_good_test_input.csv",
            "modelAD_general_transform_good_test_output.json",
        ),
        (
            # Pass with duplicated data
            "modelAD_general_transform_duplicated_input.csv",
            "modelAD_general_transform_duplicated_output.json",
        ),
        (
            # Pass with none data
            "modelAD_general_transform_none_input.csv",
            "modelAD_general_transform_none_output.json",
        ),
        (
            # Pass with missing data
            "modelAD_general_transform_missing_input.csv",
            "modelAD_general_transform_missing_output.json",
        ),
        (
            # Pass with extra column
            "modelAD_general_transform_extra_column.csv",
            "modelAD_general_transform_extra_column_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good fake data",
        "Pass with duplicated data",
        "Pass with none data",
        "Pass with missing data",
        "Pass with extra column",
    ]
    fail_test_data = [("modelAD_general_transform_missing_column.csv")]
    fail_test_ids = [("Fail with missing column")]

    @pytest.mark.parametrize(
        "modelAD_general_transform_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_modelAD_general_transform_should_pass(
        self, modelAD_general_transform_file, expected_output_file
    ):
        modelAD_general_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", modelAD_general_transform_file)
        )
        output_df = pd.DataFrame(
            modelAD_general_transform(
                datasets={"modelAD_general_transform": modelAD_general_transform_df},
                dataset_name="modelAD_general_transform",
            )
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "modelAD_general_transform_file", fail_test_data, ids=fail_test_ids
    )
    def test_modelAD_general_transform_should_fail(
        self, modelAD_general_transform_file, error_type: BaseException = ValueError
    ):
        modelAD_general_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", modelAD_general_transform_file)
        )
        with pytest.raises(error_type):
            modelAD_general_transform(
                datasets={"modelAD_general_transform": modelAD_general_transform_df},
                dataset_name="modelAD_general_transform",
            )
