import os
import pandas as pd
import json
import pytest

from agoradatatools.etl.transform import distribution_data


class TestTransformDistributionData:
    data_files_path = "tests/test_assets/distribution_data"
    param_set_1 = {
        "overall_max_score": 5,
        "genetics_max_score": 3,
        "omics_max_score": 2,
    }
    param_set_2 = {
        "overall_max_score": 8,
        "genetics_max_score": 6,
        "omics_max_score": 5,
    }

    pass_test_data = [
        (  # Pass with good data on param set 1
            "test_distribution_data_good_input.csv",
            "distribution_data_good_output_1.json",
            param_set_1,
        ),
        (  # Pass with good data on param set 2
            "test_distribution_data_good_input.csv",
            "distribution_data_good_output_2.json",
            param_set_2,
        ),
        (  # Pass with values missing from each column
            "test_distribution_data_missing_input.csv",
            "distribution_data_missing_output.json",
            param_set_1,
        ),
    ]
    pass_test_ids = [
        "Pass with good data on parameter set 1",
        "Pass with good data on parameter set 2",
        "Pass with missing values in each column",
    ]
    fail_test_data = [
        (  # Bad data type
            "test_distribution_data_wrong_data_type_overall.csv",
            param_set_1,
            ValueError,
        ),
        (  # Bad data type
            "test_distribution_data_wrong_data_type_genetics.csv",
            param_set_1,
            ValueError,
        ),
        (  # Bad data type
            "test_distribution_data_wrong_data_type_omics.csv",
            param_set_1,
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with bad data type in overall score column",
        "Fail with bad data type in genetics score column",
        "Fail with bad data type in omics score column",
    ]

    @pytest.mark.parametrize(
        "distribution_data_file, expected_output_file, param_set",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_distribution_data_should_pass(
        self, distribution_data_file, expected_output_file, param_set
    ):
        distribution_data_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", distribution_data_file),
            index_col=0,
        )
        output_dict = distribution_data.transform_distribution_data(
            datasets={"overall_scores": distribution_data_df},
            overall_max_score=param_set["overall_max_score"],
            genetics_max_score=param_set["genetics_max_score"],
            omics_max_score=param_set["omics_max_score"],
        )

        # Writing to JSON changes "bins" from tuples to lists, so output_dict and expected_dict
        # would not be equal since expected_dict is read from JSON. We solve this by turning
        # output_dict into a JSON string and reading back into a dict.
        output_dict = json.loads(json.dumps(output_dict))

        json_file = os.path.join(self.data_files_path, "output", expected_output_file)
        with open(json_file) as file:
            expected_dict = json.load(file)[0]
            assert output_dict == expected_dict

    @pytest.mark.parametrize(
        "distribution_data_file, param_set, error_type",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_distribution_data_should_fail(
        self, distribution_data_file, param_set, error_type
    ):
        with pytest.raises(error_type):
            distribution_data_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", distribution_data_file),
                index_col=0,
            )
            distribution_data.transform_distribution_data(
                datasets={"overall_scores": distribution_data_df},
                overall_max_score=param_set["overall_max_score"],
                genetics_max_score=param_set["genetics_max_score"],
                omics_max_score=param_set["omics_max_score"],
            )
