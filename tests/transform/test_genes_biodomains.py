import os

import pandas as pd
import pytest
import logging

from agoradatatools.etl.transform import genes_biodomains

logger = logging.getLogger(__name__)


class TestCountGroupedTotal:
    df = pd.DataFrame(
        {
            "col_1": ["a", "a", "a", "b", "c", "c", "c"],  # 3 'Ensembl IDs'
            "col_2": ["x", "y", "z", "x", "y", "z", "z"],  # 3 'biodomains'
            "col_3": ["1", "1", "2", "3", "2", "1", "3"],  # 3 'go_terms'
            "col_4": ["m", "m", "n", "n", "o", "o", "o"],  # An ignored column
        }
    )

    # How many unique "col_2"'s per unique "col_1" value?
    def test_count_grouped_total_one_group(self):
        expected_df = pd.DataFrame({"col_1": ["a", "b", "c"], "output": [3, 1, 2]})
        counted = genes_biodomains.count_grouped_total(
            df=self.df, grouping="col_1", input_colname="col_2", output_colname="output"
        )
        assert counted.equals(expected_df)

    # How many unique "col_3"'s per unique combination of "col_1" + "col_2"?
    def test_count_grouped_total_two_groups(self):
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "a", "a", "b", "c", "c"],
                "col_2": ["x", "y", "z", "x", "y", "z"],
                "output": [1, 1, 1, 1, 1, 2],
            }
        )

        counted = genes_biodomains.count_grouped_total(
            df=self.df,
            grouping=["col_1", "col_2"],
            input_colname="col_3",
            output_colname="output",
        )
        assert counted.equals(expected_df)


class TestTransformBiodomains:
    data_files_path = "tests/test_assets/data_files"
    test_data = [
        ("biodomains_test_input.csv", "genes_biodomains.json", True),
        (
            "biodomains_test_input_bad_but_should_pass.csv",
            "genes_biodomains_bad_output_but_should_pass.json",
            True,
        ),
        ("biodomains_test_input_bad_should_fail.csv", None, False),
    ]

    @pytest.mark.parametrize("input_file, expected_output_file, expect_pass", test_data)
    def test_transform_biodomains(self, input_file, expected_output_file, expect_pass):
        if expect_pass:
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file)
            )
            output_df = genes_biodomains.transform_genes_biodomains(
                datasets={"genes_biodomains": input_df}
            )
            expected_df = pd.read_json(
                os.path.join(self.data_files_path, "output", expected_output_file),
            )
            pd.testing.assert_frame_equal(output_df, expected_df)
            message = f"Execution on {input_file} was successful and matches the expected output."
            logger.info(message)
        else:
            try:
                input_df = pd.read_csv(
                    os.path.join(self.data_files_path, "input", input_file)
                )
                genes_biodomains.transform_genes_biodomains(
                    datasets={"genes_biodomains": input_df}
                )
            except KeyError as e:
                message = (
                    f"Execution on {input_file} results in KeyError {e} as expected."
                )
                logger.info(message)
