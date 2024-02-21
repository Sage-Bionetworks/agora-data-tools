""" Integration tests for the genes_biodomains transform """
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import genes_biodomains


class TestCountGroupedTotal:
    """Tests the count_grouped_total method two ways: grouping by one column only, and grouping by two columns."""

    df = pd.DataFrame(
        {
            "col_1": ["a", "a", "a", "b", "c", "c", "c"],  # 3 'Ensembl IDs'
            "col_2": ["x", "y", "z", "x", "y", "z", "z"],  # 3 'biodomains'
            "col_3": ["1", "1", "2", "3", "2", "1", "3"],  # 3 'go_terms'
            "col_4": ["m", "m", "n", "n", "o", "o", "o"],  # An ignored column
        }
    )

    # How many unique "col_2"'s per unique "col_1" value?
    def test_count_grouped_total_one_group(self) -> None:
        expected_df = pd.DataFrame({"col_1": ["a", "b", "c"], "output": [3, 1, 2]})
        counted = genes_biodomains.count_grouped_total(
            df=self.df, grouping="col_1", input_colname="col_2", output_colname="output"
        )
        assert counted.equals(expected_df)

    # How many unique "col_3"'s per unique combination of "col_1" + "col_2"?
    def test_count_grouped_total_two_groups(self) -> None:
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


class TestTransformGenesBiodomains:
    """Tests the genes_biodomains custom transform with 'perfect' input, input with missing data, and data that results
    in all rows being dropped from the data frame (which causes a failure)."""

    data_files_path = "tests/test_assets/genes_biodomains"
    pass_test_data = [
        (  # pass with good data
            "biodomains_test_input.csv",
            "genes_biodomains.json",
        ),
        (  # pass with imperfect data
            "biodomains_test_input_bad_but_should_pass.csv",
            "genes_biodomains_bad_output_but_should_pass.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data",
        "Pass with imperfect data",
    ]
    fail_test_data = [
        "biodomains_test_input_bad_should_fail.csv",  # fail with bad data
    ]
    fail_test_ids = [
        "Fail with bad data",
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_genes_biodomains_should_pass(
        self, input_file: str, expected_output_file: str
    ) -> None:
        input_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file))
        output_df = genes_biodomains.transform_genes_biodomains(
            datasets={"genes_biodomains": input_df}
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize("input_file", fail_test_data, ids=fail_test_ids)
    def test_transform_genes_biodomains_should_fail(self, input_file: str) -> None:
        with pytest.raises(
            ValueError, match="cannot insert ensembl_gene_id, already exists"
        ):
            input_df = pd.read_csv(
                os.path.join(self.data_files_path, "input", input_file)
            )
            genes_biodomains.transform_genes_biodomains(
                datasets={"genes_biodomains": input_df}
            )
