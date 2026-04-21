import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import genes_biodomains


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


class TestTransformGenesBiodomains:
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
        (  # Fail with bad data
            {"genes_biodomains": "biodomains_test_input_bad_should_fail.csv"},
            ValueError,
            "cannot insert ensembl_gene_id, already exists",
        ),
        (  # Fail with missing dataset
            {},
            ValueError,
            "Missing required datasets",
        ),
        (  # Fail with missing required column
            {"genes_biodomains": "biodomains_test_input_missing_column.csv"},
            ValueError,
            "Missing required columns",
        ),
    ]
    fail_test_ids = [
        "Fail with bad data",
        "Fail with missing dataset",
        "Fail with missing required column",
    ]

    @pytest.mark.parametrize(
        "input_file, expected_output_file", pass_test_data, ids=pass_test_ids
    )
    def test_transform_genes_biodomains_should_pass(
        self, input_file, expected_output_file
    ):
        input_df = pd.read_csv(os.path.join(self.data_files_path, "input", input_file))
        output_df = genes_biodomains.transform_genes_biodomains(
            datasets={"genes_biodomains": input_df}
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_datasets, error_type, error_match", fail_test_data, ids=fail_test_ids
    )
    def test_transform_genes_biodomains_should_fail(
        self, input_datasets, error_type, error_match
    ):
        with pytest.raises(error_type, match=error_match):
            datasets = {}
            for dataset_name, file_name in input_datasets.items():
                datasets[dataset_name] = pd.read_csv(
                    os.path.join(self.data_files_path, "input", file_name)
                )
            genes_biodomains.transform_genes_biodomains(datasets=datasets)
