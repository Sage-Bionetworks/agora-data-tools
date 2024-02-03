import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import gene_info


class TestTransformGeneInfo:
    data_files_path = "tests/test_assets/gene_info"
    param_set_1 = {
        "adjusted_p_value_threshold": 0.05,
        "protein_level_threshold": 0.05,
    }
    param_set_2 = {
        "adjusted_p_value_threshold": 1,
        "protein_level_threshold": 1,
    }

    pass_test_data = [
        (  # Pass with good data on param set 1
            {
                "gene_metadata": "gene_metadata_good_input.feather",
                "igap": "igap_good_input.csv",
                "eqtl": "eqtl_good_input.csv",
                "proteomics": "proteomics_good_input.csv",
                "diff_exp_data": "diff_exp_data_good_input.csv",
                "proteomics_tmt": "proteomics_tmt_good_input.csv",
                "proteomics_srm": "proteomics_srm_good_input.csv",
                "target_list": "target_list_good_input.csv",
                "median_expression": "median_expression_good_input.csv",
                "druggability": "druggability_good_input.csv",
                "genes_biodomains": "genes_biodomains_good_input.csv",
                "tep_adi_info": "tep_adi_info_good_input.csv",
            },
            "gene_info_good_output_1.json",
            param_set_1,
        ),
        (  # Pass with good data on param set 2
            {
                "gene_metadata": "gene_metadata_good_input.feather",
                "igap": "igap_good_input.csv",
                "eqtl": "eqtl_good_input.csv",
                "proteomics": "proteomics_good_input.csv",
                "diff_exp_data": "diff_exp_data_good_input.csv",
                "proteomics_tmt": "proteomics_tmt_good_input.csv",
                "proteomics_srm": "proteomics_srm_good_input.csv",
                "target_list": "target_list_good_input.csv",
                "median_expression": "median_expression_good_input.csv",
                "druggability": "druggability_good_input.csv",
                "genes_biodomains": "genes_biodomains_good_input.csv",
                "tep_adi_info": "tep_adi_info_good_input.csv",
            },
            "gene_info_good_output_2.json",
            param_set_2,
        ),
    ]
    pass_test_ids = [
        "Pass with good data on parameter set 1",
        "Pass with good data on parameter set 2",
    ]
    fail_test_data = [
        (  # Bad data type
            "??",
            param_set_1,
            ValueError,
        ),
    ]
    fail_test_ids = [
        "Fail with bad data type in ?? column",
    ]

    def read_input_files_dict(self, input_files_dict):
        """Utility function to read a dictionary of filenames into a dictionary of data frames. Most files for
        gene_info are in csv format, but the 'gene_metadata' file is in feather format and needs special casing.

        Args:
            input_files_dict - a dictionary where keys are the names of the datasets, as expected by
                               transform_gene_info, and values are the filenames to load

        Returns:
            datasets - a dictionary where the keys are the names of the datasets, as expected by
                       transform_gene_info, and the values are data frames
        """
        datasets = {}
        for key, value in input_files_dict.items():
            filename = os.path.join(self.data_files_path, "input", value)
            if key == "gene_metadata":
                datasets[key] = pd.read_feather(filename)
            else:
                datasets[key] = pd.read_csv(filename)

        return datasets

    @pytest.mark.parametrize(
        "input_files_dict, expected_output_file, param_set",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_gene_info_should_pass(
        self, input_files_dict, expected_output_file, param_set
    ):
        datasets = self.read_input_files_dict(input_files_dict)

        output_df = gene_info.transform_gene_info(
            datasets=datasets,
            adjusted_p_value_threshold=param_set["adjusted_p_value_threshold"],
            protein_level_threshold=param_set["protein_level_threshold"],
        )

        json_file = os.path.join(self.data_files_path, "output", expected_output_file)
        expected_df = pd.read_json(json_file)
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_files_dict, param_set, error_type",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_gene_info_should_fail(
        self, input_files_dict, param_set, error_type
    ):
        with pytest.raises(error_type):
            datasets = self.read_input_files_dict(input_files_dict)

            gene_info.transform_gene_info(
                datasets=datasets,
                adjusted_p_value_threshold=param_set["adjusted_p_value_threshold"],
                protein_level_threshold=param_set["protein_level_threshold"],
            )
