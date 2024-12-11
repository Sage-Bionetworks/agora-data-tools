"""Integration test for the gene_info transform.

This transform requires 13 different input datasets and tests several conditions in each one. Description and passing
or failing input for each dataset:
    gene_metadata: collection of information like gene symbol, aliases, etc about each Ensembl ID in every dataset.
                   Both the "alias" and "ensembl_possible_replacements" fields are lists of strings, so this dataset
                   needs to be saved in feather format and cannot be written as a csv file.
        passing input: any field can have missing values, so test file has rows with missing values covering each
                       column, including ensembl_gene_id. There should be no duplicate Ensembl IDs.
        failing input: duplicate Ensembl IDs should throw a MergeError
    igap: list of genes that are associated with AD as determined by GWAS
        passing input: either field can have missing values, so test file has a row with a missing hgnc_symbol. Ensembl
                       IDs should be unique.
        failing input: duplicate Ensembl IDs should throw a MergeError
    eqtl: list of genes that are associated with AD as determined by eQTL meta-analysis
        passing input: either field can have missing values, so test file has a row with a missing has_eqtl value.
                       Ensembl IDs should be unique.
        failing input: duplicate Ensembl IDs should throw a MergeError
    proteomics: differential expression for proteomics LFQ data
        passing input: any field can have missing values. Rows that are missing at least one of "log2_fc", "ci_upr",
                       "ci_lwr", or "cor_pval" are dropped, so there are several rows missing at least one of these
                       values in the test file. "cor_pval" must be numeric. The full LFQ dataset has data for multiple
                       tissues, so the test file contains several rows with the same Ensembl ID but different tissue
                       values to replicate that.
        failing input: a string value in the "cor_pval" column should throw a TypeError.
    diff_exp_data: differential expression for RNA-seq data
        passing input: any field can have missing values, however for this dataset we assume that validation has been
                       done on the dataset before ingest and that "logfc", "ci_l", "ci_r", and "adj_p_val" have no
                       missing values, so we do not test missing values in these columns. Missing values are not handled
                       or dropped in the transform. "adj_p_val" must be numeric. Duplicate Ensembl IDs are allowed due
                       to multiple studies, so the test file has rows with the same Ensembl ID but different studies.
        failing input: a string value in the "adj_p_val" column should throw a TypeError.
    proteomics_tmt: differential expression for proteomics TMT data
        passing input: this dataset has identical format to "proteomics", so the same passing rules apply. The full
                       TMT dataset only has data for one tissue, so all rows have the same tissue value in the test
                       file. Duplicate Ensembl IDs are technically allowed, but this doesn't happen (or make sense) in
                       the full data set so we do not test for it.
        failing input: a string value in the "cor_pval" column should throw a TypeError.
    proteomics_srm: differential expression for proteomics SRM data
        passing input: this dataset has identical format to "proteomics", so the same passing rules apply. The full
                       SRM dataset only has data for one tissue, so all rows have the same tissue value in the test
                       file. Duplicate Ensembl IDs are technically allowed, but this doesn't happen (or make sense) in
                       the full data set so we do not test for it.
        failing input: a string value in the "cor_pval" column should throw a TypeError.
    target_list: a list of nomination information like source, justification, etc for Ensembl IDs that are nominated as
                 potential drug targets.
        passing input: any field can be missing, and duplicate Ensembl IDs are allowed, so the test file contains rows
                       with the same Ensembl ID but different sources, and also has rows with missing values in at least
                       one column.
        failing input: none
    median_expression: distribution statistics (min, max, quartiles, etc) of RNA-seq data.
        passing input: technically any field can be missing, but we assume that this data has been validated prior to
                       ingest and has no missing values in any numerical field. We only test the case where the
                       "tissue" value is missing. Duplicate Ensembl IDs are allowed due to multiple tissues, so the test
                       file has several rows with the same Ensembl ID but different tissue value.
        failing input: none
    pharos_classes: information on the pharos class of each gene.
        passing input: any field can be missing, so there are a few rows with missing data in at least one column.
                       Duplicate Ensembl IDs are allowed and does happen in the real dataset, so we test for it.
        failing input: none
    genes_biodomains: a list of Ensembl IDs and their associated biodomains and GO terms.
        passing input: any field can be missing, so the test file has rows with missing data in at least one column.
                       Rows with a missing "biodomain" are dropped in the transform. Duplicate Ensembl IDs are allowed
                       due to association with multiple biodomains and GO terms, so the test file has rows with the same
                       Ensembl ID but different biodomain and/or GO term values.
        failing input: none
    tep_adi_info: a list of Ensembl IDs and whether they are in the AD Informer set, the TEP set, both, or neither.
        passing input: hgnc_symbol must not be missing, but is_adi and is_tep can have missing values. Missing is_adi
                       or is_tep values are assumed to mean "False". These two fields must have boolean values if the
                       data isn't missing. Ensembl IDs should be unique.
        failing input: a missing hgnc_symbol or a string value in is_adi or is_tep should throw a TypeError.
    ensg_to_uniprot_mapping: a list of Ensembl IDs and their associated Uniprot accessions.
        passing input: Duplicate Ensembl IDs are allowed due to association with multiple Uniprot accessions,
                       so the test file has rows with the same Ensembl ID but different Uniprot accession values.
        failing input: none

Other notes about the test files:
    Missing Ensembl IDs: these are allowed in any dataset, and rows with missing IDs will get dropped in the transform.
                         Since all datasets are merged into "gene_metadata" the same way, it is not necessary to put a
                         missing Ensembl ID in every test file. Instead, only "gene_metadata" and "igap" have rows with
                         a missing Ensembl ID to test the merge and drop.
    Overlap of Ensembl IDs: all test files contain only Ensembl IDs that exist in gene_metadata, except for "eqtl".
                            "gene_metadata" should ideally already contain all Ensembl IDs that are present in any
                            data set, but there is an edge case where a new or updated data set containing new IDs is
                            added but "gene_metadata" hasn't been updated to pull those new IDs in. In the transform
                            this results in the "ensembl_info" field being "null" instead of a dictionary of null
                            values. To test that the transform properly turns the null into the correct dictionary
                            format, the test file for "eqtl" contains one Ensembl ID that is not present in
                            "gene_metadata".
"""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import gene_info


class TestTransformGeneInfo:
    """Tests the gene_info transform. This transform requires 12 different data files, so this test class contains a
    util function to read them all in, formatted as transform_gene_info expects.
    """

    data_files_path = "tests/test_assets/gene_info"
    param_set_1 = {
        "adjusted_p_value_threshold": 0.05,
        "protein_level_threshold": 0.05,
    }
    param_set_2 = {
        "adjusted_p_value_threshold": 1,
        "protein_level_threshold": 1,
    }

    core_files = {
        "gene_metadata": "gene_metadata_good_input.feather",
        "igap": "igap_good_input.csv",
        "eqtl": "eqtl_good_input.csv",
        "proteomics": "proteomics_good_input.csv",
        "diff_exp_data": "diff_exp_data_good_input.csv",
        "proteomics_tmt": "proteomics_tmt_good_input.csv",
        "proteomics_srm": "proteomics_srm_good_input.csv",
        "target_list": "target_list_good_input.csv",
        "median_expression": "median_expression_good_input.csv",
        "pharos_classes": "pharos_classes_good_input.csv",
        "genes_biodomains": "genes_biodomains_good_input.csv",
        "tep_adi_info": "tep_adi_info_good_input.csv",
        "ensg_to_uniprot_mapping": "ensg_to_uniprot_mapping_good.tsv",
    }

    pval_error_match_string = "'<=' not supported"
    merge_error_match_string = "Merge keys are not unique"

    pass_test_data = [
        (  # Pass with good data on param set 1
            core_files,
            "gene_info_good_output_1.json",
            param_set_1,
        ),
        (  # Pass with good data on param set 2
            core_files,
            "gene_info_good_output_2.json",
            param_set_2,
        ),
    ]
    pass_test_ids = [
        "Pass with good data on parameter set 1",
        "Pass with good data on parameter set 2",
    ]
    fail_test_data = [
        (  # Duplicate Ensembl IDs in gene_metadata
            core_files,
            {"gene_metadata": "gene_metadata_merge_error.feather"},
            param_set_1,
            pd.errors.MergeError,
            merge_error_match_string,
        ),
        (  # Duplicate Ensembl IDs in igap
            core_files,
            {"igap": "igap_merge_error.csv"},
            param_set_1,
            pd.errors.MergeError,
            merge_error_match_string,
        ),
        (  # Duplicate Ensembl IDs in eqtl
            core_files,
            {"eqtl": "eqtl_merge_error.csv"},
            param_set_1,
            pd.errors.MergeError,
            merge_error_match_string,
        ),
        (  # Bad data type in diff_exp_data
            core_files,
            {"diff_exp_data": "diff_exp_data_type_error.csv"},
            param_set_1,
            TypeError,
            pval_error_match_string,
        ),
        (  # Bad data type in proteomics
            core_files,
            {"proteomics": "proteomics_type_error.csv"},
            param_set_1,
            TypeError,
            pval_error_match_string,
        ),
        (  # Bad data type in proteomics_tmt
            core_files,
            {"proteomics_tmt": "proteomics_tmt_type_error.csv"},
            param_set_1,
            TypeError,
            pval_error_match_string,
        ),
        (  # Bad data type in proteomics_srm
            core_files,
            {"proteomics_srm": "proteomics_srm_type_error.csv"},
            param_set_1,
            TypeError,
            pval_error_match_string,
        ),
        (  # Missing HGNC in tep_adi_info
            core_files,
            {"tep_adi_info": "tep_adi_info_type_error_1.csv"},
            param_set_1,
            TypeError,
            "can only concatenate str",
        ),
        (  # is_adi is a string
            core_files,
            {"tep_adi_info": "tep_adi_info_type_error_2.csv"},
            param_set_1,
            TypeError,
            "'is_adi' column must be 'bool'",
        ),
        (  # is_tep is a string
            core_files,
            {"tep_adi_info": "tep_adi_info_type_error_3.csv"},
            param_set_1,
            TypeError,
            "'is_tep' column must be 'bool'",
        ),
    ]
    fail_test_ids = [
        "Fail with duplicate Ensembl IDs in gene_metadata",
        "Fail with duplicate Ensembl IDs in igap",
        "Fail with duplicate Ensembl IDs in eqtl",
        "Fail with bad data type in diff_exp_data's adj_p_val column",
        "Fail with bad data type in proteomics's cor_pval column",
        "Fail with bad data type in proteomics_tmt's cor_pval column",
        "Fail with bad data type in proteomics_srm's cor_pval column",
        "Fail with missing hgnc_symbol in tep_adi_info",
        "Fail with bad data type in tep_adi_info's is_adi column",
        "Fail with bad data type in tep_adi_info's is_tep column",
    ]

    def read_input_files_dict(self, input_files_dict: dict) -> dict:
        """Utility function to read a dictionary of filenames into a dictionary of data frames. Most files for
        gene_info are in csv format, but the 'gene_metadata' file is in feather format and needs special casing.

        Args:
            input_files_dict: a dictionary where keys are the names of the datasets, as expected by
                               transform_gene_info, and values are the filenames to load

        Returns:
            datasets: a dictionary where the keys are the names of the datasets, as expected by
                       transform_gene_info, and the values are data frames
        """
        datasets = {}
        for key, value in input_files_dict.items():
            filename = os.path.join(self.data_files_path, "input", value)
            if key == "gene_metadata":
                datasets[key] = pd.read_feather(filename)
            elif value.endswith("tsv"):
                datasets[key] = pd.read_csv(filename, sep="\t")
            else:
                datasets[key] = pd.read_csv(filename)

        return datasets

    @pytest.mark.parametrize(
        "input_files_dict, expected_output_file, param_set",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_gene_info_should_pass(
        self, input_files_dict: dict, expected_output_file: str, param_set: dict
    ):
        """
        Test that the transform_gene_info function passes with the given input files and parameters.

        Args:
            input_files_dict: a dictionary where the keys are the names of the datasets, as expected by
                               transform_gene_info, and the values are the filenames to load
            expected_output_file: the filename of the expected output JSON file
            param_set: a dictionary of parameters to pass to transform_gene_info

        Returns:
            None
        """
        datasets = self.read_input_files_dict(input_files_dict)

        output_df = gene_info.transform_gene_info(
            datasets=datasets,
            adjusted_p_value_threshold=param_set["adjusted_p_value_threshold"],
            protein_level_threshold=param_set["protein_level_threshold"],
        )

        # Index needs to be reset because of dropping NA rows
        output_df = output_df.reset_index(drop=True)

        json_file = os.path.join(self.data_files_path, "output", expected_output_file)
        expected_df = pd.read_json(json_file)
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "input_files_dict, failure_case_files_dict, param_set, error_type, error_match_string",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_gene_info_should_fail(
        self,
        input_files_dict: dict,
        failure_case_files_dict: dict,
        param_set: dict,
        error_type: BaseException,
        error_match_string: str,
    ):
        """
        Test that the transform_gene_info function fails with the given input files and parameters.

        Args:
            input_files_dict: a dictionary where the keys are the names of the datasets, as expected by
                               transform_gene_info, and the values are the filenames to load
            failure_case_files_dict: a dictionary where the keys are the names of the datasets with bad data,
                                     and the values are the filenames to load
            param_set: a dictionary of parameters to pass to transform_gene_info
            error_type: the type of error that should be raised
            error_match_string: a string to match against the error message

        Returns:
            None
        """
        # Need to make a copy, otherwise this edits the original dictionary and persists through all the tests
        updated_files_dict = input_files_dict.copy()

        # Any files specified in 'failure_case_files_dict' will replace their default "good" files in input_files_dict
        for key, value in failure_case_files_dict.items():
            updated_files_dict[key] = value

        with pytest.raises(error_type, match=error_match_string):
            datasets = self.read_input_files_dict(updated_files_dict)

            gene_info.transform_gene_info(
                datasets=datasets,
                adjusted_p_value_threshold=param_set["adjusted_p_value_threshold"],
                protein_level_threshold=param_set["protein_level_threshold"],
            )
