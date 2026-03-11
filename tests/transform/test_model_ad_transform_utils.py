"""
This file contains tests for utility functions used in Model-AD transforms.
"""

import pandas as pd
import pytest
import numpy as np

from agoradatatools.etl.transform.model_ad_transform_utils import (
    build_gene_expression_url,
    process_genetic_info,
    preprocess_model_info,
    zero_pad_jax_ids,
)


class TestProcessGeneticInfo:
    """Test class for the process_genetic_info function."""

    @pytest.fixture
    def basic_human_transgene_allele_map(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "mgi_allele_id": [2672831, 1930937],
                "gene_symbol": ["APP", "PSEN1"],
                "ensembl_id": ["ENSG00000142192", "ENSG00000080815"],
            }
        )

    @pytest.fixture
    def basic_allele_info_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "name": ["Model1", "Model2"],
                "gene": ["App", "Psen1"],
                "mgi_gene_id": [11820, 19164],
                "gene_ensembl_id": ["ENSMUSG00000022892", "ENSMUSG00000019969"],
                "allele": ["APP_transgenic", "PSEN1_transgenic"],
                "allele_type": ["Transgenic", "Transgenic"],
                "mgi_allele_id": [2672831, 1930937],
            }
        )

    @pytest.fixture
    def basic_expected_output(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "name": ["Model1", "Model2"],
                "gene": ["APP", "PSEN1"],
                "mgi_gene_id": [11820, 19164],
                "allele": ["APP_transgenic", "PSEN1_transgenic"],
                "allele_type": ["Transgenic", "Transgenic"],
                "mgi_allele_id": [2672831, 1930937],
                "ensembl_gene_id": ["ENSG00000142192", "ENSG00000080815"],
            }
        )

    def test_process_genetic_info_should_pass(
        self,
        basic_human_transgene_allele_map: pd.DataFrame,
        basic_allele_info_df: pd.DataFrame,
        basic_expected_output: pd.DataFrame,
    ) -> None:
        # Transform data
        output = process_genetic_info(
            basic_human_transgene_allele_map, basic_allele_info_df
        )

        # Compare output with expected
        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_process_genetic_info_with_no_human_matches(
        self, basic_allele_info_df: pd.DataFrame, basic_expected_output: pd.DataFrame
    ) -> None:
        # Create test input DataFrames with no matching human transgenes
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [9999999],  # Different MGI ID
                "gene_symbol": ["DifferentGene"],
                "ensembl_id": ["ENSG00000000000"],
            }
        )

        # Most output is the same, but keep mouse genes instead of human genes
        basic_expected_output["gene"] = ["App", "Psen1"]
        basic_expected_output["ensembl_gene_id"] = [
            "ENSMUSG00000022892",
            "ENSMUSG00000019969",
        ]

        # Transform data
        output = process_genetic_info(
            human_transgene_allele_map_df, basic_allele_info_df
        )

        # Compare output with expected
        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_process_genetic_info_with_empty_input(self) -> None:
        # Create empty test input DataFrames
        human_transgene_allele_map_df = pd.DataFrame(
            columns=["mgi_allele_id", "gene_symbol", "ensembl_id"]
        )
        allele_info = pd.DataFrame(
            columns=[
                "name",
                "gene",
                "mgi_gene_id",
                "gene_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
            ]
        )

        # Expected output - empty data frame
        expected_output = pd.DataFrame(
            columns=[
                "name",
                "gene",
                "mgi_gene_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
                "ensembl_gene_id",
            ]
        )

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, allele_info)

        # Compare output with expected
        pd.testing.assert_frame_equal(output, expected_output)

    def test_process_genetic_info_case_insensitive_mapping(
        self,
        basic_human_transgene_allele_map: pd.DataFrame,
        basic_allele_info_df: pd.DataFrame,
        basic_expected_output: pd.DataFrame,
    ) -> None:
        # Create test input DataFrames with different gene casing to ensure that the gene names
        # match between data frames no matter what the casing is
        basic_human_transgene_allele_map["gene_symbol"] = ["APP", "psen1"]

        basic_expected_output["gene"] = ["APP", "psen1"]

        # Transform data
        output = process_genetic_info(
            basic_human_transgene_allele_map, basic_allele_info_df
        )

        # Compare output with expected
        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_process_genetic_info_mouse_and_human(
        self,
        basic_human_transgene_allele_map: pd.DataFrame,
        basic_allele_info_df: pd.DataFrame,
        basic_expected_output: pd.DataFrame,
    ) -> None:
        # Test a model with both human and mouse genes altered. Model2 should have a row for human
        # PSEN1 and a row for mouse Psen1 in this scenario.
        new_row = pd.DataFrame(
            {
                "name": ["Model2"],
                "gene": ["Psen1"],
                "mgi_gene_id": [19164],
                "gene_ensembl_id": ["ENSMUSG00000019969"],
                "allele": ["Psen1_Mouse"],
                "allele_type": ["Targeted"],
                "mgi_allele_id": [123456],
            }
        )

        basic_allele_info_df = pd.concat([basic_allele_info_df, new_row])

        expected_new_row = pd.DataFrame(
            {
                "name": ["Model2"],
                "gene": ["Psen1"],
                "mgi_gene_id": [19164],
                "allele": ["Psen1_Mouse"],
                "allele_type": ["Targeted"],
                "mgi_allele_id": [123456],
                "ensembl_gene_id": ["ENSMUSG00000019969"],
            }
        )

        basic_expected_output = pd.concat(
            [basic_expected_output, expected_new_row], ignore_index=True
        )

        output = process_genetic_info(
            basic_human_transgene_allele_map, basic_allele_info_df
        )

        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_process_gene_info_keeps_extra_data_columns(
        self,
        basic_human_transgene_allele_map: pd.DataFrame,
        basic_allele_info_df: pd.DataFrame,
        basic_expected_output: pd.DataFrame,
    ) -> None:
        # Any extra columns added to the human allele_map or allele_info df should stay in the data frame
        basic_human_transgene_allele_map["human_extra"] = [123, 456]

        basic_allele_info_df["allele_extra"] = [789, 1011]

        basic_expected_output["human_extra"] = [123, 456]
        basic_expected_output["allele_extra"] = [789, 1011]

        # Put columns in the right order
        basic_expected_output = basic_expected_output[
            [
                "name",
                "gene",
                "mgi_gene_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
                "allele_extra",
                "human_extra",
                "ensembl_gene_id",
            ]
        ]

        output = process_genetic_info(
            basic_human_transgene_allele_map, basic_allele_info_df
        )

        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_process_genetic_info_removes_duplicates(
        self,
        basic_human_transgene_allele_map: pd.DataFrame,
        basic_allele_info_df: pd.DataFrame,
        basic_expected_output: pd.DataFrame,
    ) -> None:
        # Create a situation where the first human transgene is duplicated, which results in Model1
        # having duplicate allele entries, and Model2 has a duplicate entry in allele_info
        human_transgene_allele_map = basic_human_transgene_allele_map.loc[[0, 0, 1]]
        allele_info_df = basic_allele_info_df.loc[[0, 1, 1]]

        # We have to call reset_index() so output's index matches basic_expected_output's index or
        # the assert will fail even though the data is the same.
        output = process_genetic_info(
            human_transgene_allele_map, allele_info_df
        ).reset_index(drop=True)

        pd.testing.assert_frame_equal(output, basic_expected_output)


class TestBuildGeneExpressionUrl:
    """
    This class is for testing the build_gene_expression_url function for the model_details transform. The function takes
    a pd.Series object (representing a single row from the model_info file) and builds a URL if the model has gene
    expression data.
    """

    @pytest.fixture
    def url_test_model(self) -> pd.Series:
        return pd.Series(
            {
                "name": "Model",
                "url_categories_value": "category_string",
                "url_models_value": "model1,model2",
                "gene_expression": True,
            }
        )

    @pytest.mark.parametrize(
        "false_val",
        [False, None, np.nan],
        ids=[
            "Pass with False boolean value",
            "Pass with None value",
            "Pass with NaN value",
        ],
    )
    def test_build_gene_expression_url_no_gene_expression(
        self, false_val: bool, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both None and False as gene_expression = False, and return None.
        """
        url_test_model["gene_expression"] = false_val

        url = build_gene_expression_url(url_test_model)
        assert url is None

    def test_build_gene_expression_url_all_default_values(
        self, url_test_model: pd.Series
    ) -> None:
        url_test_model["url_categories_value"] = None
        url_test_model["url_models_value"] = None

        url = build_gene_expression_url(url_test_model)
        assert url == "comparison/expression?models=Model"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None, np.nan],
        ids=[
            "Pass with empty string value",
            "Pass with None value",
            "Pass with NaN value",
        ],
    )
    def test_build_gene_expression_url_default_category(
        self, empty_val: str, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both "" and None/NA as empty values and not have a "categories=..." in the url
        """
        url_test_model["url_categories_value"] = empty_val

        url = build_gene_expression_url(url_test_model)
        assert url == "comparison/expression?models=model1,model2"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None, np.NaN],
        ids=[
            "Pass with empty string value",
            "Pass with None value",
            "Pass with NaN value",
        ],
    )
    def test_build_gene_expression_url_default_models(
        self, empty_val: str, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both "" and None/NA as empty values and have just the model name in the URL
        """
        url_test_model["url_models_value"] = empty_val

        url = build_gene_expression_url(url_test_model)
        assert url == "comparison/expression?categories=category_string&models=Model"

    @pytest.mark.parametrize(
        "missing_key",
        ["name", "url_categories_value", "url_models_value", "gene_expression"],
        ids=[
            "Fail with missing name column",
            "Fail with missing url_categories_value column",
            "Fail with missing url_models_value column",
            "Fail with missing gene_expression column",
        ],
    )
    def test_build_gene_expression_url_missing_field(
        self, missing_key: str, url_test_model: pd.Series
    ) -> None:
        """
        In the transform, the model_info and model_results_info data frames have already been validated to have all the
        required columns to correctly call build_gene_expression_url. However, we verify anyway that calling the
        function with missing columns will throw errors.
        """
        url_test_model.pop(missing_key)

        # Special case: model["name"] never gets used unless we set the url_models_value to empty
        if missing_key == "name":
            url_test_model["url_models_value"] = None

        with pytest.raises(KeyError):
            build_gene_expression_url(url_test_model)


class TestPreprocessModelInfo:
    """
    This class is for testing the preprocess_model_info function, which takes the model_info and model_results_info
    dataframes and merges them together, while also doing some preprocessing on the data (like converting certain string
    columns to lists, and zero-padding the jax_id column).

    Some of the operations in this function use util functions that have their own tests (zero-padding jax_id, changing
    matched_controls and aliases to lists of strings, and normalizing NaN values), so we don't test those operations in
    this class beyond making sure that those columns are altered as expected in the output.
    """

    @pytest.fixture
    def basic_model_info_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "name": ["Model1", "Model2"],
                "matched_controls": ["Control1", "Control2,Control3"],
                "model_type": ["Type1", "Type2"],
                "contributing_group": ["Group1", "Group2"],
                "study_synid": ["syn1234", "syn5678"],
                "rrid": ["RRID1", "RRID2"],
                "jax_id": ["1234", "5678"],
                "alzforum_id": ["AlzForumID1", "AlzForumID2"],
                "genotype": ["Genotype1", "Genotype2"],
                "aliases": ["Alias1,Alias2", "Alias3"],
                "url_categories_value": ["Category1", "Category2"],
                "url_models_value": ["Model1", "Model2"],
            }
        )

    @pytest.fixture
    def basic_model_results_info_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "name": ["Model1", "Model2"],
                "gene_expression": [True, False],
                "disease_correlation": [True, True],
                "pathology": [False, True],
                "biomarkers": [False, False],
            }
        )

    @pytest.fixture
    def basic_expected_output_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "name": ["Model1", "Model2"],
                "matched_controls": [["Control1"], ["Control2", "Control3"]],
                "model_type": ["Type1", "Type2"],
                "contributing_group": ["Group1", "Group2"],
                "study_synid": ["syn1234", "syn5678"],
                "rrid": ["RRID1", "RRID2"],
                "jax_id": ["001234", "005678"],
                "alzforum_id": ["AlzForumID1", "AlzForumID2"],
                "genotype": ["Genotype1", "Genotype2"],
                "aliases": [["Alias1", "Alias2"], ["Alias3"]],
                "url_categories_value": ["Category1", "Category2"],
                "url_models_value": ["Model1", "Model2"],
                "gene_expression": [True, False],
                "disease_correlation": [True, True],
                "pathology": [False, True],
                "biomarkers": [False, False],
            }
        )

    def test_preprocess_model_info_should_pass(
        self,
        basic_model_info_df: pd.DataFrame,
        basic_model_results_info_df: pd.DataFrame,
        basic_expected_output_df: pd.DataFrame,
    ) -> None:
        """
        This test case tests a basic merge for 2 models with no missing values. "matched_controls" and "aliases" should
        be converted to lists, "jax_id" should be zero-padded to 6 digits, and all columns from both data frames should
        be present.
        """
        output = preprocess_model_info(basic_model_info_df, basic_model_results_info_df)

        pd.testing.assert_frame_equal(output, basic_expected_output_df)

    def test_process_model_info_passes_with_no_results_info(
        self, basic_model_info_df: pd.DataFrame, basic_expected_output_df: pd.DataFrame
    ) -> None:
        """
        Tests that the function works when model_results_info_df is not provided. model_info data should still be
        adjusted as expected but be missing the boolean columns that exist in model_results_info_df.
        """
        output = preprocess_model_info(basic_model_info_df)

        basic_expected_output_df = basic_expected_output_df.drop(
            columns=[
                "gene_expression",
                "disease_correlation",
                "pathology",
                "biomarkers",
            ]
        )

        pd.testing.assert_frame_equal(output, basic_expected_output_df)

    def test_preprocess_model_info_replaces_missing_values(
        self,
        basic_model_info_df: pd.DataFrame,
        basic_model_results_info_df: pd.DataFrame,
        basic_expected_output_df: pd.DataFrame,
    ) -> None:
        """
        Test that different columns have their missing values replaced with the correct filler values (None, "", or
        False depending on the column).

        Column checks:
            gene_expression, disease_correlation, pathology, biomarkers: None/NaN -> False
            rrid, alzforum_id: None/NaN -> ""
            all other columns: NaN -> None
        """
        basic_model_info_df["rrid"] = [np.NaN, None]
        basic_model_info_df["alzforum_id"] = [np.NaN, None]

        # Ignore jax_id, matched_controls, and aliases as missing values for these are tested in their own test suites
        other_columns = [
            "model_type",
            "contributing_group",
            "study_synid",
            "genotype",
            "url_categories_value",
            "url_models_value",
        ]
        basic_model_info_df.loc[0, other_columns] = np.NaN

        boolean_columns = [
            "gene_expression",
            "disease_correlation",
            "pathology",
            "biomarkers",
        ]
        basic_model_results_info_df.loc[0, boolean_columns] = np.NaN
        basic_model_results_info_df.loc[1, boolean_columns] = None

        basic_expected_output_df[boolean_columns] = False
        basic_expected_output_df["rrid"] = ["", ""]
        basic_expected_output_df["alzforum_id"] = ["", ""]
        basic_expected_output_df.loc[0, other_columns] = None

        output = preprocess_model_info(basic_model_info_df, basic_model_results_info_df)

        pd.testing.assert_frame_equal(output, basic_expected_output_df)


class TestZeroPadJaxIds:
    """
    This class tests the util function zero_pad_jax_id and makes sure that it handles missing values and non-string
    inputs correctly.
    """

    @pytest.mark.parametrize(
        "input_ids, expected_output",
        [
            # Integer input
            (pd.Series([123, 123456, 0]), pd.Series(["000123", "123456", "000000"])),
            # String input
            (
                pd.Series(["123", "123456", "0"]),
                pd.Series(["000123", "123456", "000000"]),
            ),
            # Should handle both None and np.NaN
            (pd.Series([1234, np.NaN, 0]), pd.Series(["001234", "", "000000"])),
            (pd.Series([1234, None, 0]), pd.Series(["001234", "", "000000"])),
            # Empty strings shouldn't get padded
            (pd.Series([1234, ""]), pd.Series(["001234", ""])),
            # Empty series should return an empty series
            (pd.Series(), pd.Series()),
            # White space should be stripped before padding
            (pd.Series(["  1234", "123 ", "  "]), pd.Series(["001234", "000123", ""])),
            # IDs longer than 6 characters should stay as-is
            (pd.Series(["1234567", "12345678"]), pd.Series(["1234567", "12345678"])),
        ],
    )
    def test_zero_pad_jax_id_should_pass(
        self, input_ids: pd.Series, expected_output: pd.Series
    ) -> None:
        """
        Tests that the function works with multiple different kinds of input. It should work on integers or strings, and
        both np.NaN and None should be converted to empty strings.
        """
        output = zero_pad_jax_ids(input_ids)

        pd.testing.assert_series_equal(output, expected_output)
