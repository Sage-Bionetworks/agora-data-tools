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

    def test_process_genetic_info_should_pass(self) -> None:
        # Create test input DataFrames
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [2672831, 1930937],
                "gene_symbol": ["App", "Psen1"],
                "human_ensembl_id": ["ENSG00000142192", "ENSG00000080815"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt", "Psen1"],
                "gene_ensembl_id": [
                    "ENSMUSG00000022892",
                    "ENSMUSG00000018411",
                    "ENSMUSG00000019969",
                ],
                "allele": [
                    "APP K670_M671delinsNL (Swedish)",
                    "MAPT P301L",
                    "Psen1<sup>tm1Mpm</sup>",
                ],
                "allele_type": ["Transgenic", "Transgenic", "Targeted"],
                "mgi_allele_id": [2672831, 2672831, 1930937],
            }
        )

        # Expected output
        expected_output = [
            {
                "modified_gene": "App",
                "ensembl_gene_id": "ENSG00000142192",  # Human Ensembl ID
                "allele": "APP K670_M671delinsNL (Swedish)",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Mapt",
                "ensembl_gene_id": "ENSMUSG00000018411",  # Mouse Ensembl ID (no human match)
                "allele": "MAPT P301L",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Psen1",
                "ensembl_gene_id": "ENSG00000080815",  # Human Ensembl ID
                "allele": "Psen1<sup>tm1Mpm</sup>",
                "allele_type": "Targeted",
                "mgi_allele_id": 1930937,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_with_no_human_matches(self) -> None:
        # Create test input DataFrames with no matching human transgenes
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [9999999],  # Different MGI ID
                "gene_symbol": ["DifferentGene"],
                "human_ensembl_id": ["ENSG00000000000"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt", "Psen1"],
                "gene_ensembl_id": [
                    "ENSMUSG00000022892",
                    "ENSMUSG00000018411",
                    "ENSMUSG00000019969",
                ],
                "allele": [
                    "APP K670_M671delinsNL (Swedish)",
                    "MAPT P301L",
                    "Psen1<sup>tm1Mpm</sup>",
                ],
                "allele_type": ["Transgenic", "Transgenic", "Targeted"],
                "mgi_allele_id": [2672831, 2672831, 1930937],
            }
        )

        # Expected output - all should keep mouse Ensembl IDs
        expected_output = [
            {
                "modified_gene": "App",
                "ensembl_gene_id": "ENSMUSG00000022892",
                "allele": "APP K670_M671delinsNL (Swedish)",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Mapt",
                "ensembl_gene_id": "ENSMUSG00000018411",
                "allele": "MAPT P301L",
                "allele_type": "Transgenic",
                "mgi_allele_id": 2672831,
            },
            {
                "modified_gene": "Psen1",
                "ensembl_gene_id": "ENSMUSG00000019969",
                "allele": "Psen1<sup>tm1Mpm</sup>",
                "allele_type": "Targeted",
                "mgi_allele_id": 1930937,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_with_empty_input(self) -> None:
        # Create empty test input DataFrames
        human_transgene_allele_map_df = pd.DataFrame(
            columns=["mgi_allele_id", "gene_symbol", "human_ensembl_id"]
        )
        model_alleles = pd.DataFrame(
            columns=[
                "modified_gene",
                "gene_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
            ]
        )

        # Expected output - empty list since no alleles to process
        expected_output = []

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output

    def test_process_genetic_info_case_insensitive_mapping(self) -> None:
        # Create test input DataFrames with different gene casing
        human_transgene_allele_map_df = pd.DataFrame(
            {
                "mgi_allele_id": [1234567, 1234567],
                "gene_symbol": ["APP", "mapt"],  # Upper and lower case in mapping
                "human_ensembl_id": ["ENSG00000123456", "ENSG00000987654"],
            }
        )

        model_alleles = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt"],  # Title case in alleles
                "gene_ensembl_id": [
                    "ENSMUSG00000011111",
                    "ENSMUSG00000022222",
                ],
                "allele": [
                    "APP Example Allele",
                    "MAPT Example Allele",
                ],
                "allele_type": ["Transgenic", "Transgenic"],
                "mgi_allele_id": [1234567, 1234567],
            }
        )

        # Expected output: ENSG IDs should be mapped, gene names should keep original case
        expected_output = [
            {
                "modified_gene": "APP",
                "ensembl_gene_id": "ENSG00000123456",
                "allele": "APP Example Allele",
                "allele_type": "Transgenic",
                "mgi_allele_id": 1234567,
            },
            {
                "modified_gene": "mapt",
                "ensembl_gene_id": "ENSG00000987654",
                "allele": "MAPT Example Allele",
                "allele_type": "Transgenic",
                "mgi_allele_id": 1234567,
            },
        ]

        # Transform data
        output = process_genetic_info(human_transgene_allele_map_df, model_alleles)

        # Compare output with expected
        assert output == expected_output


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
        [False, None],
        ids=[
            "Pass with False boolean value",
            "Pass with None value",
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
        ["", None],
        ids=[
            "Pass with empty string value",
            "Pass with None value",
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
        ["", None],
        ids=[
            "Pass with empty string value",
            "Pass with None value",
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
                "model": ["Model1", "Model2"],
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
                "model": ["Model1", "Model2"],
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
                "model": ["Model1", "Model2"],
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

    def test_preprocess_model_info_changes_model_name_col(
        self,
        basic_model_info_df: pd.DataFrame,
        basic_model_results_info_df: pd.DataFrame,
        basic_expected_output_df: pd.DataFrame,
    ) -> None:
        """
        Tests that the function works when the model name column is different than the default "model".
        """
        basic_model_info_df = basic_model_info_df.rename(columns={"model": "new_name"})
        basic_model_results_info_df = basic_model_results_info_df.rename(
            columns={"model": "new_name"}
        )
        basic_expected_output_df = basic_expected_output_df.rename(
            columns={"model": "new_name"}
        )

        output = preprocess_model_info(
            basic_model_info_df, basic_model_results_info_df, model_name_col="new_name"
        )

        assert "new_name" in output.columns
        pd.testing.assert_frame_equal(output, basic_expected_output_df)

    def test_preprocess_model_info_passes_with_empty_data_frames(
        self,
        basic_model_info_df: pd.DataFrame,
        basic_model_results_info_df: pd.DataFrame,
        basic_expected_output_df: pd.DataFrame,
    ) -> None:
        """
        Tests that the function works when one or both data frames are empty.
        """
        empty_model_info = pd.DataFrame(columns=basic_model_info_df.columns)
        empty_model_results_info = pd.DataFrame(
            columns=basic_model_results_info_df.columns
        )

        # model_info empty, merged with model_results_info that has data in it
        output = preprocess_model_info(empty_model_info, basic_model_results_info_df)
        assert output.empty

        # model_info_empty, no merge
        output = preprocess_model_info(empty_model_info)
        assert output.empty

        # both data frames are empty
        output = preprocess_model_info(empty_model_info, empty_model_results_info)
        assert output.empty

        # model_info has data, merged with empty model_results_info. Output will have data but all the boolean columns
        # that come from model_results_info should be False
        output = preprocess_model_info(basic_model_info_df, empty_model_results_info)
        basic_expected_output_df[
            ["gene_expression", "disease_correlation", "pathology", "biomarkers"]
        ] = False

        pd.testing.assert_frame_equal(output, basic_expected_output_df)

    def test_preprocess_model_info_fails_with_duplicate_models(
        self,
        basic_model_info_df: pd.DataFrame,
        basic_model_results_info_df: pd.DataFrame,
    ) -> None:
        """
        Tests that a ValueError is thrown when the model_info DataFrame has two rows for the same model, and that a
        MergeError is thrown when model_results_info has duplicate rows for the same model.
        """
        model_info_df = basic_model_info_df.loc[
            [0, 1, 1],
        ]  # Duplicated row

        with pytest.raises(ValueError, match="model_info has duplicated rows"):
            preprocess_model_info(model_info_df)

        model_results_df = basic_model_results_info_df.loc[
            [0, 1, 1],
        ]  # Duplicated row

        with pytest.raises(
            pd.errors.MergeError, match="Merge keys are not unique in right dataset"
        ):
            preprocess_model_info(basic_model_info_df, model_results_df)


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
            # Floats are converted to integers before padding
            (pd.Series([123.0, 12345.0]), pd.Series(["000123", "012345"])),
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
