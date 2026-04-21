"""
This file contains tests for utility functions used in Model-AD transforms.
"""

import pandas as pd
import numpy as np
import pytest

from agoradatatools.etl.transform.model_ad_transform_utils import (
    build_transcriptomics_url,
    process_genetic_info,
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


class TestBuildTranscriptomicsUrl:
    """
    This class is for testing the build_transcriptomics_url function for the model_details & model_overview transforms.
    The function takes a pd.Series object (representing a single row from the model_info file) and builds a URL if the
    model has transcriptomics data.
    """

    @pytest.fixture
    def url_test_model(self) -> pd.Series:
        return pd.Series(
            {
                "name": "Model",
                "url_categories_value": "category_string",
                "url_models_value": "model1,model2",
                "transcriptomics": True,
            }
        )

    @pytest.mark.parametrize(
        "false_val",
        [False, None],
        ids=["Pass with False boolean value", "Pass with NA value"],
    )
    def test_build_transcriptomics_url_no_transcriptomics(
        self, false_val: bool, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both None and False as transcriptomics = False, and return None.
        """
        url_test_model["transcriptomics"] = false_val

        url = build_transcriptomics_url(url_test_model)
        assert url is None

    def test_build_transcriptomics_url_all_default_values(
        self, url_test_model: pd.Series
    ) -> None:
        url_test_model["url_categories_value"] = ""
        url_test_model["url_models_value"] = ""

        url = build_transcriptomics_url(url_test_model)
        assert url == "comparison/expression?models=Model"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None],
        ids=["Pass with empty string value", "Pass with NA value"],
    )
    def test_build_transcriptomics_url_default_category(
        self, empty_val: str, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both "" and None/NA as empty values and not have a "categories=..." in the url
        """
        url_test_model["url_categories_value"] = empty_val

        url = build_transcriptomics_url(url_test_model)
        assert url == "comparison/expression?models=model1,model2"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None],
        ids=["Pass with empty string value", "Pass with NA value"],
    )
    def test_build_transcriptomics_url_default_models(
        self, empty_val: str, url_test_model: pd.Series
    ) -> None:
        """
        The function should treat both "" and None/NA as empty values and have just the model name in the URL
        """
        url_test_model["url_models_value"] = empty_val

        url = build_transcriptomics_url(url_test_model)
        assert url == "comparison/expression?categories=category_string&models=Model"

    @pytest.mark.parametrize(
        "missing_key",
        ["name", "url_categories_value", "url_models_value", "transcriptomics"],
        ids=[
            "Fail with missing name column",
            "Fail with missing url_categories_value column",
            "Fail with missing url_models_value column",
            "Fail with missing transcriptomics column",
        ],
    )
    def test_build_transcriptomics_url_missing_field(
        self, missing_key: str, url_test_model: pd.Series
    ) -> None:
        """
        In the transform, the model_info and model_results_info data frames have already been validated to have all the
        required columns to correctly call build_transcriptomics_url. However, we verify anyway that calling the
        function with missing columns will throw errors.
        """
        url_test_model.pop(missing_key)

        # Special case: model["name"] never gets used unless we set the url_models_value to empty
        if missing_key == "name":
            url_test_model["url_models_value"] = ""

        with pytest.raises(KeyError):
            build_transcriptomics_url(url_test_model)


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
            # Should handle both None and np.NaN. The Series with None is cast to dtype=object because otherwise the
            # None is converted to NaN by pandas. Using dtype=object also matches the output of normalizing null values
            # by converting NaN to None, which is what happens in the transforms.
            (pd.Series([1234, np.NaN]), pd.Series(["001234", ""])),
            (pd.Series([1234, None], dtype="object"), pd.Series(["001234", ""])),
            # Empty series should return an empty series
            (pd.Series(), pd.Series()),
            # White space should be stripped before padding
            (pd.Series(["  1234", "123 "]), pd.Series(["001234", "000123"])),
            # Empty or all-whitespace strings are treated as missing
            (pd.Series(["", "   ", "\t"]), pd.Series(["", "", ""])),
            # IDs longer than 6 characters should stay as-is
            (pd.Series(["123456", "12345678"]), pd.Series(["123456", "12345678"])),
            (pd.Series([123456, 12345678]), pd.Series(["123456", "12345678"])),
            (pd.Series([123456.0, 12345678.0]), pd.Series(["123456", "12345678"])),
            # Floats are converted to integers before padding
            (pd.Series([123.0, 12345.0]), pd.Series(["000123", "012345"])),
            # Mixed floats and integers all become integers before padding
            (pd.Series([123.0, 12345]), pd.Series(["000123", "012345"])),
            # Floats are converted to integers and padded even when some values are NaN or None
            (pd.Series([1234.0, np.NaN]), pd.Series(["001234", ""])),
            (pd.Series([1234.0, None], dtype="object"), pd.Series(["001234", ""])),
            # Mixed None and NaN values
            (
                pd.Series(["123", None, np.NaN], dtype="object"),
                pd.Series(["000123", "", ""]),
            ),
        ],
        ids=[
            "Pass with all integer input",
            "Pass with all string input",
            "Pass with integer and NaN values",
            "Pass with integer and None values",
            "Pass with empty series",
            "Pass with numeric strings containing extra whitespace",
            "Pass with empty and all-whitespace strings",
            "Pass with numeric strings longer than 6 characters",
            "Pass with integer values longer than 6 characters",
            "Pass with float values longer than 6 characters",
            "Pass with float values that are whole numbers",
            "Pass with mixed float and integer values",
            "Pass with float and NaN values",
            "Pass with float and None values",
            "Pass with mixed None and NaN values",
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

    @pytest.mark.parametrize(
        "input_ids, error_type",
        [
            (pd.Series(["abc", "123"]), ValueError),  # Non-numeric string
            # Non-integer float inside a string throws a ValueError when trying to convert to Int64,
            # rather than the TypeError that is thrown when casting a plain non-integer float value
            (pd.Series(["1234.5", "123"]), ValueError),
            (pd.Series([123.45, 678.90]), TypeError),  # Non-integer floats
            (pd.Series([1234, "1234"]), TypeError),  # Mixed data types
        ],
        ids=[
            "Fail with non-numeric string input",
            "Fail with non-integer float string input",
            "Fail with non-integer float input",
            "Fail with mixed data types input",
        ],
    )
    def test_zero_pad_jax_ids_should_fail(
        self, input_ids: pd.Series, error_type: ValueError | TypeError
    ) -> None:
        """
        Tests that the function throws an ValueError or TypeError when given non-numeric input or non-castable input.
        """
        match_str = (
            "invalid literal for int\\(\\)"
            if error_type == ValueError
            else "cannot safely cast non-equivalent object to int64"
        )
        with pytest.raises(error_type, match=match_str):
            zero_pad_jax_ids(input_ids)
