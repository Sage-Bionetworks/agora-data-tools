"""
This file contains tests for utility functions used in Model-AD transforms.
"""

import pandas as pd
import numpy as np
import pytest

from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    build_transcriptomics_url,
    process_genetic_info,
    zero_pad_jax_ids,
    validate_jax_ids,
)


class TestProcessGeneticInfo:
    """Test class for the process_genetic_info function."""

    def test_process_genetic_info_should_pass(self) -> None:
        # Pre-merged input: gene_symbol and human_ensembl_id joined in for transgenic alleles
        model_genetic_modifications = pd.DataFrame(
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
                "gene_symbol": ["App", None, "Psen1"],
                "human_ensembl_id": ["ENSG00000142192", None, "ENSG00000080815"],
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

        output = process_genetic_info(model_genetic_modifications)

        assert output == expected_output

    def test_process_genetic_info_with_no_human_matches(self) -> None:
        # No gene_symbol or human_ensembl_id matches — all None
        model_genetic_modifications = pd.DataFrame(
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
                "gene_symbol": [None, None, None],
                "human_ensembl_id": [None, None, None],
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

        output = process_genetic_info(model_genetic_modifications)

        assert output == expected_output

    def test_process_genetic_info_with_empty_input(self) -> None:
        # Empty pre-merged dataframe
        model_genetic_modifications = pd.DataFrame(
            columns=[
                "modified_gene",
                "gene_ensembl_id",
                "allele",
                "allele_type",
                "mgi_allele_id",
                "gene_symbol",
                "human_ensembl_id",
            ]
        )

        expected_output = []

        output = process_genetic_info(model_genetic_modifications)

        assert output == expected_output

    def test_process_genetic_info_case_insensitive_mapping(self) -> None:
        # gene_symbol already has human casing — override should use gene_symbol value
        model_genetic_modifications = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt"],
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
                "gene_symbol": ["APP", "mapt"],
                "human_ensembl_id": ["ENSG00000123456", "ENSG00000987654"],
            }
        )

        # Expected output: ENSG IDs should be mapped, gene names replaced by gene_symbol value
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

        output = process_genetic_info(model_genetic_modifications)

        assert output == expected_output

    def test_process_genetic_info_normalizes_missing_values(self) -> None:
        # The third gene has no human match so gene_ensembl_id is NaN — should normalize to None
        model_genetic_modifications = pd.DataFrame(
            {
                "modified_gene": ["App", "Mapt", "Psen1"],
                "gene_ensembl_id": ["ENSMUSG00000011111", "ENSMUSG00000022222", np.nan],
                "allele": [np.nan, np.nan, np.nan],  # Missing allele names
                "allele_type": [np.nan, np.nan, np.nan],  # Missing allele type
                "mgi_allele_id": [1234567, 2345678, 3456789],
                "gene_symbol": ["APP", "MAPT", None],
                "human_ensembl_id": ["ENSG00000123456", "ENSG00000987654", None],
            },
            dtype="object",
        )

        # Expected output: missing values should be normalized to None, and the first two genes should have their
        # Ensembl IDs replaced with the human ones. The third gene should keep its missing mouse Ensembl ID, which
        # should be normalized to None.
        expected_output = [
            {
                "modified_gene": "APP",
                "ensembl_gene_id": "ENSG00000123456",
                "allele": None,  # Missing values should be normalized to None
                "allele_type": None,
                "mgi_allele_id": 1234567,
            },
            {
                "modified_gene": "MAPT",
                "ensembl_gene_id": "ENSG00000987654",
                "allele": None,
                "allele_type": None,
                "mgi_allele_id": 2345678,
            },
            {
                "modified_gene": "Psen1",
                "ensembl_gene_id": None,
                "allele": None,
                "allele_type": None,
                "mgi_allele_id": 3456789,
            },
        ]

        output = process_genetic_info(model_genetic_modifications)

        assert output == expected_output


class TestBuildTranscriptomicsUrl:
    """
    This class is for testing the build_transcriptomics_url function for the model_details & model_overview transforms.
    The function takes a pd.Series object (representing a single row from the model_info file) and builds a URL if the
    model has transcriptomics data.
    """

    @pytest.mark.parametrize(
        "false_val",
        [False, None],
        ids=["Pass with False boolean value", "Pass with None value"],
    )
    def test_build_transcriptomics_url_no_transcriptomics(
        self, false_val: bool
    ) -> None:
        """
        The function should treat both None and False as transcriptomics = False, and return None.
        """
        model = pd.Series(
            {
                "name": "Model",
                "url_categories_value": "category_string",
                "url_models_value": "model1,model2",
                "transcriptomics": false_val,
            }
        )

        url = build_transcriptomics_url(model)
        assert url is None

    def test_build_transcriptomics_url_all_default_values(self) -> None:
        model = pd.Series(
            {
                "name": "Model",
                "url_categories_value": None,
                "url_models_value": None,
                "transcriptomics": True,
            }
        )

        url = build_transcriptomics_url(model)
        assert url == "comparison/expression?models=Model"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None],
        ids=["Pass with empty string value", "Pass with None value"],
    )
    def test_build_transcriptomics_url_default_category(self, empty_val: str) -> None:
        """
        The function should treat both "" and None as empty values and not have a "categories=..." in the url
        """
        model = pd.Series(
            {
                "name": "Model",
                "url_categories_value": empty_val,
                "url_models_value": "model1,model2",
                "transcriptomics": True,
            }
        )

        url = build_transcriptomics_url(model)
        assert url == "comparison/expression?models=model1,model2"

    @pytest.mark.parametrize(
        "empty_val",
        ["", None],
        ids=["Pass with empty string value", "Pass with None value"],
    )
    def test_build_transcriptomics_url_default_models(self, empty_val: str) -> None:
        """
        The function should treat both "" and None as empty values and have just the model name in the URL
        """
        model = pd.Series(
            {
                "name": "Model",
                "url_categories_value": "category_string",
                "url_models_value": empty_val,
                "transcriptomics": True,
            }
        )

        url = build_transcriptomics_url(model)
        assert url == "comparison/expression?categories=category_string&models=Model"


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
            # Should handle both None and np.nan. The Series with None is cast to dtype=object because otherwise the
            # None is converted to NaN by pandas. Using dtype=object also matches the output of replacing NaN values
            # with None, which is what happens in the transforms.
            (pd.Series([1234, np.nan]), pd.Series(["001234", ""])),
            (pd.Series([1234, None], dtype="object"), pd.Series(["001234", ""])),
            # Empty series should return an empty series
            (pd.Series(), pd.Series()),
            # White space in strings with integers is stripped by the cast operation before padding
            (pd.Series(["  1234", "123 "]), pd.Series(["001234", "000123"])),
            # IDs longer than 6 characters should stay as-is
            (pd.Series(["123456", "12345678"]), pd.Series(["123456", "12345678"])),
            (pd.Series([123456, 12345678]), pd.Series(["123456", "12345678"])),
            (pd.Series([123456.0, 12345678.0]), pd.Series(["123456", "12345678"])),
            # Floats are converted to integers before padding
            (pd.Series([123.0, 12345.0]), pd.Series(["000123", "012345"])),
            # Mixed floats and integers all become integers before padding
            (pd.Series([123.0, 12345]), pd.Series(["000123", "012345"])),
            # Floats are converted to integers and padded even when some values are NaN or None
            (pd.Series([1234.0, np.nan]), pd.Series(["001234", ""])),
            (pd.Series([1234.0, None], dtype="object"), pd.Series(["001234", ""])),
            # Mixed None and NaN values -- None and NaN should both be converted to empty strings
            (
                pd.Series(["123", None, np.nan], dtype="object"),
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
        both np.nan and None should be converted to empty strings.
        """
        output = zero_pad_jax_ids(input_ids)

        pd.testing.assert_series_equal(output, expected_output)

    @pytest.mark.parametrize(
        "input_ids, error_type",
        [
            (pd.Series(["abc", "123"]), ValueError),
            # Non-integer float inside a string throws a ValueError when trying to convert to Int64,
            # rather than the TypeError that is thrown when casting a plain non-integer float value
            (pd.Series(["1234.5", "123"]), ValueError),
            (pd.Series(["", ""]), ValueError),
            (pd.Series([123.45, 678.90]), TypeError),
            (pd.Series([1234, "1234"]), TypeError),
        ],
        ids=[
            "Fail with non-numeric string input",
            "Fail with non-integer float inside string input",
            "Fail with empty string input",
            "Fail with non-integer float input",
            "Fail with mixed data types input",
        ],
    )
    def test_zero_pad_jax_ids_should_fail_on_non_castable_input(
        self, input_ids: pd.Series, error_type: ValueError | TypeError
    ) -> None:
        """
        Tests that the function throws a ValueError or TypeError when given non-numeric input or non-castable input.
        """
        match_str = (
            "invalid literal for int\\(\\)"
            if error_type == ValueError
            else "cannot safely cast non-equivalent object to int64"
        )
        with pytest.raises(error_type, match=match_str):
            zero_pad_jax_ids(input_ids)

    def test_zero_pad_jax_ids_should_fail_on_negative_numbers(self) -> None:
        """
        Tests that the function throws a ValueError when given negative numbers, since Jax IDs should not be negative.
        """
        input_ids = pd.Series([-1, -1234])
        with pytest.raises(
            ValueError, match="Jax IDs must be strings that contain only digits"
        ):
            zero_pad_jax_ids(input_ids)


class TestValidateJaxIds:
    """
    This class tests the validate_jax_ids function to ensure the regex correctly validates Jax ID formats.
    """

    @pytest.mark.parametrize(
        "input_ids",
        [
            pd.Series(["123456", "000001", "1234567"]),
            pd.Series(["123456", "", "000001"]),
            pd.Series(["", ""], dtype="object"),
        ],
        ids=[
            "Pass with valid Jax IDs",
            "Pass with valid Jax IDs and empty string for missing value",
            "Pass with all empty Jax IDs",
        ],
    )
    def test_validate_jax_ids_should_pass(self, input_ids: pd.Series) -> None:
        """
        Tests that the function does not raise an error when given valid Jax ID formats.
        """
        # Should not raise an error
        validate_jax_ids(input_ids)

    @pytest.mark.parametrize(
        "input_ids",
        [
            pd.Series(["12345"]),
            pd.Series(["-12345"]),
            pd.Series(["123 456"]),
            pd.Series([None]),
            pd.Series([np.nan]),
            pd.Series(["\n"]),
        ],
        ids=[
            "Fail with Jax ID that is too short",
            "Fail with Jax ID that contains non-digit character",
            "Fail with Jax ID that contains space",
            "Fail with None value instead of empty string",
            "Fail with NaN value instead of empty string",
            "Fail with string containing only whitespace",
        ],
    )
    def test_validate_jax_ids_should_fail_on_invalid_formats(
        self, input_ids: pd.Series
    ) -> None:
        """
        Tests that the function raises a ValueError when given invalid Jax ID formats.
        """
        with pytest.raises(
            ValueError, match="Jax IDs must be strings that contain only digits"
        ):
            validate_jax_ids(input_ids)
