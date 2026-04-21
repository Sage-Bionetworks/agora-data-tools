import sys
from io import StringIO
from typing import Any
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
import yaml

from agoradatatools.etl import utils


class TestLoginToSynapse:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.patch_synapseclient = patch.object(
            synapseclient, "Synapse", return_value=syn
        ).start()
        self.patch_syn_login = patch.object(syn, "login", return_value=syn).start()

    def teardown_method(self):
        mock.patch.stopall()

    def test_login_with_token(self):
        utils._login_to_synapse(token="my_auth_token")
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with(authToken="my_auth_token")

    def test_login_no_token(self):
        utils._login_to_synapse(token=None)
        self.patch_synapseclient.assert_called_once()
        self.patch_syn_login.assert_called_once_with()


def test_get_config_with_invalid_file_path():
    with pytest.raises(FileNotFoundError, match="File not found. *"):
        utils._get_config(config_path="this/is/a/bad/path")


def test_get_config_invalid_config_file():
    with pytest.raises(
        ValueError, match="YAML file must be loaded as a single dictionary. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_invalid_config.yaml")


def test_get_config_with_parser_error():
    with pytest.raises(
        yaml.parser.ParserError, match="YAML file unable to be parsed. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_parsing.yaml")


def test_get_config_with_scanner_error():
    with pytest.raises(
        yaml.scanner.ScannerError, match="YAML file unable to be scanned. *"
    ):
        utils._get_config(config_path="./tests/test_assets/bad_config_scanning.yaml")


def test_get_config_with_no_config_path():
    config = utils._get_config(config_path=None)
    assert config["destination"] == "syn12177492"


def test_get_config_with_config_path():
    config = utils._get_config(config_path="./configs/agora_preprod.yaml")
    assert config["destination"] == "syn17015333"


def test_standardize_column_names():
    df = pd.DataFrame(
        {
            "a#": ["test_value"],
            "b@": ["test_value"],
            "c&": ["test_value"],
            "d*": ["test_value"],
            "e^": ["test_value"],
            "f?": ["test_value"],
            "g(": ["test_value"],
            "h)": ["test_value"],
            "i%": ["test_value"],
            "j$": ["test_value"],
            "k#": ["test_value"],
            "l!": ["test_value"],
            "m/": ["test_value"],
            "n ": ["test_value"],
            "o-": ["test_value"],
            "p.": ["test_value"],
            "AAA": ["test_value"],
        }
    )
    standard_df = utils.standardize_column_names(df=df)
    assert list(standard_df.columns) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n_",
        "o_",
        "p_",
        "aaa",
    ]


class TestStandardizeValues:
    df = pd.DataFrame(
        {
            "a": ["n/a"],
            "b": ["N/A"],
            "c": ["n/A"],
            "d": ["N/a"],
        }
    )

    def test_standardize_values_success(self):
        standard_df = utils.standardize_values(df=self.df.copy())
        for value in standard_df.iloc[0].tolist():
            assert np.isnan(value)

    def test_standardize_values_TypeError(self):
        with patch.object(pd.DataFrame, "replace") as patch_replace:
            patch_replace.side_effect = TypeError
            captured_output = StringIO()
            sys.stdout = captured_output
            standard_df = utils.standardize_values(df=self.df.copy())
            assert "Error comparing types." in captured_output.getvalue()
            assert standard_df.equals(self.df)

    def test_standardize_values_preserves_n_a_substrings(self):
        """Test that N/A substrings within other text are not accidentally replaced."""
        df_with_substrings = pd.DataFrame(
            {
                "aliases": [
                    "Snx1*D465N/APOE4/Trem2*R47H",  # Original problematic value
                    "N/A",  # Exact N/A - should be replaced
                    "n/a",  # Exact n/a - should be replaced
                    "n/A",  # Exact n/A - should be replaced
                    "N/a",  # Exact N/a - should be replaced
                    "Some text with N/A in it",  # Contains "N/A" as substring
                    "Some text with n/a in it",  # Contains "n/a" as substring
                    "Some text with n/A in it",  # Contains "n/A" as substring
                    "Some text with N/a in it",  # Contains "N/a" as substring
                    "Normal text",  # No N/A
                ]
            }
        )

        result_df = utils.standardize_values(df_with_substrings.copy())

        # Check that N/A substrings within other text are preserved
        assert result_df.loc[0, "aliases"] == "Snx1*D465N/APOE4/Trem2*R47H"

        # Check that exact N/A values are replaced with NaN
        assert pd.isna(result_df.loc[1, "aliases"])  # "N/A" should become NaN
        assert pd.isna(result_df.loc[2, "aliases"])  # "n/a" should become NaN
        assert pd.isna(result_df.loc[3, "aliases"])  # "n/A" should become NaN
        assert pd.isna(result_df.loc[4, "aliases"])  # "N/a" should become NaN

        # Check that N/A substrings within other text are preserved
        assert result_df.loc[5, "aliases"] == "Some text with N/A in it"
        assert result_df.loc[6, "aliases"] == "Some text with n/a in it"
        assert result_df.loc[7, "aliases"] == "Some text with n/A in it"
        assert result_df.loc[8, "aliases"] == "Some text with N/a in it"
        assert result_df.loc[9, "aliases"] == "Normal text"  # Should be preserved


class TestRenameColumnsDataFrame:
    df = pd.DataFrame(
        {
            "a": ["test_value"],
            "b": ["test_value"],
            "c": ["test_value"],
            "d": ["test_value"],
        }
    )
    good_column_map = {"a": "e", "b": "f", "c": "g", "d": "h"}

    def test_rename_columns_success(self):
        renamed_df = utils.rename_columns(
            data=self.df.copy(), column_map=self.good_column_map
        )
        assert list(renamed_df.columns) == list(self.good_column_map.values())

    def test_rename_columns_TypeError(self):
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_renamed_df = utils.rename_columns(data=self.df.copy(), column_map=[])
        assert "Column mapping must be a dictionary." in captured_output.getvalue()
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())

    def test_rename_columns_non_string_keys(self):
        """Test that non-string keys in column_map are handled with error message"""
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_column_map = {1: "e", "b": "f"}  # Key '1' is not a string
        bad_renamed_df = utils.rename_columns(
            data=self.df.copy(), column_map=bad_column_map
        )
        assert (
            "Column mapping must be a dictionary with string keys."
            in captured_output.getvalue()
        )
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())

    def test_rename_columns_not_none_values(self):
        """Test that None values in column_map are handled with error message"""
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_column_map = {"a": None, "b": "f"}
        bad_renamed_df = utils.rename_columns(
            data=self.df.copy(), column_map=bad_column_map
        )
        assert (
            "Column mapping must be a dictionary with string values that are not None."
            in captured_output.getvalue()
        )
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())

    def test_rename_columns_non_string_values(self):
        """Test that non-string values in column_map are handled with error message"""
        captured_output = StringIO()
        sys.stdout = captured_output
        bad_column_map = {"a": 1, "b": [], "c": None, "d": "h"}
        bad_renamed_df = utils.rename_columns(
            data=self.df.copy(), column_map=bad_column_map
        )
        assert (
            "Column mapping must be a dictionary with string values that are not None."
            in captured_output.getvalue()
        )
        assert list(bad_renamed_df.columns) == list(self.good_column_map.keys())

    def test_rename_columns_preserves_dataframe_values(self):
        """Test that DataFrame values are preserved after renaming"""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.5, 2.5, 3.5]})
        column_map = {"a": "x", "b": "y", "c": "z"}

        result = utils.rename_columns(data=df.copy(), column_map=column_map)

        # Check column names
        assert list(result.columns) == ["x", "y", "z"]
        # Check values are preserved
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame({"x": [1, 2, 3], "y": ["x", "y", "z"], "z": [1.5, 2.5, 3.5]}),
        )

    def test_rename_columns_with_empty_dataframe(self):
        """Test renaming columns in an empty DataFrame"""
        empty_df = pd.DataFrame()
        column_map = {"a": "x", "b": "y", "c": "z"}

        result = utils.rename_columns(data=empty_df, column_map=column_map)

        # Check that columns are renamed correctly
        assert list(result.columns) == []
        # Check that the DataFrame is still empty
        assert len(result) == 0
        # Verify the original DataFrame was modified in place
        assert list(empty_df.columns) == []


class TestRenameColumnsDict:
    input_dict = {"a": "value1", "b": "value2", "c": "value3"}
    column_map = {"a": "x", "b": "y", "c": "z"}

    def test_rename_columns_with_dict_input(self):
        """Test renaming columns in a dictionary input"""
        result = utils.rename_columns(
            data=self.input_dict.copy(), column_map=self.column_map
        )
        assert result == {"x": "value1", "y": "value2", "z": "value3"}

    def test_rename_columns_with_partial_mapping(self):
        """Test renaming only some columns, leaving others unchanged"""
        partial_column_map = {"a": "x", "c": "z"}
        result = utils.rename_columns(
            data=self.input_dict.copy(), column_map=partial_column_map
        )
        assert result == {"x": "value1", "b": "value2", "z": "value3"}

    def test_rename_columns_with_nonexistent_keys(self):
        """Test renaming when column_map contains keys that don't exist in the data"""
        nonexistent_column_map = {
            "a": "x",
            "d": "z",
            "e": "w",
        }  # 'c' and 'd' don't exist
        result = utils.rename_columns(
            data=self.input_dict.copy(), column_map=nonexistent_column_map
        )
        assert result == {"x": "value1", "b": "value2", "c": "value3"}

    def test_rename_columns_with_empty_dict(self):
        """Test renaming with an empty column mapping"""
        empty_column_map = {}
        result = utils.rename_columns(
            data=self.input_dict.copy(), column_map=empty_column_map
        )
        # Should return unchanged data
        assert result == {"a": "value1", "b": "value2", "c": "value3"}

    def test_rename_columns_preserves_dict_values(self):
        """Test that dictionary values are preserved after renaming"""
        input_dict = {"a": [1, 2, 3], "b": {"nested": "value"}, "c": None, "d": 42}
        column_map = {"a": "x", "b": "y", "c": "z", "d": "w"}

        result = utils.rename_columns(data=input_dict.copy(), column_map=column_map)

        expected = {"x": [1, 2, 3], "y": {"nested": "value"}, "z": None, "w": 42}
        assert result == expected

    def test_rename_columns_with_complex_nested_structures(self):
        """Test renaming with complex nested data structures"""
        input_dict = {
            "a": {"nested": {"deep": "value"}},
            "b": [{"inner": "data"}, {"inner": "more_data"}],
            "c": "simple_string",
        }
        column_map = {"a": "x", "b": "y", "c": "z"}

        result = utils.rename_columns(data=input_dict.copy(), column_map=column_map)

        expected = {
            "x": {"nested": {"deep": "value"}},
            "y": [{"inner": "data"}, {"inner": "more_data"}],
            "z": "simple_string",
        }
        assert result == expected


class TestRenameColumnsList:
    input_list = [
        {"a": "value1", "b": "value2", "c": "value3"},
        {"a": "value4", "b": "value5", "c": "value6"},
        {"a": "value7", "b": "value8", "c": "value9"},
    ]
    column_map = {"a": "x", "b": "y", "c": "z"}

    def test_rename_columns_with_list_of_dicts_input(self):
        """Test renaming columns in a list of dictionaries input"""
        result = utils.rename_columns(
            data=self.input_list.copy(), column_map=self.column_map
        )

        expected = [
            {"x": "value1", "y": "value2", "z": "value3"},
            {"x": "value4", "y": "value5", "z": "value6"},
            {"x": "value7", "y": "value8", "z": "value9"},
        ]
        assert result == expected
        # Verify the original list was modified in place
        assert self.input_list == expected

    def test_rename_columns_with_empty_list(self):
        """Test renaming with an empty list of dictionaries"""
        input_list = []
        result = utils.rename_columns(
            data=input_list.copy(), column_map=self.column_map
        )
        # Should return unchanged empty list
        assert result == []

    def test_rename_columns_with_mixed_list_content(self):
        """Test renaming with a list containing dictionaries with different keys"""
        input_list = [
            {"a": "value1", "b": "value2"},
            {"a": "value3", "c": "value4"},  # Missing 'b', has 'c'
            {"b": "value5", "d": "value6"},  # Missing 'a', has 'd'
        ]
        column_map = {"a": "x", "b": "y"}

        result = utils.rename_columns(data=input_list.copy(), column_map=column_map)

        expected = [
            {"x": "value1", "y": "value2"},
            {"x": "value3", "c": "value4"},  # Only 'a' was renamed
            {"y": "value5", "d": "value6"},  # Only 'b' was renamed
        ]
        assert result == expected

    def test_rename_columns_with_non_dict_items_in_list(self):
        """Test that TypeError is raised when list contains non-dictionary items"""
        input_list = [
            {"a": "value1", "b": "value2"},
            "not_a_dict",  # This should cause the error
            {"a": "value3", "b": "value4"},
        ]
        column_map = {"a": "x", "b": "y"}

        with pytest.raises(TypeError, match="List must contain dictionaries."):
            utils.rename_columns(data=input_list, column_map=column_map)

    def test_rename_columns_with_invalid_data_type(self):
        """Test that TypeError is raised when data is not a DataFrame, list, or dict"""
        column_map = {"a": "x", "b": "y"}

        # Test with string data
        with pytest.raises(
            TypeError,
            match="Data must be a pandas DataFrame, list of dictionaries, or dictionary.",
        ):
            utils.rename_columns(data="invalid_string", column_map=column_map)

        # Test with integer data
        with pytest.raises(
            TypeError,
            match="Data must be a pandas DataFrame, list of dictionaries, or dictionary.",
        ):
            utils.rename_columns(data=123, column_map=column_map)

        # Test with None data
        with pytest.raises(
            TypeError,
            match="Data must be a pandas DataFrame, list of dictionaries, or dictionary.",
        ):
            utils.rename_columns(data=None, column_map=column_map)


class TestNestFields:
    """Tests the nest_fields function using a dataframe that has multiple rows per group and
    one that only has one row per group.
    """

    df_multirow = pd.DataFrame(
        {
            "a": ["group_1", "group_1", "group_2", "group_2", "group_3", "group_3"],
            "b": ["1", "1", "1", "1", "1", "1"],
            "c": ["1", "1", "1", "1", "1", "1"],
            "d": ["1", "1", "1", "1", "1", "1"],
        }
    )
    df_singlerow = pd.DataFrame(
        {
            "a": ["group_1", "group_2", "group_3"],
            "b": ["1", "1", "1"],
            "c": ["1", "1", "1"],
            "d": ["1", "1", "1"],
        }
    )

    def test_nest_fields_with_dropped_column(self):
        expected_column_e = [
            [
                {"a": "group_1", "b": "1", "c": "1"},
                {"a": "group_1", "b": "1", "c": "1"},
            ],
            [
                {"a": "group_2", "b": "1", "c": "1"},
                {"a": "group_2", "b": "1", "c": "1"},
            ],
            [
                {"a": "group_3", "b": "1", "c": "1"},
                {"a": "group_3", "b": "1", "c": "1"},
            ],
        ]

        nested_df = utils.nest_fields(
            df=self.df_multirow, grouping="a", new_column="e", drop_columns=["d"]
        )
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_with_dropped_column_list(self):
        expected_column_e = [
            [
                {"a": "group_1", "c": "1"},
                {"a": "group_1", "c": "1"},
            ],
            [
                {"a": "group_2", "c": "1"},
                {"a": "group_2", "c": "1"},
            ],
            [
                {"a": "group_3", "c": "1"},
                {"a": "group_3", "c": "1"},
            ],
        ]

        nested_df = utils.nest_fields(
            df=self.df_multirow, grouping="a", new_column="e", drop_columns=["b", "d"]
        )
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_no_drop_column(self):
        expected_column_e = [
            [
                {"a": "group_1", "b": "1", "c": "1", "d": "1"},
                {"a": "group_1", "b": "1", "c": "1", "d": "1"},
            ],
            [
                {"a": "group_2", "b": "1", "c": "1", "d": "1"},
                {"a": "group_2", "b": "1", "c": "1", "d": "1"},
            ],
            [
                {"a": "group_3", "b": "1", "c": "1", "d": "1"},
                {"a": "group_3", "b": "1", "c": "1", "d": "1"},
            ],
        ]

        nested_df = utils.nest_fields(df=self.df_multirow, grouping="a", new_column="e")
        assert list(nested_df["e"]) == expected_column_e

    def test_nest_fields_multirow_ValueError(self):
        with pytest.raises(ValueError, match="nested_field_is_list *"):
            utils.nest_fields(
                df=self.df_multirow,
                grouping="a",
                new_column="e",
                drop_columns=["d"],
                nested_field_is_list=False,
            )

    def test_nest_fields_singlerow_nested_list_false(self):
        expected_column_e = [
            {"a": "group_1", "b": "1", "c": "1"},
            {"a": "group_2", "b": "1", "c": "1"},
            {"a": "group_3", "b": "1", "c": "1"},
        ]

        nested_df = utils.nest_fields(
            df=self.df_singlerow,
            grouping="a",
            new_column="e",
            drop_columns=["d"],
            nested_field_is_list=False,
        )
        assert list(nested_df["e"]) == expected_column_e


class TestCalculateDistribution:
    # NOTE: pd.describe() calls np.quantile() with interpolation when quantiles fall between values.
    # We calculate the expected quartile values on this data by calling np.quantile() on manually-
    # broken out groups. Then the min/max values are calculated as <quartile> +/- 1.5*IQR.
    df = pd.DataFrame(
        {
            "col_1": [
                "a",
                "a",
                "a",
                "a",
                "a",
                "b",
                "c",
                "c",
                "c",
                "c",
                "c",
                "c",
            ],  # 3 main groups
            "col_2": [
                "x",
                "x",
                "y",
                "y",
                "y",
                "x",
                "x",
                "x",
                "x",
                "y",
                "y",
                "y",
            ],  # 2 subgroups
            "col_3": [1, 5, 10, 12, 14, 2, 6, 7, 9, 16, 17, 19],  # Values of interest
            "col_4": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
            ],  # Ignored column of values
            "col_5": [
                "m",
                "m",
                "n",
                "n",
                "o",
                "o",
                "o",
                "p",
                "p",
                "p",
                "q",
                "q",
            ],  # an ignored column of strings
        }
    )

    # Stats on "col_3", grouped by "col_1" only
    def test_calculate_distribution_one_group(self):
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "b", "c"],
                "min": [-5.5, 2.0, -6.375],
                "max": [22.5, 2.0, 30.625],
                "first_quartile": [5.0, 2.0, 7.5],
                "median": [10.0, 2.0, 12.5],
                "third_quartile": [12.0, 2.0, 16.75],
            }
        )
        output_df = utils.calculate_distribution(
            df=self.df, grouping="col_1", distribution_column="col_3"
        )
        assert output_df.equals(expected_df)

    # Stats on "col_3", grouped by "col_1" and "col_2"
    def test_calculate_distribution_two_groups(self):
        expected_df = pd.DataFrame(
            {
                "col_1": ["a", "a", "b", "c", "c"],
                "col_2": ["x", "y", "x", "x", "y"],
                "min": [-1.0, 8.0, 2.0, 4.25, 14.25],
                "max": [7.0, 16.0, 2.0, 10.25, 20.25],
                "first_quartile": [2.0, 11.0, 2.0, 6.5, 16.5],
                "median": [3.0, 12.0, 2.0, 7.0, 17.0],
                "third_quartile": [4.0, 13.0, 2.0, 8.0, 18.0],
            }
        )
        output_df = utils.calculate_distribution(
            df=self.df, grouping=["col_1", "col_2"], distribution_column="col_3"
        )
        assert output_df.equals(expected_df)


class TestCheckRequiredDatasetsAndColumns:
    required_input = {
        "foo": ["a", "b"],
        "bar": ["x", "y"],
    }

    def test_check_required_datasets_and_columns_all_present(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1], "b": [2]}),
            "bar": pd.DataFrame({"x": [3], "y": [4]}),
        }
        # Should not raise
        utils.check_required_datasets_and_columns(datasets, self.required_input)

    def test_check_required_datasets_and_columns_missing_dataset(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1], "b": [2]}),
        }
        with pytest.raises(ValueError, match="Missing required datasets: bar"):
            utils.check_required_datasets_and_columns(datasets, self.required_input)

    def test_check_required_datasets_and_columns_missing_column(self):
        datasets = {
            "foo": pd.DataFrame({"a": [1]}),
            "bar": pd.DataFrame({"x": [3], "y": [4]}),
        }
        with pytest.raises(
            ValueError, match="Missing required columns in foo dataset: b"
        ):
            utils.check_required_datasets_and_columns(datasets, self.required_input)


class TestFlattenList:
    def test_flatten_list_empty(self):
        assert utils.flatten_list([]) == []

    def test_flatten_list_no_nesting(self):
        input_list = [1, 2, 3, 4, 5]
        assert utils.flatten_list(input_list) == [1, 2, 3, 4, 5]

    def test_flatten_list_single_level_nesting(self):
        input_list = [1, [2, 3], 4, [5, 6]]
        assert utils.flatten_list(input_list) == [1, 2, 3, 4, 5, 6]

    def test_flatten_list_multiple_level_nesting(self):
        input_list = [1, [2, [3, 4]], [5, [6, [7, 8]]]]
        assert utils.flatten_list(input_list) == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_flatten_list_mixed_types(self):
        # Note that an empty list is not kept as an output element
        input_list = [1, ["a", [2.5, True]], [None, ["x", []]]]
        assert utils.flatten_list(input_list) == [1, "a", 2.5, True, None, "x"]


class TestRemoveDuplicatesKeepOrder:
    def test_remove_duplicates_empty(self):
        assert utils.remove_duplicates_keep_order([]) == []

    def test_remove_duplicates_no_duplicates(self):
        input_list = [1, 2, 3, 4, 5]
        assert utils.remove_duplicates_keep_order(input_list) == [1, 2, 3, 4, 5]

    def test_remove_duplicates_with_duplicates(self):
        input_list = [1, 2, 2, 3, 4, 4, 4, 5]
        assert utils.remove_duplicates_keep_order(input_list) == [1, 2, 3, 4, 5]

    def test_remove_duplicates_mixed_types(self):
        input_list = [2, "a", 2.5, True, "a", 2, None, True]
        assert utils.remove_duplicates_keep_order(input_list) == [
            2,
            "a",
            2.5,
            True,
            None,
        ]

    def test_remove_duplicates_mixed_types_true_1(self):
        # Note that True and 1 are considered equal for hashing purposes
        # Explicitly testing this so we keep track of this behavior
        input_list = [1, "a", 2.5, True, "a", 1, None, True]
        assert utils.remove_duplicates_keep_order(input_list) == [1, "a", 2.5, None]

    def test_remove_duplicates_preserves_order(self):
        input_list = ["a", "b", "a", "c", "b", "d"]
        assert utils.remove_duplicates_keep_order(input_list) == ["a", "b", "c", "d"]


class TestInputValidationModelInfo:
    """
    Test class for validating the input_validation_model_info function.
    This function validates that model information is consistent across
    multiple rows for the same model name.
    """

    def test_valid_model_info(self) -> None:
        """
        Test that valid model info with consistent data passes validation.
        Different models can have different values, but the same model
        should have consistent values across rows.
        """
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD2",  # Different model, different values are OK
                    "matched_controls": "C57BL6J",
                    "model_type": "Early Onset AD",
                },
            ]
        )
        # Should not raise any exception
        utils.input_validation_model_info(df)

    def test_inconsistent_matched_controls(self) -> None:
        """
        Test that inconsistent matched_controls values for the same model
        raise a ValueError with appropriate error message.
        """
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD1",  # Same model name but different matched_controls
                    "matched_controls": "CTRL2",
                    "model_type": "Late Onset AD",
                },
            ]
        )
        with pytest.raises(
            ValueError, match="Model LOAD1 has inconsistent matched_controls values:"
        ):
            utils.input_validation_model_info(df)

    def test_inconsistent_model_type(self) -> None:
        """
        Test that inconsistent model_type values for the same model
        raise a ValueError with appropriate error message.
        """
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD1",  # Same model name but different model_type
                    "matched_controls": "C57BL6J",
                    "model_type": "Early Onset AD",
                },
            ]
        )
        with pytest.raises(
            ValueError, match="Model LOAD1 has inconsistent model_type values:"
        ):
            utils.input_validation_model_info(df)

    def test_empty_dataframe(self) -> None:
        """
        Test that an empty dataframe passes validation without errors.
        """
        df = pd.DataFrame(columns=["name", "matched_controls", "model_type"])
        # Should not raise any exception
        utils.input_validation_model_info(df)

    def test_single_row(self) -> None:
        """
        Test that a dataframe with a single row passes validation.
        Single rows cannot have inconsistencies by definition.
        """
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                }
            ]
        )
        # Should not raise any exception
        utils.input_validation_model_info(df)


class TestNormalizeZero:
    """Test class for the normalize_zero function."""

    def test_negative_zero_becomes_positive_zero(self) -> None:
        """Test that -0.0 is converted to 0.0."""
        import math

        result = utils.normalize_zero(-0.0)
        # Verify result is non-negative (positive zero, not negative zero)
        assert result >= 0
        # Verify it's positive zero using copysign
        assert math.copysign(1.0, result) > 0

    def test_positive_zero_stays_positive_zero(self) -> None:
        """Test that 0.0 remains 0.0."""
        import math

        result = utils.normalize_zero(0.0)
        # Verify result is non-negative (positive zero, not negative zero)
        assert result >= 0
        assert math.copysign(1.0, result) > 0

    def test_positive_values_preserved(self) -> None:
        """Test that positive values are preserved."""
        assert utils.normalize_zero(1.0) == pytest.approx(1.0)
        assert utils.normalize_zero(42.5) == pytest.approx(42.5)
        assert utils.normalize_zero(0.001) == pytest.approx(0.001)
        assert utils.normalize_zero(1e10) == pytest.approx(1e10)

    def test_negative_values_preserved(self) -> None:
        """Test that negative values (other than -0.0) are preserved."""
        assert utils.normalize_zero(-1.0) == pytest.approx(-1.0)
        assert utils.normalize_zero(-42.5) == pytest.approx(-42.5)
        assert utils.normalize_zero(-0.001) == pytest.approx(-0.001)
        assert utils.normalize_zero(-1e10) == pytest.approx(-1e10)

    def test_very_small_positive_value_preserved(self) -> None:
        """Test that very small positive values are preserved."""
        small_value = 1e-15
        assert utils.normalize_zero(small_value) == pytest.approx(small_value)

    def test_very_small_negative_value_preserved(self) -> None:
        """Test that very small negative values are preserved."""
        small_value = -1e-15
        assert utils.normalize_zero(small_value) == pytest.approx(small_value)

    def test_negative_zero_via_copysign(self) -> None:
        """Test that negative zero created via copysign is normalized."""
        import math

        negative_zero = math.copysign(0.0, -1.0)
        result = utils.normalize_zero(negative_zero)
        # Verify result is non-negative (positive zero, not negative zero)
        assert result >= 0
        assert math.copysign(1.0, result) > 0


class TestExtractAgeNumeric:
    """
    Test class for validating the extract_age_numeric utility function.
    This function extracts the numeric value from an age string.
    """

    @pytest.mark.parametrize(
        "input_age,expected",
        [
            ("4 months", 4),  # Age with months
            ("8 months", 8),  # Age with months
            ("12 months", 12),  # Age with months
            ("6 weeks", 6),  # Age with weeks
            ("100 days", 100),  # Age with days
            ("", None),  # Empty string
            ("no number here", None),  # String without number
            ("number 10", 10),  # String with number
            ("10 11 12", 10),  # String with multiple numbers
            (None, None),  # None input
        ],
    )
    def test_extract_age_numeric(self, input_age, expected):
        """
        Test that extract_age_numeric correctly extracts the numeric value
        from age strings with various formats.

        Args:
            input_age: Input string that may contain age and unit
            expected: Expected numeric age value or None if no number found
        """
        assert utils.extract_age_numeric(input_age) == expected


class TestNormalizeNullValues:
    """
    Test class for validating the normalize_null_values utility function.
    """

    @pytest.fixture
    def test_data_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "bool1": [True, False, None, np.nan],
                "bool2": [np.nan] * 4,
                "string1": ["abc", None, "", np.nan],
                "string2": [np.nan] * 4,
                "numeric1": [123, np.nan, np.nan, np.nan],
                "numeric2": [np.nan] * 4,
                "extra1": ["abc", "def", None, np.nan],
                "extra2": [np.nan] * 4,
            }
        )

    @pytest.fixture
    def basic_expected_output(self) -> pd.DataFrame:
        # The .replace at the end is necessary for numeric1, which still instantiates with np.nan values despite being
        # defined with Nones
        return pd.DataFrame(
            {
                "bool1": [True, False, None, None],
                "bool2": [None] * 4,
                "string1": ["abc", None, "", None],
                "string2": [None] * 4,
                "numeric1": [123, None, None, None],
                "numeric2": [None] * 4,
                "extra1": ["abc", "def", None, None],
                "extra2": [None] * 4,
            }
        ).replace({np.nan: None})

    def test_normalize_null_values_with_empty_column_lists(
        self, test_data_frame: pd.DataFrame, basic_expected_output: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values correctly normalizes null values when all 3 *_column arguments have default
        [] values.
        """
        output = utils.normalize_null_values(test_data_frame)
        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_normalize_null_values_with_only_boolean_columns(
        self, test_data_frame: pd.DataFrame, basic_expected_output: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values correctly normalizes null values when only boolean_columns is defined.
        """
        output = utils.normalize_null_values(
            test_data_frame,
            boolean_columns=["bool1", "bool2"],
        )

        basic_expected_output[["bool1", "bool2"]] = basic_expected_output[
            ["bool1", "bool2"]
        ].fillna(False)

        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_normalize_null_values_with_only_string_columns(
        self, test_data_frame: pd.DataFrame, basic_expected_output: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values correctly normalizes null values when only string_columns is defined.
        """
        output = utils.normalize_null_values(
            test_data_frame,
            empty_string_columns=["string1", "string2"],
        )

        basic_expected_output[["string1", "string2"]] = basic_expected_output[
            ["string1", "string2"]
        ].fillna("")

        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_normalize_null_values_with_all_column_types_defined(
        self, test_data_frame: pd.DataFrame, basic_expected_output: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values correctly normalizes null values when all 3 *_columns arguments are defined.
        """
        output = utils.normalize_null_values(
            test_data_frame,
            boolean_columns=["bool1", "bool2"],
            empty_string_columns=["string1", "string2"],
        )

        basic_expected_output[["bool1", "bool2"]] = basic_expected_output[
            ["bool1", "bool2"]
        ].fillna(False)
        basic_expected_output[["string1", "string2"]] = basic_expected_output[
            ["string1", "string2"]
        ].fillna("")

        pd.testing.assert_frame_equal(output, basic_expected_output)

    def test_normalize_null_values_with_empty_data_frame(self) -> None:
        """
        Test that normalize_null_values correctly handles an empty data frame without errors.
        """
        empty_df = pd.DataFrame(columns=["bool1", "string1", "numeric1"])
        empty_df["bool1"] = empty_df["bool1"].astype(bool)

        output = utils.normalize_null_values(
            empty_df.copy(),
            boolean_columns=["bool1"],
            empty_string_columns=["string1"],
        )
        # Should return an empty data frame with the same columns
        pd.testing.assert_frame_equal(output, empty_df)

    def test_normalize_null_values_fails_with_nonexistent_columns(
        self, test_data_frame: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values raises a ValueError when columns don't exist in the data frame.
        """
        with pytest.raises(
            ValueError,
            match="Columns \\['bool_x', 'string_x'\\] do not exist in the DataFrame",
        ):
            utils.normalize_null_values(
                test_data_frame,
                boolean_columns=["bool1", "bool_x"],
                empty_string_columns=["string1", "string_x"],
            )

    def test_normalize_null_values_fails_with_overlapping_columns(
        self, test_data_frame: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values raises a ValueError when columns are included in more than one column type list.
        """
        with pytest.raises(
            ValueError, match="Columns \\['bool1', 'string2'\\] appear in both"
        ):
            utils.normalize_null_values(
                test_data_frame,
                boolean_columns=["bool1", "bool2", "string2"],
                empty_string_columns=["string1", "string2", "bool1"],
            )

    @pytest.mark.parametrize(
        "df_value",
        [
            "not_a_dataframe",  # String
            123,  # Integer
            None,  # None
            ("df1", "df2"),  # Tuple
            [1, 2, 3],  # List
            {"col": [1, 2]},  # Dict
        ],
    )
    def test_normalize_null_values_fails_with_non_dataframe_argument(
        self, df_value: Any
    ) -> None:
        """
        Test that normalize_null_values raises a TypeError when the df argument is not a DataFrame.
        """
        with pytest.raises(TypeError, match="Input must be a pandas DataFrame"):
            utils.normalize_null_values(df_value)

    @pytest.mark.parametrize(
        "column_value",
        [
            "bool1",  # Single string
            123,  # Integer
            False,  # Boolean
            ("bool1", "bool2"),  # Tuple
            {"bool1": "bool2"},  # Dict
            pd.DataFrame(),  # DataFrame
        ],
    )
    def test_normalize_null_values_fails_with_non_list_arguments(
        self, column_value: Any, test_data_frame: pd.DataFrame
    ) -> None:
        """
        Test that normalize_null_values raises a TypeError when *_columns arguments are not lists.
        """
        with pytest.raises(TypeError, match="boolean_columns must be a list"):
            utils.normalize_null_values(
                test_data_frame,
                boolean_columns=column_value,
            )

        with pytest.raises(TypeError, match="empty_string_columns must be a list"):
            utils.normalize_null_values(
                test_data_frame,
                empty_string_columns=column_value,
            )
class TestDelimStringToList:
    """
    Test class for validating the delim_string_to_list utility function.
    """

    @pytest.mark.parametrize(
        "input_string,delimiter,expected",
        [
            ("a,b,c", ",", ["a", "b", "c"]),  # String with default delimiter (,)
            ("a|b|c", "|", ["a", "b", "c"]),  # Non-default delimiter (|)
            ("a;;b;;c", ";;", ["a", "b", "c"]),  # Multi-character delimiter
            ("aa,,bbb,,c", ",,", ["aa", "bbb", "c"]),  # Multi-character items
            ("ab;c", ",", ["ab;c"]),  # Delimiter mismatch should not split the string
            # Empty elements at start, end, and middle should be removed
            (",a,b,,c,", ",", ["a", "b", "c"]),
            (",,", ",", []),  # String with only delimiters should return empty list
            (None, ",", []),  # None input should return empty list
            ("", ",", []),  # Empty string should return empty list
            # Extra whitespace should be stripped
            ("   a   ,b,   c", ",", ["a", "b", "c"]),
            # Splitting on whitespace should still strip extra whitespace
            ("a   b c  \t", " ", ["a", "b", "c"]),
            # Extra whitespace in delimiter is respected
            ("a  b c   ", "  ", ["a", "b c"]),
        ],
    )
    def test_delim_string_to_list(
        self, input_string: str, delimiter: str, expected: list[str]
    ) -> None:
        """
        Test that delim_string_to_list correctly splits the input string into a list.
        """
        assert utils.delim_string_to_list(input_string, delimiter) == expected

    def test_delim_string_to_list_uses_default_delimiter(self) -> None:
        """
        Test that delim_string_to_list uses the default delimiter (comma) when none is provided.
        """
        assert utils.delim_string_to_list("a,b,c") == ["a", "b", "c"]

        # Should not split if it's not comma-separated
        assert utils.delim_string_to_list("a|b|c") == ["a|b|c"]

    @pytest.mark.parametrize(
        "input_value,delimiter",
        [
            (123, ","),  # Non-string input should raise error
            (True, ","),  # Boolean input should raise error
            (["a", "b", "c"], ","),  # List input should raise error
            ({"key": "value"}, ","),  # Dict input should raise error
            (("a", "b"), ","),  # Tuple input should raise error
        ],
    )
    def test_delim_string_to_list_fails_on_non_string_input(
        self, input_value: Any, delimiter: str
    ) -> None:
        """
        Test that delim_string_to_list raises a TypeError when input is not a string.
        """
        with pytest.raises(TypeError, match="Input must be a string"):
            utils.delim_string_to_list(input_value, delimiter)

    @pytest.mark.parametrize(
        "input_string,delimiter",
        [
            ("a,b,c", 123),  # Non-string delimiter should raise error
            ("a,b,c", True),  # Boolean delimiter should raise error
            ("a,b,c", ["|"]),  # List delimiter should raise error
            ("a,b,c", {"key": "value"}),  # Dict delimiter should raise error
            ("a,b,c", ("|",)),  # Tuple delimiter should raise error
            ("a,b,c", np.nan),  # NaN delimiter should raise error
            ("a,b,c", None),  # None delimiter should raise error
        ],
    )
    def test_delim_string_to_list_fails_on_non_string_delimiter(
        self, input_string: str, delimiter: Any
    ) -> None:
        """
        Test that delim_string_to_list raises a TypeError when delimiter is not a string.
        """
        with pytest.raises(TypeError, match="Delimiter must be a string"):
            utils.delim_string_to_list(input_string, delimiter)
