from unittest.mock import patch
import pytest
import pandas as pd

from preprocessing import preprocessing_utils
from preprocessing.preprocessing_utils import (
    DEFAULT_STAGING_PATH,
    get_nested_dict_item,
    get_staging_path,
)


class TestGetStagingPath:
    """
    Tests for the get_staging_path utility function. Requires patching several functions that would otherwise read/write
    data on disk.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.patch_get_config = patch.object(
            preprocessing_utils.utils, "_get_config"
        ).start()
        self.patch_create_temp_location = patch.object(
            preprocessing_utils.load, "create_temp_location"
        ).start()
        yield
        patch.stopall()

    @pytest.mark.parametrize("config_path", [None, ""])
    def test_missing_config_path_raises(self, config_path):
        """
        A None or empty string config_path argument should raise a ValueError.
        """
        with pytest.raises(ValueError, match="config_path must be provided"):
            get_staging_path(config_path)
        self.patch_get_config.assert_not_called()
        self.patch_create_temp_location.assert_not_called()

    def test_staging_path_comes_from_valid_config(self):
        """
        If the config has a non-missing "staging_path" value in it, that value should be returned by
        `get_staging_path`.
        """
        self.patch_get_config.return_value = {"staging_path": "custom_staging"}

        result = get_staging_path("some/config.yaml")

        assert result == "custom_staging"
        self.patch_get_config.assert_called_once_with(config_path="some/config.yaml")
        self.patch_create_temp_location.assert_called_once_with(
            staging_path="custom_staging"
        )

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"staging_path": None},
            {"staging_path": ""},
            {"other_key": "value"},
        ],
        ids=["empty_config", "none_value", "empty_string", "key_missing"],
    )
    def test_staging_path_uses_default_when_not_in_config(self, config):
        """
        Tests that the default staging path is used when the config has None or an empty string in the "staging_path"
        field, or doesn't have a "staging_path" field at all.
        """
        self.patch_get_config.return_value = config

        result = get_staging_path("some/config.yaml")

        assert result == DEFAULT_STAGING_PATH
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=DEFAULT_STAGING_PATH
        )


class TestGetNestedDictItem:
    """
    Tests for the get_nested_dict_item utility function.
    """

    def _create_nested_dict(self):
        """
        Creates a basic nested dict with multiple levels and a list of dicts inside. There are no duplicate keys at any
        level, and no items that are not a dict or a list of dicts.
        """
        return {
            "a": {
                "b": {
                    "c": 10,
                },
                "d": "val1",
            },
            "e": [{"f": 5}, {"g": "val2"}],
        }

    @pytest.mark.parametrize(
        "key, expected",
        [("b", {"c": 10}), ("c", 10), ("d", "val1"), ("f", 5), ("g", "val2")],
    )
    def test_get_nested_dict_item_passes(self, key, expected):
        """
        Tests that get_nested_dict_item correctly retrieves a nested value from a basic nested dictionary structure.
        """
        data = self._create_nested_dict()
        result = get_nested_dict_item(data, key)
        assert result == expected

    def test_returns_default_none_when_key_not_found(self):
        """
        Tests that get_nested_dict_item will return None when the query key does not exist in the input dictionary.
        """
        data = self._create_nested_dict()
        result = get_nested_dict_item(data, key="fake_key")

        assert result is None

    def test_returns_custom_default_when_key_not_found(self):
        """
        Tests that the value in the `default` argument is returned (instead of None) if the query key does not exist.
        """
        data = self._create_nested_dict()
        result = get_nested_dict_item(data, key="fake_key", default="default")

        assert result == "default"

    def test_returns_default_on_empty_dict(self):
        """
        get_nested_dict item should return the default value if the input is an empty dict with no keys.
        """
        result = get_nested_dict_item({}, key="a", default="default")
        assert result == "default"

    @pytest.mark.parametrize("value", [None, 0, "", False, [], {}])
    def test_none_like_value_is_returned_not_default(self, value):
        """
        Tests that real None/False/empty dictionary values, which would normally fail an `if value:` check, are found
        and returned instead of the default.
        """
        data = {"outer": {"key": value}}
        result = get_nested_dict_item(data, "key", default="default")
        assert result == value

    def test_nested_value_equal_to_default_is_returned(self):
        """
        Tests that the first occurrence of the key is returned, even if the value is equal to the default value. In this
        test, the search should find "key" inside "a" and stop the search, rather than moving on to "b".
        """
        data = {"a": {"key": None}, "b": {"key": "second"}}
        result = get_nested_dict_item(data, "key", default=None)
        assert result is None

    def test_parent_level_key_takes_precedence_over_nested(self):
        """
        Tests that a key match in the parent level's keys is returned, instead of any matches deeper in the nest. In
        this test, the parent-level keys are "a" and "key", so despite "a" being first in the dict, the top-level match
        to "key" should be returned instead of a[key].
        """
        data = {"a": {"key": "nested"}, "key": "top"}
        result = get_nested_dict_item(data, "key")
        assert result == "top"

    def test_children_searched_depth_first_in_key_order(self):
        """
        Tests that each child item is completely searched before moving on to the next child item. "key" appears twice
        in the nest, but "first" should be searched completely before "second" is searched. The value from the "first"
        dict should be returned even though it nested deeper than the value in "second".
        """
        data = {
            "first": {"deeper": {"key": "first_deep"}},
            "second": {"key": "second_shallow"},
        }
        result = get_nested_dict_item(data, "key")
        assert result == "first_deep"

    def test_searches_tuple_of_dicts(self):
        """
        Tests that key matches are returned when nested inside a tuple of dicts.
        """
        data = {"items": ({"a": 1}, {"key": "in_tuple"})}
        result = get_nested_dict_item(data, "key")
        assert result == "in_tuple"

    def test_list_items_searched_in_order(self):
        """
        Tests that list items are searched completely, in list order, and the first match is returned.
        """
        data = {"items": [{"other": {"key": "first"}}, {"key": "second"}]}
        result = get_nested_dict_item(data, "key")
        assert result == "first"

    def test_non_dict_list_items_ignored(self):
        """
        Tests that non-dict items in a list are ignored when searching for the key.
        """
        data = {"items": [1, "key", None, ["key"], {"key": "found"}]}
        result = get_nested_dict_item(data, "key")
        assert result == "found"

    def test_lists_of_lists_not_searched(self):
        """
        Only dicts directly inside a list/tuple are searched, not lists of lists
        """
        data = {"items": [[{"key": "too_deep"}]]}
        result = get_nested_dict_item(data, "key", default="default")
        assert result == "default"

    def test_key_matching_a_value_is_not_returned(self):
        """
        Search should return default value/None when the key matches a value rather than a dict key.
        """
        data = {"a": "key", "b": ["key"]}
        result = get_nested_dict_item(data, "key")
        assert result is None

    @pytest.mark.parametrize(
        "complex_type",
        [
            {"a": 1},
            ["a", "b", "c"],
            [{"x": 1, "y": 2}],
            pd.DataFrame({"x": [1, 2], "y": [3, 4]}),
        ],
        ids=["dict_value", "list_value", "list_of_dicts_value", "dataframe_value"],
    )
    def test_complex_value_returned_when_key_matches(self, complex_type):
        """
        Tests that the value associated with the key is returned as-is, even if it is a complex structure like a list, a
        dict, or a pandas DataFrame. This test doesn't test all possible objects, just the most common ones that might
        exist in ADT pipelines.
        """
        data = {"outer": {"key": complex_type}}
        result = get_nested_dict_item(data, "key")

        if isinstance(complex_type, pd.DataFrame):
            pd.testing.assert_frame_equal(result, complex_type)
        else:
            assert result == complex_type

    @pytest.mark.parametrize(
        "key, expected",
        [
            (1, "int_key"),
            ((1, 2), "tuple_key"),
            (None, "none_key"),
        ],
    )
    def test_non_string_hashable_keys_recognized(self, key, expected):
        """
        Tests that non-string hashable keys (like integers, tuples, and None) are correctly recognized and returned.
        """
        data = {"outer": {1: "int_key", (1, 2): "tuple_key", None: "none_key"}}
        assert get_nested_dict_item(data, key) == expected

    @pytest.mark.parametrize(
        "bad_input, type_name",
        [
            ([{"a": 1}], "list"),
            (("a", 1), "tuple"),
            ("a", "str"),
            (None, "NoneType"),
            (42, "int"),
        ],
    )
    def test_non_dict_input_raises_type_error(self, bad_input, type_name):
        """
        Tests that providing a non-dict input raises a TypeError with the appropriate message.
        """
        with pytest.raises(
            TypeError,
            match=f"The 'dictionary' argument must be of type dict, got {type_name}",
        ):
            get_nested_dict_item(bad_input, "a")
