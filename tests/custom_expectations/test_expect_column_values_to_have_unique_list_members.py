from agoradatatools.great_expectations.gx.plugins.expectations.expect_column_values_to_have_unique_list_members import (
    ColumnValuesListMembersUnique,
)


class TestColumnValuesListMembersUnique:
    def test_unique_list_members_returns_true(self):
        assert ColumnValuesListMembersUnique._check_unique_list_members(["a", "b", "c"])

    def test_duplicate_list_members_returns_false(self):
        assert not ColumnValuesListMembersUnique._check_unique_list_members(["a", "b", "a"])

    def test_empty_list_returns_true(self):
        assert ColumnValuesListMembersUnique._check_unique_list_members([])

    def test_non_list_input_returns_false(self):
        assert not ColumnValuesListMembersUnique._check_unique_list_members("not a list")
        assert not ColumnValuesListMembersUnique._check_unique_list_members(None)
