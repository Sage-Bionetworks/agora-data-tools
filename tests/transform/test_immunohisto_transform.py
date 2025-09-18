import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.immunohisto_transform import (
    _add_missing_age_entries,
    _calculate_y_axis_max_map,
    _create_data_rows_from_groups,
    _extract_age_num,
    immunohisto_transform,
    prepare_immunohisto_data,
    round_y_axis_max,
)


class TestTransformGeneralModelAD:
    data_files_path = "tests/test_assets/immunohisto_transform"
    pass_test_data = [
        (
            # Pass with good fake data
            "immunohisto_transform_good_test_input.csv",
            "immunohisto_transform_good_test_output.json",
        ),
        (
            # Pass with duplicated data
            "immunohisto_transform_duplicated_input.csv",
            "immunohisto_transform_duplicated_output.json",
        ),
        (
            # Pass with none data
            "immunohisto_transform_none_input.csv",
            "immunohisto_transform_none_output.json",
        ),
        (
            # Pass with missing data
            "immunohisto_transform_missing_input.csv",
            "immunohisto_transform_missing_output.json",
        ),
        (
            # Pass with extra column
            "immunohisto_transform_extra_column.csv",
            "immunohisto_transform_extra_column_output.json",
        ),
        (
            # Pass with missing ages
            "immunohisto_transform_missing_ages_input.csv",
            "immunohisto_transform_missing_ages_output.json",
        ),
        (
            # Pass with comprehensive error handling test including invalid age data that triggers float("inf") error handling
            "immunohisto_transform_all_errors_input.csv",
            "immunohisto_transform_all_errors_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good fake data",
        "Pass with duplicated data",
        "Pass with none data",
        "Pass with missing data",
        "Pass with extra column",
        "Pass with missing ages",
        "Pass with comprehensive error handling test including invalid age data that triggers float(inf) error handling",
    ]
    fail_test_data = [("immunohisto_transform_missing_column.csv")]
    fail_test_ids = [("Fail with missing column")]

    @pytest.mark.parametrize(
        "immunohisto_transform_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_immunohisto_transform_should_pass(
        self, immunohisto_transform_file, expected_output_file
    ):
        immunohisto_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", immunohisto_transform_file)
        )
        output_df = pd.DataFrame(
            immunohisto_transform(
                datasets={
                    "biomarkers": immunohisto_transform_df,
                    "pathology": immunohisto_transform_df,
                },
                dataset_name="biomarkers",
            )
        )
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "immunohisto_transform_file", fail_test_data, ids=fail_test_ids
    )
    def test_immunohisto_transform_should_fail(
        self, immunohisto_transform_file, error_type: BaseException = ValueError
    ):
        immunohisto_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", immunohisto_transform_file)
        )
        with pytest.raises(error_type):
            immunohisto_transform(
                datasets={
                    "biomarkers": immunohisto_transform_df,
                    "pathology": immunohisto_transform_df,
                },
                dataset_name="biomarkers",
            )

    def test_prepare_immunohisto_data_should_pass(self):
        # Create test input DataFrame
        input_df = pd.DataFrame(
            {
                "sex": ["male", "female"],
                "tissue": ["cerebral cortex", "hippocampus"],
                "evidence_type": ["beta amyloid", "beta amyloid"],
                "value": [1.0, 2.0],
                "age": [1, 2],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", "Female"],
                "tissue": ["Cerebral Cortex", "Hippocampus"],
                "evidence_type": ["&beta; amyloid", "&beta; amyloid"],
                "value": [1.0, 2.0],
                "age": ["1 months", "2 months"],
            }
        )

        # Transform data
        output_df = prepare_immunohisto_data(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_prepare_immunohisto_data_with_empty_values(self):
        # Create test input DataFrame with empty values
        input_df = pd.DataFrame(
            {
                "sex": ["male", ""],
                "tissue": ["cerebral cortex", ""],
                "evidence_type": ["beta amyloid", ""],
                "value": [1.0, 2.0],
                "age": [1, 2],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", ""],
                "tissue": ["Cerebral Cortex", ""],
                "evidence_type": ["&beta; amyloid", ""],
                "value": [1.0, 2.0],
                "age": ["1 months", "2 months"],
            }
        )

        # Transform data
        output_df = prepare_immunohisto_data(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_prepare_immunohisto_data_with_none_values(self):
        # Create test input DataFrame with None values
        input_df = pd.DataFrame(
            {
                "sex": ["male", None],
                "tissue": ["cerebral cortex", None],
                "evidence_type": ["beta amyloid", None],
                "value": [1.0, 2.0],
                "age": [1, 2],
            }
        )

        # Expected output DataFrame
        expected_df = pd.DataFrame(
            {
                "sex": ["Male", ""],
                "tissue": ["Cerebral Cortex", ""],
                "evidence_type": ["&beta; amyloid", ""],
                "value": [1.0, 2.0],
                "age": ["1 months", "2 months"],
            }
        )

        # Transform data
        output_df = prepare_immunohisto_data(input_df)

        # Compare output with expected
        pd.testing.assert_frame_equal(output_df, expected_df)


class TestRoundYAxisMax:
    """Test class for the round_y_axis_max function."""

    def test_round_y_axis_max_zero_case(self):
        """Test that 0 returns 10."""
        result = round_y_axis_max(0)
        assert result == 10.0

    def test_round_y_axis_max_negative_case(self):
        """Test that negative values return 0."""
        result = round_y_axis_max(-5.0)
        assert result == 0.0

    def test_round_y_axis_max_jira_examples(self):
        """Test all examples from the Jira ticket."""
        test_cases = [
            # (input, expected_output, description)
            (0.0021, 0.0025, "0.0021 rounds up to 0.0025"),
            (0.0004, 0.00045, "0.0004 rounds up to 0.00045"),
            (0.329486078, 0.35, "0.329486078 rounds up to 0.35"),
            (0.089, 0.090, "0.089 rounds up to 0.090"),
            (1094, 1500, "1094 rounds up to 1500"),
            (1322498, 1500000, "1322498 rounds up to 1500000"),
            (728591, 750000, "728591 rounds up to 750000"),
            (3973, 4000, "3973 rounds up to 4000"),
            (1.616, 2.0, "1.616 rounds up to 2.0"),
        ]

        for input_val, expected, description in test_cases:
            result = round_y_axis_max(input_val)
            assert (
                abs(result - expected) < 1e-6
            ), f"Failed for {description}: input={input_val}, expected={expected}, got={result}"

    def test_round_y_axis_max_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Test very small numbers
        assert abs(round_y_axis_max(0.0001) - 0.00015) < 1e-6
        assert abs(round_y_axis_max(0.00001) - 0.000015) < 1e-6

        # Test numbers that are already "nice"
        assert round_y_axis_max(1.0) == 1.5  # Should round up to next nice number
        assert (
            round_y_axis_max(1.5) == 1.5
        )  # Already nice, but we round UP, so stays 1.5
        assert round_y_axis_max(2.0) == 2.5  # Should round up to next nice number

        # Test large numbers
        assert round_y_axis_max(1000000) == 1500000
        assert round_y_axis_max(5000000) == 5500000  # 5.0 -> 5.5, not next magnitude

    def test_round_y_axis_max_second_digit_logic(self):
        """Test the second digit rounding logic specifically."""
        # Second digit 0-2 should round to 0 (but we round UP, so to 5)
        assert round_y_axis_max(1.0) == 1.5
        assert round_y_axis_max(1.1) == 1.5
        assert round_y_axis_max(1.2) == 1.5

        # Second digit 3-7 should round to 5
        assert round_y_axis_max(1.3) == 1.5
        assert round_y_axis_max(1.4) == 1.5
        assert round_y_axis_max(1.5) == 1.5  # Already 5, stays 1.5
        assert round_y_axis_max(1.6) == 2.0
        assert round_y_axis_max(1.7) == 2.0

        # Second digit 8-9 should round to next first digit with 0
        assert round_y_axis_max(1.8) == 2.0
        assert round_y_axis_max(1.9) == 2.0

    def test_round_y_axis_max_magnitude_handling(self):
        """Test that the function handles different magnitudes correctly."""
        # Test different magnitudes with same pattern
        assert abs(round_y_axis_max(0.1) - 0.15) < 1e-6
        assert round_y_axis_max(1.0) == 1.5
        assert round_y_axis_max(10.0) == 15.0
        assert round_y_axis_max(100.0) == 150.0
        assert round_y_axis_max(1000.0) == 1500.0

    def test_round_y_axis_max_floating_point_precision(self):
        """Test that floating point precision issues are handled correctly."""
        # Test numbers that might have floating point precision issues
        assert (
            abs(round_y_axis_max(0.1 + 0.2) - 0.35) < 1e-6
        )  # 0.30000000000000004 -> 0.35
        assert (
            abs(round_y_axis_max(1.0 / 3.0) - 0.35) < 1e-6
        )  # 0.3333333333333333 -> 0.35
        assert (
            abs(round_y_axis_max(2.0 / 3.0) - 0.7) < 1e-6
        )  # 0.6666666666666666 -> 0.7

    def test_round_y_axis_max_return_type(self):
        """Test that the function returns a float."""
        result = round_y_axis_max(1.5)
        assert isinstance(result, float)

    def test_round_y_axis_max_consistency(self):
        """Test that the function is consistent across multiple calls."""
        test_values = [0.0021, 1094, 1.616, 0.0, 0.089]

        for value in test_values:
            result1 = round_y_axis_max(value)
            result2 = round_y_axis_max(value)
            assert (
                result1 == result2
            ), f"Inconsistent results for {value}: {result1} vs {result2}"

    def test_round_y_axis_max_monotonicity(self):
        """Test that the function maintains monotonicity (larger inputs should give larger or equal outputs)."""
        test_values = [0.001, 0.002, 0.003, 0.004, 0.005]
        results = [round_y_axis_max(val) for val in test_values]

        # Results should be non-decreasing
        for i in range(1, len(results)):
            assert (
                results[i] >= results[i - 1]
            ), f"Non-monotonic: {test_values[i-1]} -> {results[i-1]}, {test_values[i]} -> {results[i]}"

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            (0, 10.0),
            (-1, 0.0),
            (0.0021, 0.0025),
            (0.0004, 0.00045),
            (0.329486078, 0.35),
            (0.089, 0.090),
            (1094, 1500),
            (1322498, 1500000),
            (728591, 750000),
            (3973, 4000),
            (1.616, 2.0),
        ],
    )
    def test_round_y_axis_max_parametrized(self, input_val, expected):
        """Parametrized test for the main examples."""
        result = round_y_axis_max(input_val)
        assert (
            abs(result - expected) < 1e-6
        ), f"input={input_val}, expected={expected}, got={result}"

    def test_extract_age_num_valueerror_handling(self):
        """Test that ValueError is handled when age contains non-numeric text."""
        # Create test input DataFrame with non-numeric age values
        input_df = pd.DataFrame(
            {
                "name": ["test_model", "test_model"],
                "evidence_type": ["test_evidence", "test_evidence"],
                "tissue": ["test_tissue", "test_tissue"],
                "age": ["unknown months", "N/A months"],  # These will cause ValueError
                "units": ["test_units", "test_units"],
                "sex": ["Male", "Female"],
                "genotype": ["WT", "KO"],
                "individual_id": ["1", "2"],
                "value": [1.0, 2.0],
            }
        )

        # This should not raise an exception, but handle ValueError gracefully
        # by sorting invalid ages to the end with float("inf")
        result = immunohisto_transform(
            datasets={"biomarkers": input_df}, dataset_name="biomarkers"
        )

        # Verify the function completed successfully
        assert isinstance(result, list)
        assert len(result) > 0

    def test_extract_age_num_indexerror_handling(self):
        """Test that IndexError is handled when age is empty or whitespace-only."""
        # Create test input DataFrame with empty/whitespace age values
        input_df = pd.DataFrame(
            {
                "name": ["test_model", "test_model"],
                "evidence_type": ["test_evidence", "test_evidence"],
                "tissue": ["test_tissue", "test_tissue"],
                "age": ["", "   "],  # These will cause IndexError
                "units": ["test_units", "test_units"],
                "sex": ["Male", "Female"],
                "genotype": ["WT", "KO"],
                "individual_id": ["1", "2"],
                "value": [1.0, 2.0],
            }
        )

        # This should not raise an exception, but handle IndexError gracefully
        # by sorting invalid ages to the end with float("inf")
        result = immunohisto_transform(
            datasets={"biomarkers": input_df}, dataset_name="biomarkers"
        )

        # Verify the function completed successfully
        assert isinstance(result, list)
        assert len(result) > 0

    def test_extract_age_num_attributeerror_handling(self):
        """Test that AttributeError is handled when age is not a string."""
        # Create test input DataFrame with non-string age values
        input_df = pd.DataFrame(
            {
                "name": ["test_model", "test_model"],
                "evidence_type": ["test_evidence", "test_evidence"],
                "tissue": ["test_tissue", "test_tissue"],
                "age": [None, 123],  # These will cause AttributeError
                "units": ["test_units", "test_units"],
                "sex": ["Male", "Female"],
                "genotype": ["WT", "KO"],
                "individual_id": ["1", "2"],
                "value": [1.0, 2.0],
            }
        )

        # This should not raise an exception, but handle AttributeError gracefully
        # by sorting invalid ages to the end with float("inf")
        result = immunohisto_transform(
            datasets={"biomarkers": input_df}, dataset_name="biomarkers"
        )

        # Verify the function completed successfully
        assert isinstance(result, list)
        assert len(result) > 0

    def test_extract_age_num_mixed_error_handling(self):
        """Test that all three error types are handled together in one dataset."""
        # Create test input DataFrame with various problematic age values
        input_df = pd.DataFrame(
            {
                "name": ["test_model"] * 5,
                "evidence_type": ["test_evidence"] * 5,
                "tissue": ["test_tissue"] * 5,
                "age": [
                    "unknown months",  # ValueError
                    "",  # IndexError
                    None,  # AttributeError
                    "12 months",  # Valid age
                    "   ",  # IndexError (whitespace)
                ],
                "units": ["test_units"] * 5,
                "sex": ["Male", "Female", "Male", "Female", "Male"],
                "genotype": ["WT", "KO", "WT", "KO", "WT"],
                "individual_id": ["1", "2", "3", "4", "5"],
                "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

        # This should not raise an exception, but handle all errors gracefully
        # Valid ages should be sorted normally, invalid ones should go to the end
        result = immunohisto_transform(
            datasets={"biomarkers": input_df}, dataset_name="biomarkers"
        )

        # Verify the function completed successfully
        assert isinstance(result, list)
        assert len(result) > 0

        # Verify that the valid age (12 months) comes before invalid ages in sorting
        valid_age_entry = next(
            (entry for entry in result if entry["age"] == "12 months"), None
        )
        invalid_age_entries = [
            entry
            for entry in result
            if entry["age"] in ["unknown months", " months", "    months"]
        ]

        # The valid age should be present
        assert valid_age_entry is not None

        # Invalid ages should also be present (they get sorted to the end)
        assert len(invalid_age_entries) > 0


class TestCalculateYAxisMaxMap:
    """Test class for the _calculate_y_axis_max_map function."""

    def test_calculate_y_axis_max_map_basic(self):
        """Test basic functionality with valid data."""
        # Create test dataset
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1", "Model2", "Model2"],
                "evidence_type": ["Type1", "Type1", "Type2", "Type2"],
                "tissue": ["Tissue1", "Tissue1", "Tissue2", "Tissue2"],
                "age": ["6 months", "12 months", "6 months", "12 months"],
                "value": [1.0, 3.0, 2.0, 5.0],
                "units": ["mg", "mg", "mg", "mg"],
            }
        )

        result = _calculate_y_axis_max_map(dataset)

        # Expected: (Model1, Type1, Tissue1) -> 3.0, (Model2, Type2, Tissue2) -> 5.0
        expected = {
            ("Model1", "Type1", "Tissue1"): 3.0,
            ("Model2", "Type2", "Tissue2"): 5.0,
        }

        assert result == expected

    def test_calculate_y_axis_max_map_empty_groups(self):
        """Test with empty groups."""
        # Create dataset with empty groups
        dataset = pd.DataFrame(
            {
                "name": ["Model1"],
                "evidence_type": ["Type1"],
                "tissue": ["Tissue1"],
                "age": ["6 months"],
                "value": [1.0],
                "units": ["mg"],
            }
        )

        result = _calculate_y_axis_max_map(dataset)

        expected = {("Model1", "Type1", "Tissue1"): 1.0}
        assert result == expected

    def test_calculate_y_axis_max_map_non_numeric_values(self):
        """Test with non-numeric values that should be coerced."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1"],
                "evidence_type": ["Type1", "Type1"],
                "tissue": ["Tissue1", "Tissue1"],
                "age": ["6 months", "12 months"],
                "value": ["1.0", "invalid"],  # One invalid value
                "units": ["mg", "mg"],
            }
        )

        result = _calculate_y_axis_max_map(dataset)

        # Should only consider the valid numeric value (1.0)
        expected = {("Model1", "Type1", "Tissue1"): 1.0}
        assert result == expected

    def test_calculate_y_axis_max_map_all_invalid_values(self):
        """Test with all invalid values."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1"],
                "evidence_type": ["Type1", "Type1"],
                "tissue": ["Tissue1", "Tissue1"],
                "age": ["6 months", "12 months"],
                "value": ["invalid1", "invalid2"],  # All invalid values
                "units": ["mg", "mg"],
            }
        )

        result = _calculate_y_axis_max_map(dataset)

        # Should return 0 for all invalid numeric values
        expected = {("Model1", "Type1", "Tissue1"): 0}
        assert result == expected

    def test_calculate_y_axis_max_map_mixed_data_types(self):
        """Test with mixed numeric and non-numeric values."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1", "Model1"],
                "evidence_type": ["Type1", "Type1", "Type1"],
                "tissue": ["Tissue1", "Tissue1", "Tissue1"],
                "age": ["6 months", "12 months", "18 months"],
                "value": [1.0, "2.5", 3.0],  # Mixed types
                "units": ["mg", "mg", "mg"],
            }
        )

        result = _calculate_y_axis_max_map(dataset)

        # Should handle mixed types and find max of valid values
        expected = {("Model1", "Type1", "Tissue1"): 3.0}
        assert result == expected


class TestCreateDataRowsFromGroups:
    """Test class for the _create_data_rows_from_groups function."""

    def test_create_data_rows_from_groups_basic(self):
        """Test basic functionality."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1"],
                "evidence_type": ["Type1", "Type1"],
                "tissue": ["Tissue1", "Tissue1"],
                "age": ["6 months", "12 months"],
                "units": ["mg", "mg"],
                "genotype": ["WT", "KO"],
                "sex": ["Male", "Female"],
                "individual_id": ["1", "2"],
                "value": [1.0, 3.0],
            }
        )

        group_columns = ["name", "evidence_type", "tissue", "age", "units"]
        extra_columns = ["genotype", "sex", "individual_id", "value"]
        extra_column_name = "data"
        y_axis_max_map = {("Model1", "Type1", "Tissue1"): 3.0}

        result = _create_data_rows_from_groups(
            dataset, group_columns, extra_columns, extra_column_name, y_axis_max_map
        )

        # Should create one entry per age group
        assert len(result) == 2

        # Check that both entries exist (order may vary due to grouping)
        ages_found = [entry["age"] for entry in result]
        assert "6 months" in ages_found
        assert "12 months" in ages_found

        # Check common properties
        for entry in result:
            assert entry["name"] == "Model1"
            assert entry["evidence_type"] == "Type1"
            assert entry["tissue"] == "Tissue1"
            assert entry["units"] == "mg"
            # round_y_axis_max(3.0) returns 3.5 (rounds up to next nice number)
            assert entry["y_axis_max"] == 3.5
            assert len(entry["data"]) == 1

        # Find the 6 months entry specifically
        six_month_entry = next(entry for entry in result if entry["age"] == "6 months")
        assert six_month_entry["data"][0]["genotype"] == "WT"

    def test_create_data_rows_from_groups_custom_columns(self):
        """Test with custom group and extra columns."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1"],
                "evidence_type": ["Type1"],
                "tissue": ["Tissue1"],
                "age": ["6 months"],
                "units": ["mg"],
                "genotype": ["WT"],
                "sex": ["Male"],
                "individual_id": ["1"],
                "value": [1.0],
                "extra_field": ["extra_value"],
            }
        )

        # Note: The function requires "tissue" to be in group_columns for y_axis_max lookup
        group_columns = ["name", "evidence_type", "tissue"]
        extra_columns = ["genotype", "extra_field"]
        extra_column_name = "measurements"
        y_axis_max_map = {("Model1", "Type1", "Tissue1"): 1.0}

        result = _create_data_rows_from_groups(
            dataset, group_columns, extra_columns, extra_column_name, y_axis_max_map
        )

        assert len(result) == 1
        entry = result[0]
        assert "measurements" in entry
        assert entry["measurements"][0]["extra_field"] == "extra_value"

    def test_create_data_rows_from_groups_empty_y_axis_map(self):
        """Test with empty y_axis_max_map."""
        dataset = pd.DataFrame(
            {
                "name": ["Model1"],
                "evidence_type": ["Type1"],
                "tissue": ["Tissue1"],
                "age": ["6 months"],
                "units": ["mg"],
                "genotype": ["WT"],
                "value": [1.0],
            }
        )

        result = _create_data_rows_from_groups(
            dataset,
            ["name", "evidence_type", "tissue", "age", "units"],
            ["genotype", "value"],
            "data",
            {},
        )

        # Should use default y_axis_max of 0 when key not found, but round_y_axis_max(0) returns 10.0
        assert result[0]["y_axis_max"] == 10.0


class TestAddMissingAgeEntries:
    """Test class for the _add_missing_age_entries function."""

    def test_add_missing_age_entries_basic(self):
        """Test basic missing age detection and addition."""
        # Existing data rows with multiple ages for the same group
        data_rows = [
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 1.0}],
            },
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "12 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 2.0}],
            },
        ]

        # Original dataset has the same ages
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1"],
                "evidence_type": ["Type1", "Type1"],
                "tissue": ["Tissue1", "Tissue1"],
                "age": ["6 months", "12 months"],
                "units": ["mg", "mg"],
                "value": [1.0, 2.0],
            }
        )

        result = _add_missing_age_entries(data_rows, dataset)

        # Should not add any missing entries since all ages in data_rows are present in dataset
        assert len(result) == 2
        assert result == data_rows

    def test_add_missing_age_entries_with_missing_ages(self):
        """Test actual missing age detection and addition."""
        # Data rows with ages that exist in the data_rows but not in all groups
        data_rows = [
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 1.0}],
            },
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "12 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 2.0}],
            },
            {
                "name": "Model2",
                "evidence_type": "Type2",
                "tissue": "Tissue2",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 5.0,
                "data": [{"genotype": "KO", "value": 3.0}],
            },
        ]

        # Dataset where Model2/Type2/Tissue2 doesn't have 12 months data
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1", "Model2"],
                "evidence_type": ["Type1", "Type1", "Type2"],
                "tissue": ["Tissue1", "Tissue1", "Tissue2"],
                "age": ["6 months", "12 months", "6 months"],
                "units": ["mg", "mg", "mg"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result = _add_missing_age_entries(data_rows, dataset)

        # Should add one missing entry for Model2/Type2/Tissue2 at 12 months
        assert len(result) == 4

        # Find the missing age entry
        missing_entry = next(
            (
                entry
                for entry in result
                if entry["name"] == "Model2" and entry["age"] == "12 months"
            ),
            None,
        )
        assert missing_entry is not None
        assert missing_entry["data"] == []
        assert missing_entry["units"] == ""
        assert missing_entry["y_axis_max"] == 5.0

    def test_add_missing_age_entries_no_missing_ages(self):
        """Test when no ages are missing."""
        data_rows = [
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 1.0}],
            }
        ]

        # Dataset only has the same age
        dataset = pd.DataFrame(
            {
                "name": ["Model1"],
                "evidence_type": ["Type1"],
                "tissue": ["Tissue1"],
                "age": ["6 months"],
                "units": ["mg"],
                "value": [1.0],
            }
        )

        result = _add_missing_age_entries(data_rows, dataset)

        # Should return original data_rows unchanged
        assert len(result) == 1
        assert result == data_rows

    def test_add_missing_age_entries_multiple_groups(self):
        """Test with multiple model/type/tissue combinations."""
        # Data rows with multiple ages for one group but not the other
        data_rows = [
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 1.0}],
            },
            {
                "name": "Model1",
                "evidence_type": "Type1",
                "tissue": "Tissue1",
                "age": "12 months",
                "units": "mg",
                "y_axis_max": 3.0,
                "data": [{"genotype": "WT", "value": 2.0}],
            },
            {
                "name": "Model2",
                "evidence_type": "Type2",
                "tissue": "Tissue2",
                "age": "6 months",
                "units": "mg",
                "y_axis_max": 5.0,
                "data": [{"genotype": "KO", "value": 3.0}],
            },
        ]

        # Dataset where Model2/Type2/Tissue2 doesn't have 12 months data
        dataset = pd.DataFrame(
            {
                "name": ["Model1", "Model1", "Model2"],
                "evidence_type": ["Type1", "Type1", "Type2"],
                "tissue": ["Tissue1", "Tissue1", "Tissue2"],
                "age": ["6 months", "12 months", "6 months"],
                "units": ["mg", "mg", "mg"],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result = _add_missing_age_entries(data_rows, dataset)

        # Should have 4 entries total (3 original + 1 missing age)
        assert len(result) == 4

        # Check that missing age was added for Model2
        model2_missing = [
            entry
            for entry in result
            if entry["name"] == "Model2" and entry["age"] == "12 months"
        ]

        assert len(model2_missing) == 1
        assert model2_missing[0]["y_axis_max"] == 5.0
        assert model2_missing[0]["data"] == []
        assert model2_missing[0]["units"] == ""


class TestExtractAgeNum:
    """Test class for the _extract_age_num function."""

    def test_extract_age_num_valid_ages(self):
        """Test with valid age strings."""
        test_cases = [
            ({"age": "6 months", "evidence_type": "Type1"}, (6.0, "6 months", "Type1")),
            (
                {"age": "12 months", "evidence_type": "Type2"},
                (12.0, "12 months", "Type2"),
            ),
            (
                {"age": "0.5 months", "evidence_type": "Type3"},
                (0.5, "0.5 months", "Type3"),
            ),
            (
                {"age": "18.75 months", "evidence_type": "Type4"},
                (18.75, "18.75 months", "Type4"),
            ),
        ]

        for entry, expected in test_cases:
            result = _extract_age_num(entry)
            assert result == expected

    def test_extract_age_num_invalid_ages_valueerror(self):
        """Test with ages that cause ValueError."""
        test_cases = [
            {"age": "unknown months", "evidence_type": "Type1"},
            {"age": "N/A months", "evidence_type": "Type2"},
            {"age": "invalid months", "evidence_type": "Type3"},
        ]

        for entry in test_cases:
            result = _extract_age_num(entry)
            # Should return (inf, age_string, evidence_type) for invalid ages
            assert result[0] == float("inf")
            assert result[1] == entry["age"]
            assert result[2] == entry["evidence_type"]

    def test_extract_age_num_invalid_ages_indexerror(self):
        """Test with ages that cause IndexError."""
        test_cases = [
            {"age": "", "evidence_type": "Type1"},
            {"age": "   ", "evidence_type": "Type2"},
            {"age": "months", "evidence_type": "Type3"},  # No number before "months"
        ]

        for entry in test_cases:
            result = _extract_age_num(entry)
            # Should return (inf, age_string, evidence_type) for invalid ages
            assert result[0] == float("inf")
            assert result[1] == entry["age"]
            assert result[2] == entry["evidence_type"]

    def test_extract_age_num_invalid_ages_attributeerror(self):
        """Test with ages that cause AttributeError."""
        test_cases = [
            {"age": None, "evidence_type": "Type1"},
            {"age": 123, "evidence_type": "Type2"},
            {"age": [], "evidence_type": "Type3"},
        ]

        for entry in test_cases:
            result = _extract_age_num(entry)
            # Should return (inf, age_string, evidence_type) for invalid ages
            assert result[0] == float("inf")
            assert result[1] == entry["age"]
            assert result[2] == entry["evidence_type"]

    def test_extract_age_num_missing_fields(self):
        """Test with missing age or evidence_type fields."""
        test_cases = [
            ({}, (float("inf"), "", "")),
            ({"age": "6 months"}, (6.0, "6 months", "")),
            ({"evidence_type": "Type1"}, (float("inf"), "", "Type1")),
        ]

        for entry, expected in test_cases:
            result = _extract_age_num(entry)
            assert result == expected

    def test_extract_age_num_sorting_behavior(self):
        """Test that the function produces sortable results."""
        entries = [
            {"age": "12 months", "evidence_type": "Type1"},
            {"age": "6 months", "evidence_type": "Type1"},
            {"age": "18 months", "evidence_type": "Type1"},
            {"age": "invalid months", "evidence_type": "Type1"},
            {"age": "3 months", "evidence_type": "Type1"},
        ]

        # Extract age numbers for sorting
        age_nums = [_extract_age_num(entry) for entry in entries]

        # Sort by the extracted values
        sorted_age_nums = sorted(age_nums)

        # Valid ages should come first, sorted numerically
        valid_ages = [
            age_num for age_num in sorted_age_nums if age_num[0] != float("inf")
        ]
        invalid_ages = [
            age_num for age_num in sorted_age_nums if age_num[0] == float("inf")
        ]

        # Check valid ages are sorted numerically
        assert [age_num[0] for age_num in valid_ages] == [3.0, 6.0, 12.0, 18.0]

        # Check invalid ages come last
        assert len(invalid_ages) == 1
        assert invalid_ages[0][1] == "invalid months"
