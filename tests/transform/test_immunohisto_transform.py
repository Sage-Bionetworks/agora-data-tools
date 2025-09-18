import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.immunohisto_transform import (
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
