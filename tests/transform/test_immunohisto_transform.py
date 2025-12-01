"""
Test suite for immunohisto_transform module.

This module contains comprehensive tests for the biomarkers and pathology data
transformation functions used in the Model AD project. It validates:
- Data preparation and cleaning (capitalization, beta symbol replacement, age formatting)
- Y-axis maximum calculation and rounding for visualizations
- Missing age entry detection and placeholder addition
- Age value extraction and sorting behavior
- End-to-end transformation with various data scenarios and edge cases
"""

import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.immunohisto_transform import (
    _add_missing_age_entries,
    _calculate_y_axis_max_map,
    _create_measure_order_key,
    _extract_age_num,
    immunohisto_transform,
    prepare_immunohisto_data,
    round_y_axis_max,
)


def _load_test_measure_order_config():
    """Load the test measure order config from test assets as a DataFrame."""
    from agoradatatools.etl.extract import read_yaml_into_df

    config_path = os.path.join(
        "tests/test_assets/immunohisto_transform/input",
        "immunohisto_measure_order.yaml",
    )

    return read_yaml_into_df(config_path)


class TestTransformGeneralModelAD:
    """
    Test suite for the main immunohisto_transform function and related data preparation.

    This class tests the end-to-end transformation of biomarkers and pathology data
    for the Model AD project, including proper handling of various data formats,
    edge cases, and error conditions.
    """

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
    ]
    pass_test_ids = [
        "Pass with good fake data",
        "Pass with duplicated data",
        "Pass with none data",
        "Pass with missing data",
        "Pass with extra column",
        "Pass with missing ages",
    ]
    fail_test_data = [
        "immunohisto_transform_missing_column.csv",
        "immunohisto_transform_all_errors_input.csv",
    ]
    fail_test_ids = [
        "Fail with missing column",
        "Fail with invalid ages",
    ]

    @pytest.mark.parametrize(
        "immunohisto_transform_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_immunohisto_transform_should_pass(
        self, immunohisto_transform_file: str, expected_output_file: str
    ) -> None:
        """
        Test that immunohisto_transform produces correct output for valid inputs.

        This parametrized test covers multiple scenarios including good data,
        duplicated entries, missing values, and edge cases.

        Args:
            immunohisto_transform_file: Name of CSV input file in test_assets
            expected_output_file: Name of expected JSON output file in test_assets
        """
        immunohisto_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", immunohisto_transform_file)
        )
        output_df = pd.DataFrame(
            immunohisto_transform(
                datasets={
                    "biomarkers": immunohisto_transform_df,
                    "pathology": immunohisto_transform_df,
                    "immunohisto_measure_order": _load_test_measure_order_config(),
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
        self, immunohisto_transform_file: str, error_type: BaseException = ValueError
    ) -> None:
        """
        Test that immunohisto_transform raises appropriate errors for invalid inputs.

        This parametrized test verifies that the function correctly raises ValueError
        when required columns are missing from the input data.

        Args:
            immunohisto_transform_file: Name of CSV input file with missing columns
            error_type: Expected exception type (defaults to ValueError)
        """
        immunohisto_transform_df = pd.read_csv(
            os.path.join(self.data_files_path, "input", immunohisto_transform_file)
        )
        with pytest.raises(error_type):
            immunohisto_transform(
                datasets={
                    "biomarkers": immunohisto_transform_df,
                    "pathology": immunohisto_transform_df,
                    "immunohisto_measure_order": _load_test_measure_order_config(),
                },
                dataset_name="biomarkers",
            )

    def test_prepare_immunohisto_data_should_pass(self) -> None:
        """
        Test that prepare_immunohisto_data correctly transforms data.

        Verifies that the function:
        - Capitalizes 'sex' and 'tissue' columns
        - Replaces 'beta' with '&beta;' in evidence_type
        - Appends 'months' to age values
        """
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

    def test_prepare_immunohisto_data_with_empty_values(self) -> None:
        """Test that prepare_immunohisto_data handles empty string values correctly."""
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

    def test_prepare_immunohisto_data_with_none_values(self) -> None:
        """Test that prepare_immunohisto_data handles None/NaN values by converting them to empty strings."""
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

    def test_immunohisto_transform_with_empty_dataset(self) -> None:
        """
        Test that empty dataset returns empty list.

        Verifies that when an empty DataFrame is provided (with proper column structure),
        the function returns an empty list rather than failing.
        """
        # Create empty DataFrame with required columns
        empty_df = pd.DataFrame(
            columns=[
                "name",
                "evidence_type",
                "value",
                "units",
                "age",
                "tissue",
                "sex",
                "genotype",
                "individual_id",
            ]
        )

        result = immunohisto_transform(
            datasets={
                "biomarkers": empty_df,
                "immunohisto_measure_order": _load_test_measure_order_config(),
            },
            dataset_name="biomarkers",
        )

        # Should return empty list
        assert result == []
        assert isinstance(result, list)

    def test_immunohisto_transform_missing_both_datasets(self) -> None:
        """
        Test that ValueError is raised when neither biomarkers nor pathology is present.

        Verifies that the function properly validates input datasets and raises an error
        when the required dataset types (biomarkers or pathology) are not provided.
        """
        # Create a dataset that is neither biomarkers nor pathology
        other_df = pd.DataFrame(
            {
                "name": ["test"],
                "evidence_type": ["test"],
                "value": [1.0],
                "units": ["test"],
                "age": [1],
                "tissue": ["test"],
                "sex": ["male"],
                "genotype": ["test"],
                "individual_id": ["test"],
            }
        )

        with pytest.raises(ValueError, match="At least one of"):
            immunohisto_transform(
                datasets={
                    "other_data": other_df,
                    "immunohisto_measure_order": _load_test_measure_order_config(),
                },
                dataset_name="other_data",
            )

    def test_immunohisto_transform_missing_measure_order_config(self) -> None:
        """
        Test that ValueError is raised when measure order config is missing.

        Verifies that the function properly validates that the config dataset is provided.
        """
        # Create a test DataFrame
        input_df = pd.DataFrame(
            {
                "name": ["test"],
                "evidence_type": ["test"],
                "value": [1.0],
                "units": ["test"],
                "age": [1],
                "tissue": ["test"],
                "sex": ["male"],
                "genotype": ["test"],
                "individual_id": ["test"],
            }
        )

        with pytest.raises(ValueError, match="immunohisto_measure_order.*required"):
            immunohisto_transform(
                datasets={"biomarkers": input_df},
                dataset_name="biomarkers",
            )

    def test_immunohisto_transform_with_pathology_dataset(self) -> None:
        """
        Test that the transform works with pathology dataset name.

        Verifies that the function correctly processes data when using 'pathology'
        as the dataset_name parameter instead of 'biomarkers'.
        """
        # Create test input DataFrame
        input_df = pd.DataFrame(
            {
                "name": ["ModelA", "ModelA"],
                "evidence_type": ["TypeA", "TypeA"],
                "value": [1, 2],
                "units": ["A", "A"],
                "age": [1, 1],
                "tissue": ["TissueA", "TissueA"],
                "sex": ["male", "male"],
                "genotype": ["genotype1", "genotype1"],
                "individual_id": ["individual_1", "individual_2"],
            }
        )

        # Transform using pathology dataset name
        result = immunohisto_transform(
            datasets={
                "pathology": input_df,
                "immunohisto_measure_order": _load_test_measure_order_config(),
            },
            dataset_name="pathology",
        )

        # Should successfully return results
        assert isinstance(result, list)
        assert len(result) > 0
        # Verify structure
        assert "name" in result[0]
        assert "evidence_type" in result[0]
        assert "tissue" in result[0]
        assert "age" in result[0]
        assert "data" in result[0]
        assert "y_axis_max" in result[0]

    def test_immunohisto_transform_measure_type_ordering_biomarkers(self) -> None:
        """
        Test that biomarkers are ordered according to the BIOMARKER_MEASURE_ORDER constant.

        Validates that the transform function correctly orders evidence types
        for biomarkers according to the measure order constants.
        """
        # Create test data with biomarkers in non-alphabetical order
        # Using the first 3 types from BIOMARKER_MEASURE_ORDER
        # Note: Using "beta" in raw form since prepare_immunohisto_data will convert it to "&beta;"
        input_df = pd.DataFrame(
            {
                "name": ["ModelA"] * 6,
                "evidence_type": [
                    "Soluble Abeta42",
                    "NfL",
                    "Soluble Abeta40",
                    "Soluble Abeta42",
                    "NfL",
                    "Soluble Abeta40",
                ],
                "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "units": ["pg/mg"] * 6,
                "age": [6, 6, 6, 12, 12, 12],
                "tissue": ["Brain"] * 6,
                "sex": ["male"] * 6,
                "genotype": ["WT"] * 6,
                "individual_id": ["1", "2", "3", "4", "5", "6"],
            }
        )

        # Transform
        result = immunohisto_transform(
            datasets={
                "biomarkers": input_df,
                "immunohisto_measure_order": _load_test_measure_order_config(),
            },
            dataset_name="biomarkers",
        )

        # Extract the evidence_type ordering from the result
        evidence_types = [entry["evidence_type"] for entry in result]

        # Expected order based on measure order config:
        # NfL, Soluble A&beta;40, Soluble A&beta;42 (for both age 6 and 12)
        expected_order = [
            "NfL",
            "Soluble A&beta;40",
            "Soluble A&beta;42",
            "NfL",
            "Soluble A&beta;40",
            "Soluble A&beta;42",
        ]

        assert evidence_types == expected_order

    def test_immunohisto_transform_measure_type_ordering_pathology(self) -> None:
        """
        Test that pathology measures are ordered according to the PATHOLOGY_MEASURE_ORDER constant.

        Validates that the transform function correctly orders evidence types
        for pathology according to the measure order constants.
        """
        # Create test data with pathology measures in non-alphabetical order
        # Using the first 3 types from PATHOLOGY_MEASURE_ORDER
        input_df = pd.DataFrame(
            {
                "name": ["ModelA"] * 3,
                "evidence_type": [
                    "Tau (HT7)",
                    "Plaque Density (Thio-S)",
                    "Plaque Size (Thio-S)",
                ],
                "value": [10.0, 20.0, 30.0],
                "units": ["cells/mm2", "plaques/mm2", "plaques/mm2"],
                "age": [12, 12, 12],
                "tissue": ["Hippocampus"] * 3,
                "sex": ["female"] * 3,
                "genotype": ["KO"] * 3,
                "individual_id": ["1", "2", "3"],
            }
        )

        # Transform
        result = immunohisto_transform(
            datasets={
                "pathology": input_df,
                "immunohisto_measure_order": _load_test_measure_order_config(),
            },
            dataset_name="pathology",
        )

        # Extract the evidence_type ordering from the result
        evidence_types = [entry["evidence_type"] for entry in result]

        # Expected order based on PATHOLOGY_MEASURE_ORDER:
        # Plaque Density (Thio-S), Plaque Size (Thio-S), Tau (HT7)
        expected_order = [
            "Plaque Density (Thio-S)",
            "Plaque Size (Thio-S)",
            "Tau (HT7)",
        ]

        assert evidence_types == expected_order

    def test_immunohisto_transform_unlisted_measure_types(self) -> None:
        """
        Test that unlisted measure types are sorted alphabetically after listed ones.

        Validates that evidence types not in BIOMARKER_MEASURE_ORDER are placed after
        configured types and sorted alphabetically among themselves.
        """
        # Create test data with both listed and unlisted types
        # Note: Using "beta" in raw form since prepare_immunohisto_data will convert it to "&beta;"
        input_df = pd.DataFrame(
            {
                "name": ["ModelA"] * 4,
                "evidence_type": [
                    "Unknown Type B",
                    "NfL",
                    "Unknown Type A",
                    "Soluble Abeta40",
                ],
                "value": [1.0, 2.0, 3.0, 4.0],
                "units": ["pg/mg"] * 4,
                "age": [12] * 4,
                "tissue": ["Brain"] * 4,
                "sex": ["male"] * 4,
                "genotype": ["WT"] * 4,
                "individual_id": ["1", "2", "3", "4"],
            }
        )

        # Transform
        result = immunohisto_transform(
            datasets={
                "biomarkers": input_df,
                "immunohisto_measure_order": _load_test_measure_order_config(),
            },
            dataset_name="biomarkers",
        )

        # Extract the evidence_type ordering from the result
        evidence_types = [entry["evidence_type"] for entry in result]

        # Expected order: NfL, Soluble A&beta;40 (from measure order config),
        # then Unknown Type A, Unknown Type B (alphabetical)
        expected_order = [
            "NfL",
            "Soluble A&beta;40",
            "Unknown Type A",
            "Unknown Type B",
        ]

        assert evidence_types == expected_order


class TestRoundYAxisMax:
    """Test class for the round_y_axis_max function."""

    def test_round_y_axis_max_zero_case(self) -> None:
        """
        Test that 0 returns 10.

        Zero is a special case that defaults to 10.0 for visualization purposes.
        """
        result = round_y_axis_max(0)
        assert result == pytest.approx(10.0)

    def test_round_y_axis_max_negative_case(self) -> None:
        """
        Test that negative values return 0.

        Negative values are not expected in the data and default to 0.0.
        """
        result = round_y_axis_max(-5.0)
        assert result == pytest.approx(0.0)

    def test_round_y_axis_max_edge_cases(self) -> None:
        """
        Test edge cases and boundary conditions.

        Tests very small numbers, "nice" numbers that still need rounding up,
        and large numbers to ensure the algorithm works across all scales.
        """
        # Test very small numbers
        assert abs(round_y_axis_max(0.0001) - 0.00015) < 1e-6
        assert abs(round_y_axis_max(0.00001) - 0.000015) < 1e-6

        # Test numbers that are already "nice"
        assert round_y_axis_max(1.0) == pytest.approx(
            1.5
        )  # Should round up to next nice number
        assert round_y_axis_max(1.5) == pytest.approx(
            2.0
        )  # According to JIRA instructions, always round UP to next 5 or 0
        assert round_y_axis_max(2.0) == pytest.approx(
            2.5
        )  # Should round up to next nice number

        # Test large numbers
        assert round_y_axis_max(1000000) == pytest.approx(1500000)
        assert round_y_axis_max(5000000) == pytest.approx(
            5500000
        )  # 5.0 -> 5.5, not next magnitude

    def test_round_y_axis_max_second_digit_logic(self) -> None:
        """
        Test the second digit rounding logic specifically.

        Validates that numbers are always rounded UP to the next value where
        the second significant digit is 0 or 5.
        """
        # Second digit 0-2 should round to 0 (but we round UP, so to 5)
        assert round_y_axis_max(1.0) == pytest.approx(1.5)
        assert round_y_axis_max(1.1) == pytest.approx(1.5)
        assert round_y_axis_max(1.2) == pytest.approx(1.5)

        # Second digit 3-7 should round to 5
        assert round_y_axis_max(1.3) == pytest.approx(1.5)
        assert round_y_axis_max(1.4) == pytest.approx(1.5)
        assert round_y_axis_max(1.5) == pytest.approx(2.0)
        assert round_y_axis_max(1.6) == pytest.approx(2.0)
        assert round_y_axis_max(1.7) == pytest.approx(2.0)

        # Second digit 8-9 should round to next first digit with 0
        assert round_y_axis_max(1.8) == pytest.approx(2.0)
        assert round_y_axis_max(1.9) == pytest.approx(2.0)

    def test_round_y_axis_max_magnitude_handling(self) -> None:
        """
        Test that the function handles different magnitudes correctly.

        Ensures consistent rounding behavior across different orders of magnitude
        (0.1, 1, 10, 100, 1000, etc.).
        """
        # Test different magnitudes with same pattern
        assert abs(round_y_axis_max(0.1) - 0.15) < 1e-6
        assert round_y_axis_max(1.0) == pytest.approx(1.5)
        assert round_y_axis_max(10.0) == pytest.approx(15.0)
        assert round_y_axis_max(100.0) == pytest.approx(150.0)
        assert round_y_axis_max(1000.0) == pytest.approx(1500.0)

    def test_round_y_axis_max_floating_point_precision(self) -> None:
        """
        Test that floating point precision issues are handled correctly.

        Verifies that common floating point arithmetic issues (like 0.1 + 0.2)
        don't cause incorrect rounding results.
        """
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

    def test_round_y_axis_max_return_type(self) -> None:
        """
        Test that the function returns a float.

        Ensures the return type is always float, even for integer inputs.
        """
        result = round_y_axis_max(1.5)
        assert isinstance(result, float)

    def test_round_y_axis_max_consistency(self) -> None:
        """
        Test that the function is consistent across multiple calls.

        Verifies that calling the function multiple times with the same input
        always produces the same output.
        """
        test_values = [0.0021, 1094, 1.616, 0.0, 0.089]

        for value in test_values:
            result1 = round_y_axis_max(value)
            result2 = round_y_axis_max(value)
            assert (
                result1 == result2
            ), f"Inconsistent results for {value}: {result1} vs {result2}"

    def test_round_y_axis_max_monotonicity(self) -> None:
        """
        Test that the function maintains monotonicity.

        Ensures that larger inputs always produce larger or equal outputs,
        which is critical for proper data visualization scaling.
        """
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
    def test_round_y_axis_max_parametrized(
        self, input_val: float, expected: float
    ) -> None:
        """
        Parametrized test for the main examples.

        Comprehensive parametrized test covering all key test cases including
        zero, negative, and various positive values from the JIRA specification.

        Args:
            input_val: Input value to test
            expected: Expected rounded output
        """
        result = round_y_axis_max(input_val)
        assert (
            abs(result - expected) < 1e-6
        ), f"input={input_val}, expected={expected}, got={result}"

    def test_round_y_axis_max_string_inputs(self) -> None:
        """
        Test that string inputs are properly converted and handled.

        Validates that the function accepts string inputs (per type signature)
        and properly converts valid numeric strings or returns 10.0 for invalid ones.
        """
        # Valid string numbers
        assert abs(round_y_axis_max("1.5") - 2.0) < 1e-6
        assert abs(round_y_axis_max("100") - 150.0) < 1e-6
        assert abs(round_y_axis_max("0.0021") - 0.0025) < 1e-6

        # Invalid string (should return 10.0)
        assert round_y_axis_max("invalid") == pytest.approx(10.0)
        assert round_y_axis_max("abc123") == pytest.approx(10.0)
        assert round_y_axis_max("") == pytest.approx(10.0)

        # None input (should return 10.0)
        assert round_y_axis_max(None) == pytest.approx(10.0)

    def test_extract_age_num_valueerror_handling(self) -> None:
        """Test that non-numeric age values raise a descriptive error."""
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

        with pytest.raises(ValueError, match="Invalid age value"):
            immunohisto_transform(
                datasets={"biomarkers": input_df}, dataset_name="biomarkers"
            )

    def test_extract_age_num_indexerror_handling(self) -> None:
        """Test that empty or whitespace-only age values raise a descriptive error."""
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

        with pytest.raises(ValueError, match="Invalid age value"):
            immunohisto_transform(
                datasets={"biomarkers": input_df}, dataset_name="biomarkers"
            )

    def test_extract_age_num_attributeerror_handling(self) -> None:
        """Test that non-string age values raise a descriptive error."""
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

        with pytest.raises(ValueError, match="Invalid age value"):
            immunohisto_transform(
                datasets={"biomarkers": input_df}, dataset_name="biomarkers"
            )

    def test_extract_age_num_mixed_error_handling(self) -> None:
        """Test that datasets with mixed invalid ages raise a descriptive error."""
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

        with pytest.raises(ValueError, match="Invalid age value"):
            immunohisto_transform(
                datasets={"biomarkers": input_df}, dataset_name="biomarkers"
            )

    def test_immunohisto_transform_invalid_age_error_message(self) -> None:
        """Ensure the transformation error message includes the invalid age value."""

        input_df = pd.DataFrame(
            {
                "name": ["test_model"],
                "evidence_type": ["test_evidence"],
                "tissue": ["test_tissue"],
                "age": ["invalid months"],
                "units": ["test_units"],
                "sex": ["Male"],
                "genotype": ["WT"],
                "individual_id": ["1"],
                "value": [1.0],
            }
        )

        with pytest.raises(ValueError, match="Invalid age value: 'invalid months'"):
            immunohisto_transform(
                datasets={"biomarkers": input_df}, dataset_name="biomarkers"
            )


class TestCalculateYAxisMaxMap:
    """Test class for the _calculate_y_axis_max_map function."""

    def test_calculate_y_axis_max_map_basic(self) -> None:
        """
        Test basic functionality with valid data.

        Validates that the function correctly calculates y_axis_max values for
        different combinations of (name, evidence_type, tissue).
        """
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

        # Expected: (Model1, Type1, Tissue1) -> round_y_axis_max(3.0) = 3.5, (Model2, Type2, Tissue2) -> round_y_axis_max(5.0) = 5.5
        expected = {
            ("Model1", "Type1", "Tissue1"): 3.5,
            ("Model2", "Type2", "Tissue2"): 5.5,
        }

        assert result == expected

    def test_calculate_y_axis_max_map_empty_groups(self) -> None:
        """
        Test with minimal data (single group).

        Validates that the function handles datasets with only one group correctly.
        """
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

        expected = {("Model1", "Type1", "Tissue1"): 1.5}
        assert result == expected

    def test_calculate_y_axis_max_map_non_numeric_values(self) -> None:
        """
        Test with non-numeric values that should be coerced.

        Validates that the function handles mixed valid/invalid values by
        coercing non-numeric values to NaN and only considering valid ones.
        """
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

        expected = {("Model1", "Type1", "Tissue1"): 1.5}
        assert result == expected

    def test_calculate_y_axis_max_map_all_invalid_values(self) -> None:
        """
        Test with all invalid values.

        Validates that when all values are non-numeric, the function returns
        the default y_axis_max of 10.0.
        """
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

        # Should return round_y_axis_max(0) = 10.0 for all invalid numeric values
        expected = {("Model1", "Type1", "Tissue1"): 10.0}
        assert result == expected

    def test_calculate_y_axis_max_map_mixed_data_types(self) -> None:
        """
        Test with mixed numeric and non-numeric values.

        Validates that the function correctly handles values that are numeric,
        string-numeric, and truly numeric, finding the max and rounding it.
        """
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

        expected = {("Model1", "Type1", "Tissue1"): 3.5}
        assert result == expected


class TestAddMissingAgeEntries:
    """Test class for the _add_missing_age_entries function."""

    def test_add_missing_age_entries_basic(self) -> None:
        """
        Test basic missing age detection when no ages are missing.

        Validates that when all age combinations are already present,
        no additional entries are added.
        """
        # Existing data rows with multiple ages for the same group
        data_rows = pd.DataFrame(
            [
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
        )

        result = _add_missing_age_entries(data_rows)

        # Should not add any missing entries since all ages are already present for all groups
        assert len(result) == 2
        # Convert to dicts for comparison
        result_dicts = result.to_dict("records")
        expected_dicts = data_rows.to_dict("records")
        assert result_dicts == expected_dicts

    def test_add_missing_age_entries_with_missing_ages(self) -> None:
        """
        Test actual missing age detection and addition.

        Validates that when some age time points are missing for certain groups,
        the function adds placeholder entries with empty data arrays.
        """
        # Data rows with ages that exist in the data_rows but not in all groups
        data_rows = pd.DataFrame(
            [
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
        )

        result = _add_missing_age_entries(data_rows)

        # Should add one missing entry for Model2/Type2/Tissue2 at 12 months
        assert len(result) == 4

        # Convert to dicts for easier inspection
        result_dicts = result.to_dict("records")

        # Find the missing age entry
        missing_entry = next(
            (
                entry
                for entry in result_dicts
                if entry["name"] == "Model2" and entry["age"] == "12 months"
            ),
            None,
        )
        assert missing_entry is not None
        assert missing_entry["data"] == []
        assert missing_entry["units"] == ""
        assert missing_entry["y_axis_max"] == pytest.approx(5.0)

    def test_add_missing_age_entries_no_missing_ages(self) -> None:
        """
        Test when no ages are missing (single age case).

        Validates that when there's only one age in the dataset,
        no additional entries need to be added.
        """
        data_rows = pd.DataFrame(
            [
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
        )

        result = _add_missing_age_entries(data_rows)

        # Should return data_rows unchanged (only one age, so nothing to fill)
        assert len(result) == 1
        result_dicts = result.to_dict("records")
        expected_dicts = data_rows.to_dict("records")
        assert result_dicts == expected_dicts


class TestMeasureOrderConfig:
    """Test class for validating the measure order configuration structure."""

    def test_measure_order_config_structure(self) -> None:
        """
        Test that the test measure order config has the expected structure.

        Validates that the test config file has the required columns (generic key/items)
        and expected evidence types.
        """
        config = _load_test_measure_order_config()

        # Check for generic columns returned by read_yaml_into_df
        assert "key" in config.columns
        assert "items" in config.columns

        # Check biomarkers
        biomarkers = config[config["key"] == "biomarkers"]
        expected_biomarkers = [
            "NfL",
            "Soluble A&beta;40",
            "Soluble A&beta;42",
            "Insoluble A&beta;40",
            "Insoluble A&beta;42",
        ]
        assert biomarkers["items"].tolist() == expected_biomarkers

        # Check pathology
        pathology = config[config["key"] == "pathology"]
        expected_pathology = [
            "Plaque Density (Thio-S)",
            "Plaque Size (Thio-S)",
            "Tau (HT7)",
            "Phospho-Tau (AT8)",
            "Dystrophic Neurites (LAMP1)",
            "Microglia Cell Density (IBA1)",
            "Astrocyte Cell Density (GFAP)",
            "Astrocyte Cell Density (S100B)",
        ]
        assert pathology["items"].tolist() == expected_pathology


class TestCreateMeasureOrderKey:
    """Test class for the _create_measure_order_key function."""

    def test_create_measure_order_key_basic_ordering(self) -> None:
        """
        Test basic ordering functionality.

        Validates that the sorting key function correctly orders evidence types
        according to provided config.
        """
        # Create a config DataFrame
        config_df = pd.DataFrame(
            {
                "dataset_name": ["biomarkers", "biomarkers", "biomarkers"],
                "evidence_type": ["NfL", "Soluble A&beta;40", "Soluble A&beta;42"],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        key_func = _create_measure_order_key(datasets, "biomarkers")

        # Test that the key function returns correct tuples for biomarkers
        assert key_func("NfL") == (0, "NfL")
        assert key_func("Soluble A&beta;40") == (1, "Soluble A&beta;40")
        assert key_func("Soluble A&beta;42") == (2, "Soluble A&beta;42")

    def test_create_measure_order_key_unlisted_types(self) -> None:
        """
        Test that unlisted evidence types are sorted after listed ones.

        Validates that types not in the config get a high index value
        and are sorted alphabetically among themselves.
        """
        # Create a config DataFrame
        config_df = pd.DataFrame(
            {
                "dataset_name": ["biomarkers", "biomarkers"],
                "evidence_type": ["NfL", "Soluble A&beta;40"],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        key_func = _create_measure_order_key(datasets, "biomarkers")

        # Unlisted types should get a large index
        unlisted_key = key_func("Unknown Type")
        assert unlisted_key[0] > 1000
        assert unlisted_key[1] == "Unknown Type"

    def test_create_measure_order_key_sorting_behavior(self) -> None:
        """
        Test actual sorting behavior with mixed listed and unlisted types.

        Validates that when used with Python's sorted(), the key function
        produces the correct ordering.
        """
        # Create a config DataFrame
        config_df = pd.DataFrame(
            {
                "dataset_name": ["biomarkers", "biomarkers", "biomarkers"],
                "evidence_type": ["NfL", "Soluble A&beta;40", "Soluble A&beta;42"],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        key_func = _create_measure_order_key(datasets, "biomarkers")

        # Create a list with both configured and unconfigured types
        evidence_types = [
            "Unknown 2",
            "Soluble A&beta;40",
            "Unknown 1",
            "NfL",
            "Soluble A&beta;42",
        ]

        sorted_types = sorted(evidence_types, key=key_func)

        # Expected order: configured types first (in order),
        # then alphabetical (Unknown 1, Unknown 2)
        expected = [
            "NfL",
            "Soluble A&beta;40",
            "Soluble A&beta;42",
            "Unknown 1",
            "Unknown 2",
        ]
        assert sorted_types == expected

    def test_create_measure_order_key_pathology_dataset(self) -> None:
        """
        Test ordering for pathology dataset.

        Validates that the function works correctly when using the 'pathology'
        dataset name.
        """
        # Create a config DataFrame
        config_df = pd.DataFrame(
            {
                "dataset_name": ["biomarkers", "pathology", "pathology", "pathology"],
                "evidence_type": [
                    "NfL",
                    "Plaque Density (Thio-S)",
                    "Plaque Size (Thio-S)",
                    "Tau (HT7)",
                ],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        key_func = _create_measure_order_key(datasets, "pathology")

        # Test that it uses pathology config
        assert key_func("Plaque Density (Thio-S)") == (0, "Plaque Density (Thio-S)")
        assert key_func("Plaque Size (Thio-S)") == (1, "Plaque Size (Thio-S)")
        assert key_func("Tau (HT7)") == (2, "Tau (HT7)")

        # NfL should be unlisted for pathology
        nfl_key = key_func("NfL")
        assert nfl_key[0] > 1000

    def test_create_measure_order_key_unknown_dataset(self) -> None:
        """
        Test behavior with unknown dataset name.

        Validates that when an unknown dataset name is provided, all types are
        treated as unlisted and sorted alphabetically.
        """
        # Create a config DataFrame
        config_df = pd.DataFrame(
            {
                "dataset_name": ["biomarkers", "pathology"],
                "evidence_type": ["NfL", "Tau (HT7)"],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        key_func = _create_measure_order_key(datasets, "unknown_dataset")

        # All types should be unlisted since the dataset isn't in the config
        key1 = key_func("Type A")
        key2 = key_func("Type B")

        # Both should have the same large index, sorted alphabetically
        assert key1[0] == key2[0]
        assert key1[0] >= 1000
        assert key1[1] < key2[1]  # Alphabetical ordering

    def test_create_measure_order_key_missing_config(self) -> None:
        """
        Test that ValueError is raised when config is missing from datasets.

        Validates proper error handling when config dataset is not provided.
        """
        datasets = {"biomarkers": pd.DataFrame()}

        with pytest.raises(ValueError, match="Measure order configuration.*not found"):
            _create_measure_order_key(datasets, "biomarkers")

    def test_create_measure_order_key_invalid_columns(self) -> None:
        """
        Test that ValueError is raised when config has wrong columns.

        Validates proper error handling for malformed config.
        """
        # Create config with wrong columns
        config_df = pd.DataFrame(
            {
                "wrong_column": ["biomarkers"],
                "another_wrong_column": ["NfL"],
            }
        )
        datasets = {"immunohisto_measure_order": config_df}

        with pytest.raises(
            ValueError, match="must have 'dataset_name' and 'evidence_type' columns"
        ):
            _create_measure_order_key(datasets, "biomarkers")


class TestExtractAgeNum:
    """Test class for the _extract_age_num function."""

    def test_extract_age_num_valid_ages(self) -> None:
        """
        Test with valid age strings.

        Validates that the function correctly extracts numeric values from
        properly formatted age strings like "6 months" or "12.5 months".
        """
        test_cases = [
            ("6 months", 6.0),
            ("12 months", 12.0),
            ("0.5 months", 0.5),
            ("18.75 months", 18.75),
        ]

        for age_str, expected in test_cases:
            result = _extract_age_num(age_str)
            assert result == expected

    def test_extract_age_num_invalid_ages_valueerror(self) -> None:
        """Test that age strings with non-numeric text raise ValueError."""
        test_cases = [
            "unknown months",
            "N/A months",
            "invalid months",
        ]

        for age_str in test_cases:
            with pytest.raises(ValueError, match="Invalid age value"):
                _extract_age_num(age_str)

    def test_extract_age_num_invalid_ages_indexerror(self) -> None:
        """Test that empty strings raise ValueError."""
        test_cases = [
            "",
            "   ",
            "months",  # No number before "months"
        ]

        for age_str in test_cases:
            with pytest.raises(ValueError, match="Invalid age value"):
                _extract_age_num(age_str)

    def test_extract_age_num_invalid_ages_attributeerror(self) -> None:
        """Test that non-string types raise ValueError."""
        test_cases = [
            None,
            123,
            [],
        ]

        for age_value in test_cases:
            with pytest.raises(ValueError, match="Invalid age value"):
                _extract_age_num(age_value)

    def test_extract_age_num_empty_string(self) -> None:
        """Test that an empty string raises ValueError."""

        with pytest.raises(ValueError, match="Invalid age value"):
            _extract_age_num("")

    def test_extract_age_num_sorting_behavior(self) -> None:
        """
        Test that the function produces sortable results.

        Validates that valid ages sort numerically once converted to floats.
        """
        age_strings = ["12 months", "6 months", "18 months", "3 months"]

        # Extract age numbers for sorting
        age_nums = [_extract_age_num(age_str) for age_str in age_strings]

        # Sort by the extracted values
        sorted_age_nums = sorted(age_nums)

        # Valid ages should come first, sorted numerically
        expected_ages = [3.0, 6.0, 12.0, 18.0]
        assert len(sorted_age_nums) == len(expected_ages)
        for actual, expected in zip(sorted_age_nums, expected_ages):
            assert actual == pytest.approx(expected)
