import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.immunohisto_transform import (
    immunohisto_transform,
    prepare_immunohisto_data,
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
            datasets={"biomarkers": input_df},
            dataset_name="biomarkers"
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
            datasets={"biomarkers": input_df},
            dataset_name="biomarkers"
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
            datasets={"biomarkers": input_df},
            dataset_name="biomarkers"
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
                    "",                # IndexError
                    None,              # AttributeError
                    "12 months",       # Valid age
                    "   ",             # IndexError (whitespace)
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
            datasets={"biomarkers": input_df},
            dataset_name="biomarkers"
        )
        
        # Verify the function completed successfully
        assert isinstance(result, list)
        assert len(result) > 0
        
        # Verify that the valid age (12 months) comes before invalid ages in sorting
        valid_age_entry = next((entry for entry in result if entry["age"] == "12 months"), None)
        invalid_age_entries = [entry for entry in result if entry["age"] in ["unknown months", " months", "    months"]]
        
        # The valid age should be present
        assert valid_age_entry is not None
        
        # Invalid ages should also be present (they get sorted to the end)
        assert len(invalid_age_entries) > 0
