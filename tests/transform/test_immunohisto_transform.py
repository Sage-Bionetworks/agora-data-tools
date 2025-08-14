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
    ]
    pass_test_ids = [
        "Pass with good fake data",
        "Pass with duplicated data",
        "Pass with none data",
        "Pass with missing data",
        "Pass with extra column",
        "Pass with missing ages",
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

    def test_immunohisto_transform_missing_ages_functionality(self):
        """Test that the transform adds missing age entries for each name/evidence_type/tissue combination."""
        # Create test input with missing ages
        input_df = pd.DataFrame(
            {
                "name": ["ModelA", "ModelA", "ModelA", "ModelB"],
                "evidence_type": ["TypeA", "TypeA", "TypeA", "TypeA"],
                "tissue": ["TissueA", "TissueA", "TissueA", "TissueB"],
                "age": [1, 2, 3, 1],  # ModelB/TypeA/TissueB missing age 2 and 3
                "units": ["A", "A", "A", "A"],
                "sex": ["male", "male", "male", "male"],
                "genotype": ["genotype1", "genotype1", "genotype1", "genotype1"],
                "individual_id": ["individual_1", "individual_2", "individual_3", "individual_4"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        # Run the transform
        result = immunohisto_transform(
            datasets={"biomarkers": input_df},
            dataset_name="biomarkers",
        )

        # Check that we have entries for all ages
        ages = [entry["age"] for entry in result]
        names = [entry["name"] for entry in result]
        evidence_types = [entry["evidence_type"] for entry in result]
        tissues = [entry["tissue"] for entry in result]

        # Should have entries for all combinations
        assert "1 months" in ages
        assert "2 months" in ages
        assert "3 months" in ages

        # Check that missing age entries have empty data arrays
        missing_age_entries = [
            entry for entry in result 
            if entry["data"] == [] and entry["units"] == ""
        ]
        assert len(missing_age_entries) == 2  # Two missing age combinations

        # Verify specific missing entries
        missing_entries_found = 0
        for entry in missing_age_entries:
            if (entry["name"] == "ModelA" and 
                entry["evidence_type"] == "TypeA" and 
                entry["tissue"] == "Tissuea" and 
                entry["age"] == "2 months"):
                missing_entries_found += 1
            elif (entry["name"] == "ModelB" and 
                  entry["evidence_type"] == "TypeA" and 
                  entry["tissue"] == "Tissueb" and 
                  entry["age"] == "2 months"):
                missing_entries_found += 1
            elif (entry["name"] == "ModelB" and 
                  entry["evidence_type"] == "TypeA" and 
                  entry["tissue"] == "Tissueb" and 
                  entry["age"] == "3 months"):
                missing_entries_found += 1

        assert missing_entries_found == 2
