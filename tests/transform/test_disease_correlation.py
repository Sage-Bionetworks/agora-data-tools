import pytest
import pandas as pd
import os
import json
from agoradatatools.etl.transform.disease_correlation import (
    transform_disease_correlation,
    create_lookup,
    extract_module_name,
    process_group,
    input_validation_model_info,
)


class TestDiseaseCorrelationAssets:
    data_files_path = "tests/test_assets/disease_correlation"

    pass_test_data = [
        (
            {
                "disease_correlation_results": "disease_correlation_results.csv",
                "allele_info": "model_allele_info.csv",
                "model_info": "model_info.csv",
            },
            "disease_correlation_expected_output.json",
        )
    ]

    pass_test_ids = [
        "Test assets should pass",
    ]

    @pytest.mark.parametrize(
        "input_files, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_disease_correlation_transform_assets_should_pass(
        self, input_files, expected_output_file
    ):
        # Create datasets dictionary
        datasets = {}
        for dataset_name, file_name in input_files.items():
            datasets[dataset_name] = pd.read_csv(
                os.path.join(self.data_files_path, "input", file_name)
            )

        # Transform data
        output_data = transform_disease_correlation(datasets=datasets)

        # Load expected output
        with open(
            os.path.join(self.data_files_path, "output", expected_output_file)
        ) as f:
            expected_data = json.load(f)

        # Compare output with expected
        assert output_data == expected_data


class TestTransformDiseaseCorrelation:
    pass_test_data = [
        # Basic valid input
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                        {
                            "cluster": "Cluster A",
                            "module": "PHGbrown",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.6",
                            "adjusted_p_value": "0.02",
                        },
                        {
                            "cluster": "Cluster B",
                            "module": "TCXturquoise",
                            "mouse_model": "LOAD2",
                            "sex": "Male",
                            "age": "6 months",
                            "correlation": "0.7",
                            "adjusted_p_value": "0.03",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "name": "LOAD2",
                            "matched_controls": "C57BL6J",
                            "model_type": "Early Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                        {"name": "LOAD1", "gene": "TREM2"},
                        {"name": "LOAD2", "gene": "APP"},
                    ]
                ),
            },
            lambda output: (
                isinstance(output, list)
                and len(output) == 2
                and any(
                    entry["name"] == "LOAD1"
                    and entry["matched_control"] == "C57BL6J"
                    and entry["model_type"] == "Late Onset AD"
                    and entry["modified_genes"] == ["APOE4", "TREM2"]
                    and entry["cluster"] == "Cluster A"
                    and entry["age"] == "4 months"
                    and entry["sex"] == "Female"
                    and len(entry["IFG"]) == 2
                    and len(entry["PHG"]) == 2
                    and isinstance(entry["IFG"]["correlation"], float)
                    and isinstance(entry["PHG"]["adj_p_val"], float)
                    for entry in output
                )
            ),
        ),
        # Duplicate allele_info
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            lambda output: output[0]["modified_genes"] == "APOE4",
        ),
    ]
    pass_test_ids = [
        "Basic valid input should pass",
        "Duplicate allele_info includes all genes should pass",
    ]

    @pytest.mark.parametrize(
        "datasets, assertion_fn", pass_test_data, ids=pass_test_ids
    )
    def test_transform_disease_correlation_should_pass(self, datasets, assertion_fn):
        output = transform_disease_correlation(datasets)
        assert assertion_fn(output)

    dataset_error_test_data = [
        # Missing model_info
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Missing required datasets: model_info",
        ),
        # Duplicate results in disease_correlation_results
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Module IFG already exists for LOAD1",
        ),
        # Inconsistent model_info
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "age": "4 months",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "name": "LOAD1",
                            "matched_controls": "CTRL2",
                            "model_type": "Wrong",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Model LOAD1 has inconsistent matched_controls values:",
        ),
    ]

    column_error_test_data = [
        # Missing required column in disease_correlation_results
        (
            {
                "disease_correlation_results": pd.DataFrame(
                    [
                        {
                            "cluster": "Cluster A",
                            "module": "IFGyellow",
                            "mouse_model": "LOAD1",
                            "sex": "Female",
                            "correlation": "0.5",
                            "adjusted_p_value": "0.01",
                        },
                    ]
                ),
                "model_info": pd.DataFrame(
                    [
                        {
                            "name": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"name": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Missing required columns in disease_correlation_results dataset: age",
        ),
    ]

    dataset_error_test_ids = [
        "Missing model_info",
        "Duplicate results in disease_correlation_results",
        "Inconsistent model_info",
    ]
    column_error_test_ids = ["Missing required column in disease_correlation_results"]

    @pytest.mark.parametrize(
        "datasets, error_type, error_msg",
        dataset_error_test_data,
        ids=dataset_error_test_ids,
    )
    def test_transform_disease_correlation_missing_dataset(
        self, datasets, error_type, error_msg
    ):
        with pytest.raises(error_type, match=error_msg):
            transform_disease_correlation(datasets)

    @pytest.mark.parametrize(
        "datasets, error_type, error_msg",
        column_error_test_data,
        ids=column_error_test_ids,
    )
    def test_transform_disease_correlation_missing_column(
        self, datasets, error_type, error_msg
    ):
        with pytest.raises(error_type, match=error_msg):
            transform_disease_correlation(datasets)


class TestCreateLookup:
    def test_create_lookup(self):
        input_dataframe = pd.DataFrame(
            [
                {"A": "a1", "B": "b1", "C": "c1"},
                {"A": "a1", "B": "b2", "C": "c1"},
                {"A": "a1", "B": "b3", "C": "c1"},
                {"A": "a2", "B": "b4", "C": "c2"},
            ]
        )
        group_by_col = "A"
        expected_output = {
            "a1": {"B": ["b1", "b2", "b3"], "C": "c1"},
            "a2": {"B": "b4", "C": "c2"},
        }
        output = create_lookup(df=input_dataframe, group_by_col=group_by_col)
        assert output == expected_output


class TestExtractModuleName:
    @pytest.mark.parametrize(
        "input_module,expected",
        [
            ("IFGyellow", "IFG"),
            ("PHGbrown", "PHG"),
            ("TCXturquoise", "TCX"),
            ("IFG", "IFG"),  # No color suffix
            ("", ""),  # Empty string
            ("123ABC", "123ABC"),  # No match for regex
        ],
    )
    def test_extract_module_name(self, input_module, expected):
        assert extract_module_name(input_module) == expected


class TestProcessGroup:
    def test_process_group_with_valid_data(self):
        # Create test data
        group = pd.DataFrame(
            [
                {
                    "module": "IFGyellow",
                    "correlation": "0.5",
                    "adjusted_p_value": "0.01",
                },
                {
                    "module": "PHGbrown",
                    "correlation": "0.6",
                    "adjusted_p_value": "0.02",
                },
            ]
        )

        model_info = {"matched_controls": "C57BL6J", "model_type": "Late Onset AD"}

        allele_info = {"gene": ["APOE4", "TREM2"]}

        result = process_group(
            group=group,
            model_info=model_info,
            allele_info=allele_info,
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        assert result == {
            "name": "LOAD1",
            "matched_control": "C57BL6J",
            "model_type": "Late Onset AD",
            "modified_genes": ["APOE4", "TREM2"],
            "cluster": "Cluster A",
            "age": "4 months",
            "sex": "Female",
            "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
            "PHG": {"correlation": 0.6, "adj_p_val": 0.02},
        }

    def test_process_group_with_empty_model_info(self):

        # Create test data
        group = pd.DataFrame(
            [{"module": "IFGyellow", "correlation": "0.5", "adjusted_p_value": "0.01"}]
        )

        result = process_group(
            group=group,
            model_info={},
            allele_info={},
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        assert result == {
            "name": "LOAD1",
            "matched_control": "",
            "model_type": "",
            "modified_genes": "",
            "cluster": "Cluster A",
            "age": "4 months",
            "sex": "Female",
            "IFG": {"correlation": 0.5, "adj_p_val": 0.01},
        }

    def test_process_group_with_list_matched_controls(self):

        # Create test data
        group = pd.DataFrame(
            [{"module": "IFGyellow", "correlation": "0.5", "adjusted_p_value": "0.01"}]
        )

        model_info = {
            "matched_controls": ["C57BL6J", "CTRL2"],
            "model_type": "Late Onset AD",
        }

        result = process_group(
            group=group,
            model_info=model_info,
            allele_info={},
            name="LOAD1",
            cluster="Cluster A",
            age="4 months",
            sex="Female",
        )

        assert (
            result["matched_control"] == "C57BL6J"
        )  # Should take first element from list


class TestInputValidationModelInfo:
    def test_valid_model_info(self):
        """Test that valid model info passes validation."""
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD2",
                    "matched_controls": "C57BL6J",
                    "model_type": "Early Onset AD",
                },
            ]
        )
        # Should not raise any exception
        input_validation_model_info(df)

    def test_inconsistent_matched_controls(self):
        """Test that inconsistent matched_controls values raise ValueError."""
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD1",
                    "matched_controls": "CTRL2",
                    "model_type": "Late Onset AD",
                },
            ]
        )
        with pytest.raises(
            ValueError, match="Model LOAD1 has inconsistent matched_controls values:"
        ):
            input_validation_model_info(df)

    def test_inconsistent_model_type(self):
        """Test that inconsistent model_type values raise ValueError."""
        df = pd.DataFrame(
            [
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Late Onset AD",
                },
                {
                    "name": "LOAD1",
                    "matched_controls": "C57BL6J",
                    "model_type": "Early Onset AD",
                },
            ]
        )
        with pytest.raises(
            ValueError, match="Model LOAD1 has inconsistent model_type values:"
        ):
            input_validation_model_info(df)

    def test_empty_dataframe(self):
        """Test that empty dataframe passes validation."""
        df = pd.DataFrame(columns=["name", "matched_controls", "model_type"])
        # Should not raise any exception
        input_validation_model_info(df)

    def test_single_row(self):
        """Test that single row dataframe passes validation."""
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
        input_validation_model_info(df)
