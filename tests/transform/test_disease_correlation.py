import pytest
import pandas as pd
from agoradatatools.etl.transform.disease_correlation import (
    transform_disease_correlation,
)


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
                            "model": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "model": "LOAD2",
                            "matched_controls": "C57BL6J",
                            "model_type": "Early Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"model": "LOAD1", "gene": "APOE4"},
                        {"model": "LOAD1", "gene": "TREM2"},
                        {"model": "LOAD2", "gene": "APP"},
                    ]
                ),
            },
            lambda output: (
                isinstance(output, list)
                and len(output) == 2
                and any(
                    entry["model"] == "LOAD1"
                    and entry["matched_control"] == "C57BL6J"
                    and entry["model_type"] == "Late Onset AD"
                    and set(entry["modified_genes"]) == {"APOE4", "TREM2"}
                    and entry["cluster"] == "Cluster A"
                    and entry["age"] == "4 months"
                    and entry["sex"] == "Female"
                    and len(entry["results"]) == 2
                    and entry["results"][0]["module"] == "IFG"
                    and entry["results"][1]["module"] == "PHG"
                    and isinstance(entry["results"][0]["correlation"], float)
                    and isinstance(entry["results"][0]["adj_p_val"], float)
                    for entry in output
                )
            ),
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
                            "model": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"model": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            lambda output: (
                len(output) == 1
                and len(output[0]["results"]) == 2
                and output[0]["results"][0] == output[0]["results"][1]
            ),
        ),
        # Duplicate model_info, last one should be used
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
                            "model": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                        {
                            "model": "LOAD1",
                            "matched_controls": "CTRL2",
                            "model_type": "Override",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"model": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            lambda output: (
                output[0]["matched_control"] == "C57BL6J"
                and output[0]["model_type"] == "Late Onset AD"
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
                            "model": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"model": "LOAD1", "gene": "APOE4"},
                        {"model": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            lambda output: output[0]["modified_genes"] == ["APOE4", "APOE4"],
        ),
    ]
    pass_test_ids = [
        "Basic valid input should pass",
        "Duplicate results in disease_correlation_results should pass",
        "Duplicate model_info uses first row should pass",
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
                        {"model": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Missing required datasets: model_info",
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
                            "model": "LOAD1",
                            "matched_controls": "C57BL6J",
                            "model_type": "Late Onset AD",
                        },
                    ]
                ),
                "allele_info": pd.DataFrame(
                    [
                        {"model": "LOAD1", "gene": "APOE4"},
                    ]
                ),
            },
            ValueError,
            "Missing required columns in disease_correlation_results dataset: age",
        ),
    ]

    dataset_error_test_ids = ["Missing model_info"]
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
