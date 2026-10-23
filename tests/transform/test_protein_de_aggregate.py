"""Tests for transform_protein_de_aggregate."""
from typing import Any
import json
import os

import pandas as pd
import numpy as np
import pytest

from agoradatatools.etl.transform.protein_de_aggregate import (
    REQUIRED_INPUT,
    DATA_FILE_COLUMNS,
    _model_display_labels,
    _validate_file_metadata,
    transform_protein_de_aggregate,
)

ASSETS = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "test_assets",
    "protein_de_aggregate",
)
KNOWN_MODELS = {"LOAD2"}
FIXTURE_FILES = {
    "genotype_label_map": "genotype_label_map.csv",
    "mouse_gene_metadata": "mouse_gene_metadata.json",
    "biodom_genes_mm": "biodom_genes_mm.csv",
    "uniprot_ensembl_map": "uniprot_ensembl_map.csv",
    "jax_load2_f_4mo": "overlay_f4.csv",
    "jax_load2_f_12mo": "overlay_f12.csv",
    "jax_load2_m_4mo": "overlay_m4.csv",
}
FILE_METADATA = {
    "jax_load2_f_4mo": {
        "model": "LOAD2",
        "sex": "Female",
        "age": "4 months",
        "tissue": "Hemibrain",
    },
    "jax_load2_f_12mo": {
        "model": "LOAD2",
        "sex": "Female",
        "age": "12 months",
        "tissue": "Hemibrain",
    },
    "jax_load2_m_4mo": {
        "model": "LOAD2",
        "sex": "Male",
        "age": "4 months",
        "tissue": "Hemibrain",
    },
}


def _create_metadata(
    model: str = "LOAD2",
    sex: str = "Female",
    age: str = "4 months",
    tissue: str = "Hemibrain",
) -> dict[str, str]:
    return {"model": model, "sex": sex, "age": age, "tissue": tissue}


def _create_datasets_with(*data_files: str) -> dict[str, pd.DataFrame]:
    """Create a 'datasets' dictionary with empty DataFrames for all required inputs, and add single-row data frames with
    a protein_id column for any number of specified data files."""

    datasets = {name: pd.DataFrame() for name in REQUIRED_INPUT}
    for name in data_files:
        datasets[name] = pd.DataFrame({"protein_id": ["Gnai3|P27144"]})
    return datasets


def _create_label_map(*rows: tuple[str, str, int]) -> pd.DataFrame:
    """Create a genotype label map DataFrame from any number of (model, display_label, result_order) values."""
    return pd.DataFrame(rows, columns=["model", "display_label", "result_order"])


def _load_datasets() -> dict[str, pd.DataFrame]:
    input_path = os.path.join(ASSETS, "input")

    datasets = {}
    for name, filename in FIXTURE_FILES.items():
        if filename.endswith(".csv"):
            datasets[name] = pd.read_csv(os.path.join(input_path, filename))
        else:
            datasets[name] = pd.read_json(os.path.join(input_path, filename))
    return datasets


class TestModelDisplayLabels:
    def test_lowest_result_order_is_the_matched_control(self) -> None:
        """name is the highest result_order and matched_control is the lowest."""
        label_map = _create_label_map(("LOAD2", "LOAD2", 2), ("LOAD2", "LOAD1", 1))

        assert _model_display_labels(label_map, {"LOAD2"}) == {
            "LOAD2": {"matched_control": "LOAD1", "name": "LOAD2"}
        }

    @pytest.mark.parametrize("n_labels", [1, 3])
    def test_a_used_model_without_exactly_two_labels_raises(
        self, n_labels: int
    ) -> None:
        """There is no case or control column, so a third or missing label cannot be resolved."""
        rows = [("LOAD2", f"Label_{index}", index) for index in range(1, n_labels + 1)]

        with pytest.raises(ValueError, match="exactly two"):
            _model_display_labels(_create_label_map(*rows), {"LOAD2"})

    def test_a_model_that_is_not_used_is_ignored(self) -> None:
        """Normally a third genotype label for a model would raise an error. However, a third label on a model this
        dataset does not use does should not raise an error."""
        label_map = _create_label_map(
            ("LOAD2", "LOAD2", 2),
            ("LOAD2", "LOAD1", 1),
            ("Model_B", "Model_B", 3),
            ("Model_B", "Control_B", 2),
            ("Model_B", "Other_B", 1),
        )

        labels = _model_display_labels(label_map, {"LOAD2"})

        assert set(labels) == {"LOAD2"}


class TestValidateFileMetadata:
    def test_sex_and_tissue_are_normalized(self) -> None:
        """Plural sex and the hemisphere alias in the metadata are normalized:
        Females -> Female, Right Cerebral Hemisphere -> Hemibrain"""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        file_metadata = {
            "jax_load2_f_4mo": _create_metadata(
                sex="Females", tissue="Right Cerebral Hemisphere"
            )
        }

        normalized = _validate_file_metadata(
            datasets, file_metadata, REQUIRED_INPUT, KNOWN_MODELS
        )

        assert normalized["jax_load2_f_4mo"]["sex"] == "Female"
        assert normalized["jax_load2_f_4mo"]["tissue"] == "Hemibrain"

    def test_a_data_file_missing_from_the_map_raises(self) -> None:
        """Every differential expression file has to have a file_metadata entry."""
        datasets = _create_datasets_with("jax_load2_f_4mo", "jax_load2_f_12mo")

        # Missing jax_load2_f_12mo
        metadata = {"jax_load2_f_4mo": _create_metadata()}

        with pytest.raises(ValueError, match="not in file_metadata"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    def test_an_unknown_file_name_raises(self) -> None:
        """file_metadata cannot name a file that is not in the dataset."""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        metadata = {"not_a_file": _create_metadata()}

        with pytest.raises(ValueError, match="not files in this dataset"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    def test_a_metadata_dataset_in_the_map_raises(self) -> None:
        """Should raise an error if a required input file (not a differential expression file) is listed in the
        metadata map."""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        metadata = {
            "genotype_label_map": _create_metadata(),
            "jax_load2_f_4mo": _create_metadata(),
        }

        with pytest.raises(ValueError, match="metadata datasets"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    @pytest.mark.parametrize("field", ["model", "sex", "age", "tissue"])
    @pytest.mark.parametrize(
        "value",
        ["", " ", None, np.nan],
        ids=["empty string", "whitespace", "None", "NaN"],
    )
    def test_a_blank_field_raises(self, field: str, value: Any) -> None:
        """Each of model, sex, age, and tissue has to be filled in with a non-whitespace value."""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        metadata = {"jax_load2_f_4mo": _create_metadata(**{field: value})}

        with pytest.raises(ValueError, match=f"column '{field}'.*not_empty"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    def test_a_bad_age_raises(self) -> None:
        """Age has to be "N months", the same shape the RNA transforms require."""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        metadata = {"jax_load2_f_4mo": _create_metadata(age="4mo")}

        with pytest.raises(ValueError, match="column 'age'.*matches_regex"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    def test_an_unknown_model_raises(self) -> None:
        """The model has to be spelled the way the genotype label map spells it. The dataset is created with
        model = "LOAD2", while the metadata has model = "load2"."""
        datasets = _create_datasets_with("jax_load2_f_4mo")
        metadata = {"jax_load2_f_4mo": _create_metadata(model="load2")}

        with pytest.raises(ValueError, match="column 'model'.*one_of"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)

    def test_two_files_in_one_group_raise(self) -> None:
        """Two files for one model, tissue, sex, and age would both write the same age."""
        datasets = _create_datasets_with("jax_load2_f_4mo", "jax_load2_f_4mo_copy")
        metadata = {
            "jax_load2_f_4mo": _create_metadata(),
            # "Females" will map to "Female" before validation, causing both files to map to the same group.
            "jax_load2_f_4mo_copy": _create_metadata(sex="Females"),
        }

        with pytest.raises(ValueError, match="same model, tissue, sex, and age"):
            _validate_file_metadata(datasets, metadata, REQUIRED_INPUT, KNOWN_MODELS)


class TestTransformProteinDeAggregate:
    def test_ages_from_two_files_merge_into_one_entry(self) -> None:
        """Ages in different files for the same protein, tissue, and sex are merged into one entry. The datasets in
        this test have 4 month data in one file and 12 month data in another, but 12 month data only exists for
        females. The output for males should only have the 4 month data, while females should have both 4 and 12 month
        data."""
        with open(os.path.join(ASSETS, "output", "overlay_output.json")) as handle:
            expected = json.load(handle)

        output = transform_protein_de_aggregate(_load_datasets(), FILE_METADATA)

        assert output == expected
        female = output[1]
        male = output[2]
        assert "4 months" in female
        assert "12 months" in female
        assert "12 months" not in male

    def test_two_accessions_for_one_gene_are_separate_entries(self) -> None:
        """Two accessions of one gene stay two entries, ordered by unique_id."""
        output = transform_protein_de_aggregate(_load_datasets(), FILE_METADATA)

        gnai3 = [
            (entry["unique_id"], entry["sex"])
            for entry in output
            if entry["gene_symbol"] == "Gnai3"
        ]
        assert gnai3 == [
            ("ENSMUSG00000000001P00001", "Female"),
            ("ENSMUSG00000000001P27144", "Female"),
            ("ENSMUSG00000000001P27144", "Male"),
        ]

    def test_missing_dataset_raises(self) -> None:
        """A missing required metadata dataset raises before any file is read."""
        datasets = _load_datasets()
        del datasets["mouse_gene_metadata"]

        with pytest.raises(ValueError, match="mouse_gene_metadata"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)

    def test_empty_file_raises(self) -> None:
        """An empty differential expression file raises."""
        datasets = _load_datasets()
        datasets["jax_load2_f_4mo"] = pd.DataFrame(
            columns=["protein_id", "log2foldchange", "padj"]
        )

        with pytest.raises(ValueError, match="empty"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)

    def test_protein_id_without_a_pipe_raises(self) -> None:
        """protein_id has to be gene_symbol|uniprotid, so a bare accession raises."""
        datasets = _load_datasets()
        datasets["jax_load2_f_4mo"].loc[0, "protein_id"] = "P27144"

        with pytest.raises(ValueError, match="protein_id.*contains_substring"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)

    def test_blank_mouse_ensembl_id_raises(self) -> None:
        """A blank ensembl_gene_id would become a live key in the gene-symbol lookup."""
        datasets = _load_datasets()
        datasets["mouse_gene_metadata"]["ensembl_gene_id"] = ""

        with pytest.raises(ValueError, match="not_empty"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)

    @pytest.mark.parametrize("missing_column", DATA_FILE_COLUMNS)
    def test_missing_column_in_data_file_raises(self, missing_column: str) -> None:
        """A missing required column in the data file raises."""
        datasets = _load_datasets()
        datasets["jax_load2_f_4mo"] = datasets["jax_load2_f_4mo"].drop(
            columns=[missing_column]
        )

        with pytest.raises(ValueError, match=missing_column):
            transform_protein_de_aggregate(datasets, FILE_METADATA)
