"""Tests for transform_protein_de_aggregate."""

import json
import os

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_aggregate import (
    REQUIRED_INPUT,
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


def _meta(
    model: str = "LOAD2",
    sex: str = "Female",
    age: str = "4 months",
    tissue: str = "Hemibrain",
) -> dict[str, str]:
    return {"model": model, "sex": sex, "age": age, "tissue": tissue}


def _datasets_with(*data_files: str) -> dict[str, pd.DataFrame]:
    datasets = {name: pd.DataFrame() for name in REQUIRED_INPUT}
    for name in data_files:
        datasets[name] = pd.DataFrame({"protein_id": ["Gnai3|P27144"]})
    return datasets


def _label_rows(*rows: tuple[str, str, int]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["model", "display_label", "result_order"])


def _load_datasets() -> dict[str, pd.DataFrame]:
    input_path = os.path.join(ASSETS, "input")
    datasets = {
        "genotype_label_map": pd.read_csv(
            os.path.join(input_path, "genotype_label_map.csv")
        ),
        "mouse_gene_metadata": pd.read_json(
            os.path.join(input_path, "mouse_gene_metadata.json")
        ),
        "biodom_genes_mm": pd.read_csv(os.path.join(input_path, "biodom_genes_mm.csv")),
        "uniprot_ensembl_map": pd.read_csv(
            os.path.join(input_path, "uniprot_ensembl_map.csv")
        ),
    }
    for name, filename in FIXTURE_FILES.items():
        datasets[name] = pd.read_csv(os.path.join(input_path, filename))
    return datasets


class TestModelDisplayLabels:
    def test_lowest_result_order_is_the_matched_control(self) -> None:
        """name is the highest result_order and matched_control is the lowest."""
        label_map = _label_rows(("LOAD2", "LOAD2", 2), ("LOAD2", "LOAD1", 1))

        assert _model_display_labels(label_map, {"LOAD2"}) == {
            "LOAD2": {"matched_control": "LOAD1", "name": "LOAD2"}
        }

    def test_a_model_that_is_not_used_is_ignored(self) -> None:
        """A third label on a model this dataset does not use does not raise."""
        label_map = _label_rows(
            ("LOAD2", "LOAD2", 2),
            ("LOAD2", "LOAD1", 1),
            ("Model_B", "Model_B", 3),
            ("Model_B", "Control_B", 2),
            ("Model_B", "Other_B", 1),
        )

        labels = _model_display_labels(label_map, {"LOAD2"})

        assert set(labels) == {"LOAD2"}

    @pytest.mark.parametrize("n_labels", [1, 3])
    def test_a_used_model_without_exactly_two_labels_raises(
        self, n_labels: int
    ) -> None:
        """There is no case or control column, so a third or missing label cannot be resolved."""
        rows = [("LOAD2", f"Label_{index}", index) for index in range(1, n_labels + 1)]

        with pytest.raises(ValueError, match="exactly two"):
            _model_display_labels(_label_rows(*rows), {"LOAD2"})


class TestValidateFileMetadata:
    def test_sex_and_tissue_are_normalized_once(self) -> None:
        """Plural sex and the hemisphere alias are normalized on the metadata, not per row."""
        datasets = _datasets_with("jax_load2_f_4mo")
        file_metadata = {
            "jax_load2_f_4mo": _meta(sex="Females", tissue="Right Cerebral Hemisphere")
        }

        normalized = _validate_file_metadata(
            datasets, file_metadata, REQUIRED_INPUT, KNOWN_MODELS
        )

        assert normalized["jax_load2_f_4mo"]["sex"] == "Female"
        assert normalized["jax_load2_f_4mo"]["tissue"] == "Hemibrain"

    def test_a_data_file_missing_from_the_map_raises(self) -> None:
        """Every differential expression file has to have a file_metadata entry."""
        datasets = _datasets_with("jax_load2_f_4mo", "jax_load2_f_12mo")

        with pytest.raises(ValueError, match="not in file_metadata"):
            _validate_file_metadata(
                datasets,
                {"jax_load2_f_4mo": _meta()},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_an_unknown_file_name_raises(self) -> None:
        """file_metadata cannot name a file that is not in the dataset."""
        datasets = _datasets_with("jax_load2_f_4mo")

        with pytest.raises(ValueError, match="not files in this dataset"):
            _validate_file_metadata(
                datasets,
                {"not_a_file": _meta()},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_a_metadata_dataset_in_the_map_raises(self) -> None:
        """A required metadata dataset is not a differential expression file."""
        datasets = _datasets_with("jax_load2_f_4mo")

        with pytest.raises(ValueError, match="metadata datasets"):
            _validate_file_metadata(
                datasets,
                {"genotype_label_map": _meta(), "jax_load2_f_4mo": _meta()},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_a_blank_field_raises(self) -> None:
        """Each of model, sex, age, and tissue has to be filled in."""
        datasets = _datasets_with("jax_load2_f_4mo")

        with pytest.raises(ValueError, match="column 'age'.*not_empty"):
            _validate_file_metadata(
                datasets,
                {"jax_load2_f_4mo": _meta(age="  ")},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_a_bad_age_raises(self) -> None:
        """Age has to be N months, the same shape the RNA transforms require."""
        datasets = _datasets_with("jax_load2_f_4mo")

        with pytest.raises(ValueError, match="column 'age'.*matches_regex"):
            _validate_file_metadata(
                datasets,
                {"jax_load2_f_4mo": _meta(age="4mo")},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_an_unknown_model_raises(self) -> None:
        """The model has to be spelled the way the genotype label map spells it."""
        datasets = _datasets_with("jax_load2_f_4mo")

        with pytest.raises(ValueError, match="column 'model'.*one_of"):
            _validate_file_metadata(
                datasets,
                {"jax_load2_f_4mo": _meta(model="load2")},
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )

    def test_two_files_in_one_group_raise(self) -> None:
        """Two files for one model, tissue, sex, and age would both write the same age."""
        datasets = _datasets_with("jax_load2_f_4mo", "jax_load2_f_4mo_copy")

        with pytest.raises(ValueError, match="same model, tissue, sex, and age"):
            _validate_file_metadata(
                datasets,
                {
                    "jax_load2_f_4mo": _meta(),
                    "jax_load2_f_4mo_copy": _meta(sex="Females"),
                },
                REQUIRED_INPUT,
                KNOWN_MODELS,
            )


class TestTransformProteinDeAggregate:
    def test_ages_from_two_files_merge_into_one_entry(self) -> None:
        """One protein, tissue, and sex collects an age from each file, and the sexes stay separate."""
        with open(os.path.join(ASSETS, "output", "overlay_output.json")) as handle:
            expected = json.load(handle)

        output = transform_protein_de_aggregate(_load_datasets(), FILE_METADATA)

        assert output == expected
        female = output[1]
        male = output[2]
        assert list(female)[-2:] == ["4 months", "12 months"]
        assert "12 months" not in male

    def test_two_accessions_for_one_gene_are_separate_entries(self) -> None:
        """Two accessions of one gene stay two entries, ordered by unique_id."""
        output = transform_protein_de_aggregate(_load_datasets(), FILE_METADATA)

        gnai3 = [
            entry["unique_id"] for entry in output if entry["gene_symbol"] == "Gnai3"
        ]
        assert gnai3 == [
            "ENSMUSG00000000001P00001",
            "ENSMUSG00000000001P27144",
            "ENSMUSG00000000001P27144",
        ]
        assert output[1]["sex"] == "Female"
        assert output[2]["sex"] == "Male"

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

    def test_missing_log2foldchange_raises(self) -> None:
        """log2foldchange is required. The pipeline renames diff before the transform runs."""
        datasets = _load_datasets()
        datasets["jax_load2_f_4mo"] = datasets["jax_load2_f_4mo"].drop(
            columns=["log2foldchange"]
        )

        with pytest.raises(ValueError, match="log2foldchange"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)

    def test_blank_mouse_ensembl_id_raises(self) -> None:
        """A blank ensembl_gene_id would become a live key in the gene-symbol lookup."""
        datasets = _load_datasets()
        datasets["mouse_gene_metadata"]["ensembl_gene_id"] = ""

        with pytest.raises(ValueError, match="not_empty"):
            transform_protein_de_aggregate(datasets, FILE_METADATA)
