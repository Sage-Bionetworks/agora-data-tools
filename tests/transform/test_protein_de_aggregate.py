"""Tests for transform_protein_de_aggregate."""

import json
import math
import os
from typing import Any

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_aggregate import (
    transform_protein_de_aggregate,
)

ASSETS = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "test_assets",
    "protein_de_aggregate",
)
DEFAULT_DE_FILE = "jax_load2_f_4mo"


def _default_label_map() -> dict[str, list]:
    return {
        "model": ["LOAD2", "LOAD2"],
        "model_group": ["LOAD2", "LOAD2"],
        "display_label": ["LOAD2", "LOAD1"],
        "genotype": ["geno_hom", "geno_wt"],
        "result_order": [2, 1],
        "model_type": ["Late Onset AD", "Late Onset AD"],
    }


def _build_datasets(
    data_files: dict[str, dict[str, Any]] | None = None,
    label_map: dict[str, Any] | None = None,
    gene_metadata: pd.DataFrame | None = None,
    mapping: pd.DataFrame | None = None,
    biodom: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    datasets: dict[str, pd.DataFrame] = {
        "genotype_label_map": pd.DataFrame(label_map or _default_label_map()),
        "mouse_gene_metadata": (
            gene_metadata
            if gene_metadata is not None
            else pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": ["Gnai3"],
                    "alias": [[]],
                }
            )
        ),
        "biodom_genes_mm": (
            biodom
            if biodom is not None
            else pd.DataFrame(
                {
                    "biodomain": ["Apoptosis"],
                    "abbr": ["Apo"],
                    "label": ["Apoptosis"],
                    "color": ["#000"],
                    "go_id": ["GO:1"],
                    "goterm_name": ["apoptosis"],
                    "n_symbol": [1],
                    "symbol": ["Gnai3"],
                    "ensembl_id": ["ENSMUSG00000000001"],
                }
            )
        ),
        "uniprot_ensembl_map": (
            mapping
            if mapping is not None
            else pd.DataFrame(
                {
                    "uniprot_id": ["P27144", "Q8C8R3"],
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000001",
                    ],
                }
            )
        ),
    }
    if data_files is None:
        data_files = {
            DEFAULT_DE_FILE: {
                "protein_id": ["Gnai3|P27144"],
                "diff": [0.5],
                "padj": [0.01],
            }
        }
    for name, columns in data_files.items():
        datasets[name] = pd.DataFrame(columns)
    return datasets


def _load_shared_inputs() -> dict[str, pd.DataFrame]:
    input_path = os.path.join(ASSETS, "input")
    return {
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


def _de_only() -> dict[str, Any]:
    return {
        "protein_id": ["Gnai3|P27144"],
        "diff": [0.5],
        "padj": [0.01],
    }


class TestTransformProteinDeAggregate:
    def test_biology_comes_from_the_dataset_name(self) -> None:
        datasets = _load_shared_inputs()
        input_path = os.path.join(ASSETS, "input")
        datasets["jax_load2_f_4mo"] = pd.read_csv(
            os.path.join(input_path, "overlay_f4.csv")
        )
        datasets["jax_load2_f_12mo"] = pd.read_csv(
            os.path.join(input_path, "overlay_f12.csv")
        )
        with open(os.path.join(ASSETS, "output", "overlay_output.json")) as handle:
            expected = json.load(handle)

        output = transform_protein_de_aggregate(datasets)

        assert output == expected
        assert "12 months" not in output[1]

    def test_one_gene_many_proteins(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Gnai3|P27144", "Gnai3|Q8C8R3"],
                    "diff": [0.1, 0.2],
                    "padj": [0.01, 0.02],
                }
            }
        )

        output = transform_protein_de_aggregate(datasets)

        assert [entry["unique_id"] for entry in output] == [
            "ENSMUSG00000000001P27144",
            "ENSMUSG00000000001Q8C8R3",
        ]

    def test_lowest_mouse_ensembl_id_ignores_header_symbol(self) -> None:
        """An accession with several mouse genes takes the smallest id, not the header symbol."""
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Cycs|P27144"],
                    "diff": [0.5],
                    "padj": [0.01],
                }
            },
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000009",
                    ],
                    "gene_symbol": ["Gm10053", "Cycs"],
                }
            ),
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["P27144", "P27144", "P27144"],
                    "ensembl_gene_id": [
                        "ENSG00000000001",
                        "ENSMUSG00000000009",
                        "ENSMUSG00000000001",
                    ],
                }
            ),
        )

        entry = transform_protein_de_aggregate(datasets)[0]

        assert entry["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert entry["gene_symbol"] == "Gm10053"
        assert entry["unique_id"] == "ENSMUSG00000000001P27144"

    def test_hyphenated_accession_stays_a_separate_entry(self) -> None:
        """An isoform maps on its base accession but stays a separate output entry."""
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Ank2|Q8C8R3", "Ank2|Q8C8R3-2"],
                    "diff": [0.1, 0.2],
                    "padj": [0.01, 0.02],
                }
            },
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["Q8C8R3"],
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                }
            ),
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": ["Gnai3"],
                }
            ),
        )

        by_uniprot = {
            entry["uniprotid"]: entry
            for entry in transform_protein_de_aggregate(datasets)
        }

        assert set(by_uniprot) == {"Q8C8R3", "Q8C8R3-2"}
        isoform = by_uniprot["Q8C8R3-2"]
        assert isoform["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert isoform["unique_id"] == "ENSMUSG00000000001Q8C8R3-2"
        assert isoform["display_symbol"] == "Gnai3 (Q8C8R3-2)"

    def test_display_symbol_falls_back_to_ensembl(self) -> None:
        datasets = _build_datasets(
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": [""],
                    "alias": [[]],
                }
            )
        )

        output = transform_protein_de_aggregate(datasets)

        assert output[0]["display_symbol"] == "ENSMUSG00000000001 (P27144)"

    def test_na_padj_becomes_one_and_negative_zero_is_normalized(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Gnai3|P27144"],
                    "diff": [-0.0],
                    "padj": [float("nan")],
                }
            }
        )

        age = transform_protein_de_aggregate(datasets)[0]["4 months"]

        assert age["adj_p_val"] == 1.0
        assert age["log2_fc"] == 0.0
        assert not math.copysign(1.0, age["log2_fc"]) < 0


class TestFilenameTokens:
    def test_too_few_filename_tokens_raises(self) -> None:
        datasets = _build_datasets(data_files={"de_file": _de_only()})

        with pytest.raises(ValueError, match="does not match"):
            transform_protein_de_aggregate(datasets)

    def test_unknown_sex_token_raises(self) -> None:
        datasets = _build_datasets(data_files={"jax_load2_x_4mo": _de_only()})

        with pytest.raises(ValueError, match="unrecognized sex token"):
            transform_protein_de_aggregate(datasets)

    def test_bad_age_token_raises(self) -> None:
        datasets = _build_datasets(data_files={"jax_load2_f_4weeks": _de_only()})

        with pytest.raises(ValueError, match="unrecognized age token"):
            transform_protein_de_aggregate(datasets)

    def test_unrecognized_center_raises(self) -> None:
        datasets = _build_datasets(data_files={"uci_load2_f_4mo": _de_only()})

        with pytest.raises(ValueError, match="Unrecognized center"):
            transform_protein_de_aggregate(datasets)

    def test_unknown_model_token_raises(self) -> None:
        datasets = _build_datasets(data_files={"jax_unknown_f_4mo": _de_only()})

        with pytest.raises(ValueError, match="not in the genotype label map"):
            transform_protein_de_aggregate(datasets)


class TestDuplicateAges:
    def test_same_protein_and_age_from_two_files_raises(self) -> None:
        """Two dataset names that resolve to the same biology cannot both supply one age."""
        row = {
            "protein_id": ["Gnai3|P27144"],
            "diff": [0.1],
            "padj": [0.01],
        }
        datasets = _build_datasets(
            data_files={
                "jax_load2_f_4mo": row,
                "jax_LOAD2_f_4mo": {
                    "protein_id": ["Gnai3|P27144"],
                    "diff": [0.2],
                    "padj": [0.02],
                },
            }
        )

        with pytest.raises(ValueError, match="which fold change belongs"):
            transform_protein_de_aggregate(datasets)


class TestInputValidation:
    def test_empty_file_raises(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": pd.Series(dtype=str),
                    "diff": pd.Series(dtype=float),
                    "padj": pd.Series(dtype=float),
                }
            }
        )

        with pytest.raises(ValueError, match="empty"):
            transform_protein_de_aggregate(datasets)

    def test_missing_dataset_raises(self) -> None:
        datasets = _build_datasets()
        del datasets["mouse_gene_metadata"]

        with pytest.raises(ValueError, match="mouse_gene_metadata"):
            transform_protein_de_aggregate(datasets)

    def test_blank_mouse_ensembl_id_raises(self) -> None:
        """A blank ensembl_gene_id would become a live key in the gene-symbol lookup."""
        datasets = _build_datasets()
        datasets["mouse_gene_metadata"]["ensembl_gene_id"] = ""

        with pytest.raises(ValueError, match="not_empty"):
            transform_protein_de_aggregate(datasets)

    def test_missing_protein_id_column_raises(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "diff": [0.5],
                    "padj": [0.01],
                }
            }
        )

        with pytest.raises(ValueError, match="protein_id"):
            transform_protein_de_aggregate(datasets)

    def test_protein_id_without_pipe_raises(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["P27144"],
                    "diff": [0.5],
                    "padj": [0.01],
                }
            }
        )

        with pytest.raises(ValueError, match="gene_symbol\\|uniprotid"):
            transform_protein_de_aggregate(datasets)

    def test_negative_padj_raises(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Gnai3|P27144"],
                    "diff": [0.5],
                    "padj": [-0.01],
                }
            }
        )

        with pytest.raises(ValueError, match="Negative adjusted p-value"):
            transform_protein_de_aggregate(datasets)

    def test_missing_fold_change_raises(self) -> None:
        datasets = _build_datasets(
            data_files={
                DEFAULT_DE_FILE: {
                    "protein_id": ["Gnai3|P27144"],
                    "padj": [0.01],
                }
            }
        )

        with pytest.raises(ValueError, match="fold-change"):
            transform_protein_de_aggregate(datasets)
