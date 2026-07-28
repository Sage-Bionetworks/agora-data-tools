"""
Test suite for the individual proteomics transformation.

This module tests transform_protein_de_individual and its helpers, which reshape wide
Model AD proteomics files, join per-animal harmonized metadata, map proteins to mouse
Ensembl genes, and produce an RNA-style output plus the proteomics-specific fields
uniprotid, unique_id, and display_symbol.

Test Classes:
    - TestBuildUniprotToEnsembl: Unit tests for the UniProt to Ensembl lookup builder.
    - TestNormalizeTissue: Unit tests for the tissue normalization helper.
    - TestTransformProteinDeIndividual: Integration tests for the full transform.

The integration tests use synthetic fixtures in tests/test_assets/protein_de_individual/
for the happy-path golden comparison and inline DataFrames for targeted behaviors
(wildtype/heterozygous exclusion, UniProt isoform recovery, missing-metadata dropping,
unmapped/human-gene dropping, age bucketing, and error handling).
"""

import json
import os
from typing import Any, Dict, List

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_individual import (
    transform_protein_de_individual,
    _build_uniprot_to_ensembl,
    _normalize_tissue,
)


class TestBuildUniprotToEnsembl:
    """
    Unit tests for _build_uniprot_to_ensembl, which selects mouse genes and de-duplicates
    UniProt accessions that map to more than one Ensembl gene.
    """

    def test_filters_human_genes(self) -> None:
        """Test that accessions mapping only to human genes (ENSG*) are excluded."""
        mapping = pd.DataFrame(
            {
                "uniprotkb_accession": ["P1", "P2"],
                "resource_identifier": ["ENSMUSG00000000001", "ENSG00000000001"],
            }
        )

        result = _build_uniprot_to_ensembl(mapping)

        assert result == {"P1": "ENSMUSG00000000001"}

    def test_multi_mapped_accession_dedup_keeps_smallest(self) -> None:
        """Test that an accession mapping to several mouse genes keeps the smallest id.

        De-duplication is deterministic: the lexicographically smallest Ensembl id wins,
        so runs are reproducible.
        """
        mapping = pd.DataFrame(
            {
                "uniprotkb_accession": ["P1", "P1", "P3"],
                "resource_identifier": [
                    "ENSMUSG00000000005",
                    "ENSMUSG00000000002",
                    "ENSMUSG00000000003",
                ],
            }
        )

        result = _build_uniprot_to_ensembl(mapping)

        assert result == {
            "P1": "ENSMUSG00000000002",
            "P3": "ENSMUSG00000000003",
        }


class TestNormalizeTissue:
    """
    Unit tests for _normalize_tissue, which maps the JAX tissue name to Hemibrain and
    defaults missing values to Hemibrain.
    """

    def test_maps_right_cerebral_hemisphere_case_insensitive(self) -> None:
        """Test that the JAX tissue name is mapped regardless of case."""
        result = _normalize_tissue(
            pd.Series(["right cerebral hemisphere", "Right Cerebral Hemisphere"])
        )
        assert result.tolist() == ["Hemibrain", "Hemibrain"]

    def test_defaults_missing_and_empty_to_hemibrain(self) -> None:
        """Test that null and empty tissue values default to Hemibrain."""
        result = _normalize_tissue(pd.Series([None, ""]))
        assert result.tolist() == ["Hemibrain", "Hemibrain"]


class TestTransformProteinDeIndividual:
    """
    Integration tests for the full individual proteomics transformation.
    """

    data_files_path = "tests/test_assets/protein_de_individual"

    def _load_fixture_datasets(self, data_files: List[str]) -> Dict[str, pd.DataFrame]:
        """Load synthetic fixture CSVs as DataFrames keyed for the transform."""
        input_path = os.path.join(self.data_files_path, "input")
        file_to_key_mapping = {
            "synthetic_genotype_label_map.csv": "genotype_label_map",
            "synthetic_mouse_gene_metadata.csv": "mouse_gene_metadata",
            "synthetic_harmonized_metadata.csv": "load2_harmonized_metadata",
            "synthetic_uniprot_ensembl_map.csv": "uniprot_ensembl_map",
        }
        datasets = {}
        for file_name in data_files:
            df = pd.read_csv(os.path.join(input_path, file_name))
            key = file_to_key_mapping.get(file_name, file_name.replace(".csv", ""))
            datasets[key] = df
        return datasets

    @staticmethod
    def _normalize(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort inner data lists and the outer list for order-independent comparison."""
        for entry in entries:
            entry["data"] = sorted(entry["data"], key=lambda x: x["individual_id"])
        return sorted(entries, key=lambda x: (x["unique_id"], x["age"]))

    def _build_datasets(
        self,
        harmonized_overrides: Dict[str, Any] = None,
        data_overrides: Dict[str, Any] = None,
        data_key: str = "proteomics_file",
        label_map_overrides: Dict[str, Any] = None,
        mapping: pd.DataFrame = None,
        gene_metadata: pd.DataFrame = None,
    ) -> Dict[str, pd.DataFrame]:
        """Build a minimal valid datasets dict, allowing targeted overrides.

        Defaults: one homozygous (LOAD2) and one hAPP-WT (LOAD1 control) animal, both at
        4 months, and a single wide protein column Gene1|P00001 mapping to a mouse gene.
        """
        label_map = {
            "model": ["LOAD2", "LOAD2"],
            "model_group": ["LOAD2", "LOAD2"],
            "display_label": ["LOAD2", "LOAD1"],
            "genotype": ["geno_hom", "geno_wt"],
            "result_order": [2, 1],
        }
        if label_map_overrides:
            label_map.update(label_map_overrides)

        harmonized = {
            "individualid": ["i1", "i2"],
            "sex": ["male", "female"],
            "agedeath": [4.0, 4.5],
            "genotype": ["geno_hom", "geno_wt"],
            "tissue": ["right cerebral hemisphere", "right cerebral hemisphere"],
        }
        if harmonized_overrides:
            harmonized.update(harmonized_overrides)

        data_file = {
            "specimenid": ["c1", "c2"],
            "individualid": ["i1", "i2"],
            "Gene1|P00001": [1.0, 2.0],
        }
        if data_overrides:
            data_file = data_overrides

        if mapping is None:
            mapping = pd.DataFrame(
                {
                    "uniprotkb_accession": ["P00001", "P00002", "Q00003"],
                    "resource_identifier": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000002",
                        "ENSMUSG00000000003",
                    ],
                }
            )
        if gene_metadata is None:
            gene_metadata = pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000002",
                        "ENSMUSG00000000003",
                    ],
                    "gene_symbol": ["Gnai3", "Cdc45", "Xpo6"],
                }
            )

        return {
            "genotype_label_map": pd.DataFrame(label_map),
            "mouse_gene_metadata": gene_metadata,
            "load2_harmonized_metadata": pd.DataFrame(harmonized),
            "uniprot_ensembl_map": mapping,
            data_key: pd.DataFrame(data_file),
        }

    def test_synthetic_basic_data(self) -> None:
        """Test the happy path against the golden fixture output.

        Covers wide-to-long melt, harmonized join, UniProt to Ensembl mapping, genotype
        label mapping, tissue mapping, sex title-casing, unique_id/display_symbol, missing
        gene metadata (empty gene_symbol), and wildtype exclusion (i3 is dropped).
        """
        datasets = self._load_fixture_datasets(
            [
                "synthetic_basic_data.csv",
                "synthetic_genotype_label_map.csv",
                "synthetic_mouse_gene_metadata.csv",
                "synthetic_harmonized_metadata.csv",
                "synthetic_uniprot_ensembl_map.csv",
            ]
        )

        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected = json.load(f)

        output = transform_protein_de_individual(datasets=datasets)

        assert self._normalize(output) == self._normalize(expected)

    def test_wildtype_and_heterozygous_excluded(self) -> None:
        """Test that animals whose genotype is absent from the label map are dropped.

        Full-wildtype and heterozygous animals have genotypes with no label-map row, so
        their individuals must not appear in any output entry.
        """
        datasets = self._build_datasets(
            harmonized_overrides={
                "individualid": ["i1", "i2", "i3", "i4"],
                "sex": ["male", "female", "male", "female"],
                "agedeath": [4.0, 4.5, 5.0, 5.1],
                "genotype": ["geno_hom", "geno_wt", "geno_fullwt", "geno_het"],
                "tissue": ["right cerebral hemisphere"] * 4,
            },
            data_overrides={
                "specimenid": ["c1", "c2", "c3", "c4"],
                "individualid": ["i1", "i2", "i3", "i4"],
                "Gene1|P00001": [1.0, 2.0, 3.0, 4.0],
            },
        )

        output = transform_protein_de_individual(datasets=datasets)

        assert len(output) == 1
        individuals = {d["individual_id"] for d in output[0]["data"]}
        assert individuals == {"i1", "i2"}
        genotypes = {d["genotype"] for d in output[0]["data"]}
        assert genotypes == {"LOAD2", "LOAD1"}

    def test_uniprot_isoform_recovered_and_preserved(self) -> None:
        """Test that a pipeline-mangled isoform header is recovered and kept distinct.

        The pipeline lowercases headers and turns isoform hyphens into underscores. The
        transform must recover the canonical accession (q8c8r3_2 -> Q8C8R3-2), map on the
        base accession (Q8C8R3), and keep the isoform in uniprotid/unique_id so distinct
        proteoforms stay distinct.
        """
        datasets = self._build_datasets(
            data_overrides={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "ank2|q8c8r3": [1.0, 2.0],
                "ank2|q8c8r3_2": [3.0, 4.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprotkb_accession": ["Q8C8R3"],
                    "resource_identifier": ["ENSMUSG00000000001"],
                }
            ),
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "gene_symbol": ["Ank2"],
                }
            ),
        )

        output = transform_protein_de_individual(datasets=datasets)

        uniprotids = {e["uniprotid"] for e in output}
        assert uniprotids == {"Q8C8R3", "Q8C8R3-2"}

        by_uniprot = {e["uniprotid"]: e for e in output}
        assert by_uniprot["Q8C8R3-2"]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert by_uniprot["Q8C8R3-2"]["unique_id"] == "ENSMUSG00000000001Q8C8R3-2"
        assert by_uniprot["Q8C8R3-2"]["display_symbol"] == "Ank2 (Q8C8R3-2)"

    def test_individual_missing_from_metadata_is_dropped(self) -> None:
        """Test that a proteomics individual absent from harmonized metadata is dropped."""
        datasets = self._build_datasets(
            data_overrides={
                "specimenid": ["c1", "c2", "c3"],
                "individualid": ["i1", "i2", "i_unknown"],
                "Gene1|P00001": [1.0, 2.0, 3.0],
            }
        )

        output = transform_protein_de_individual(datasets=datasets)

        individuals = {d["individual_id"] for e in output for d in e["data"]}
        assert "i_unknown" not in individuals
        assert individuals == {"i1", "i2"}

    def test_unmapped_and_human_proteins_dropped(self) -> None:
        """Test that proteins with no mouse Ensembl mapping are dropped.

        One protein has an accession absent from the mapping and another maps only to a
        human gene; neither should appear in the output.
        """
        datasets = self._build_datasets(
            data_overrides={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "Gene1|P00001": [1.0, 2.0],
                "Bad|NOMAP": [3.0, 4.0],
                "Hum|HUMANP": [5.0, 6.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprotkb_accession": ["P00001", "HUMANP"],
                    "resource_identifier": [
                        "ENSMUSG00000000001",
                        "ENSG00000000001",
                    ],
                }
            ),
        )

        output = transform_protein_de_individual(datasets=datasets)

        uniprotids = {e["uniprotid"] for e in output}
        assert uniprotids == {"P00001"}

    def test_age_bucketing(self) -> None:
        """Test that continuous ageDeath is bucketed into nominal age groups.

        Uses the confirmed right-closed thresholds, including the 14.2 -> 12 months case.
        """
        datasets = self._build_datasets(
            harmonized_overrides={
                "individualid": ["i1", "i2", "i3", "i4"],
                "sex": ["male", "female", "male", "female"],
                "agedeath": [4.0, 14.2, 18.0, 24.2],
                "genotype": ["geno_hom", "geno_hom", "geno_hom", "geno_hom"],
                "tissue": ["right cerebral hemisphere"] * 4,
            },
            data_overrides={
                "specimenid": ["c1", "c2", "c3", "c4"],
                "individualid": ["i1", "i2", "i3", "i4"],
                "Gene1|P00001": [1.0, 2.0, 3.0, 4.0],
            },
        )

        output = transform_protein_de_individual(datasets=datasets)

        age_by_individual = {}
        for entry in output:
            for record in entry["data"]:
                age_by_individual[record["individual_id"]] = entry["age"]

        assert age_by_individual == {
            "i1": "4 months",
            "i2": "12 months",
            "i3": "18 months",
            "i4": "24 months",
        }

    def test_missing_required_dataset_raises(self) -> None:
        """Test that omitting a required dataset raises ValueError."""
        datasets = self._build_datasets()
        del datasets["uniprot_ensembl_map"]

        with pytest.raises(ValueError, match="Missing required datasets"):
            transform_protein_de_individual(datasets=datasets)

    def test_data_file_missing_id_column_raises(self) -> None:
        """Test that a wide data file missing an id column raises ValueError."""
        datasets = self._build_datasets(
            data_overrides={
                "individualid": ["i1", "i2"],
                "Gene1|P00001": [1.0, 2.0],
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_protein_de_individual(datasets=datasets)

    def test_empty_data_file_raises(self) -> None:
        """Test that an empty wide data file raises ValueError."""
        datasets = self._build_datasets(
            data_overrides={
                "specimenid": [],
                "individualid": [],
                "Gene1|P00001": [],
            }
        )

        with pytest.raises(ValueError, match="is empty"):
            transform_protein_de_individual(datasets=datasets)

    def test_no_data_files_raises(self) -> None:
        """Test that providing only metadata (no wide data file) raises ValueError."""
        datasets = self._build_datasets()
        del datasets["proteomics_file"]

        with pytest.raises(ValueError, match="No proteomics data files"):
            transform_protein_de_individual(datasets=datasets)

    def test_check_column_rules_rejects_empty_display_label(self) -> None:
        """Test that an empty display_label in the label map raises via check_column_rules."""
        datasets = self._build_datasets(
            label_map_overrides={"display_label": ["", "LOAD1"]}
        )

        with pytest.raises(ValueError, match="not_empty"):
            transform_protein_de_individual(datasets=datasets)

    def test_multiple_data_files_are_combined(self) -> None:
        """Test that two wide data files are melted and combined into one output set."""
        datasets = self._build_datasets(
            harmonized_overrides={
                "individualid": ["i1", "i2"],
                "sex": ["male", "female"],
                "agedeath": [4.0, 24.2],
                "genotype": ["geno_hom", "geno_hom"],
                "tissue": ["right cerebral hemisphere", "right cerebral hemisphere"],
            },
            data_overrides={
                "specimenid": ["c1"],
                "individualid": ["i1"],
                "Gene1|P00001": [1.0],
            },
            data_key="proteomics_4mo",
        )
        datasets["proteomics_24mo"] = pd.DataFrame(
            {
                "specimenid": ["c2"],
                "individualid": ["i2"],
                "Gene1|P00001": [2.0],
            }
        )

        output = transform_protein_de_individual(datasets=datasets)

        ages = {e["age"] for e in output}
        assert ages == {"4 months", "24 months"}
