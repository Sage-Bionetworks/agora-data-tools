"""
Test suite for the individual proteomics transformation.

Covers transform_protein_de_individual and its helpers, which reshape wide Model AD
proteomics files, join per-animal harmonized metadata, map proteins to mouse Ensembl genes,
and produce an RNA-style output plus the proteomics-specific fields uniprotid, unique_id,
and display_symbol.

The happy path is asserted against the golden fixtures in
tests/test_assets/protein_de_individual/; targeted behaviors use inline DataFrames.
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
    """Unit tests for the UniProt accession to mouse Ensembl gene id lookup."""

    def test_drops_human_genes_and_dedups_multi_mapped_accessions(self) -> None:
        """Test that human genes are excluded and multi-mapped accessions keep the smallest id."""
        mapping = pd.DataFrame(
            {
                "uniprotkb_accession": ["P1", "P1", "P2", "P3"],
                "resource_identifier": [
                    "ENSMUSG00000000005",
                    "ENSMUSG00000000002",
                    "ENSG00000000001",
                    "ENSMUSG00000000003",
                ],
            }
        )

        assert _build_uniprot_to_ensembl(mapping) == {
            "P1": "ENSMUSG00000000002",
            "P3": "ENSMUSG00000000003",
        }


class TestNormalizeTissue:
    """Unit tests for tissue alias mapping and the Hemibrain default."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("right cerebral hemisphere", "Hemibrain"),
            ("Right Cerebral Hemisphere", "Hemibrain"),
            (" right cerebral hemisphere ", "Hemibrain"),
            (None, "Hemibrain"),
            ("", "Hemibrain"),
            ("Cortex", "Cortex"),
        ],
    )
    def test_normalize_tissue(self, value: Any, expected: str) -> None:
        assert _normalize_tissue(pd.Series([value])).iloc[0] == expected

    def test_all_null_column(self) -> None:
        """Test that an entirely unpopulated tissue column defaults instead of raising.

        MG-985 flagged that the harmonized metadata may carry no tissue value at all, which
        pandas reads as a float column with no usable str accessor.
        """
        assert _normalize_tissue(pd.Series([None, None])).tolist() == [
            "Hemibrain",
            "Hemibrain",
        ]


class TestTransformProteinDeIndividual:
    """Integration tests for the full individual proteomics transformation."""

    data_files_path = "tests/test_assets/protein_de_individual"

    @staticmethod
    def _normalize(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort inner data lists and the outer list for order-independent comparison."""
        for entry in entries:
            entry["data"] = sorted(entry["data"], key=lambda x: x["individual_id"])
        return sorted(entries, key=lambda x: (x["unique_id"], x["age"]))

    def _build_datasets(
        self,
        harmonized: Dict[str, Any] = None,
        data_file: Dict[str, Any] = None,
        data_key: str = "proteomics_file",
        label_map: Dict[str, Any] = None,
        mapping: pd.DataFrame = None,
        gene_metadata: pd.DataFrame = None,
    ) -> Dict[str, pd.DataFrame]:
        """Build a minimal valid datasets dict, allowing any input to be replaced.

        Defaults: one homozygous (LOAD2) and one hAPP-WT (LOAD1 control) animal, both at
        4 months, and a single wide protein column gene1|p00001 mapping to a mouse gene.
        """
        return {
            "genotype_label_map": pd.DataFrame(
                label_map
                or {
                    "model": ["LOAD2", "LOAD2"],
                    "model_group": ["LOAD2", "LOAD2"],
                    "display_label": ["LOAD2", "LOAD1"],
                    "genotype": ["geno_hom", "geno_wt"],
                    "result_order": [2, 1],
                }
            ),
            "mouse_gene_metadata": (
                gene_metadata
                if gene_metadata is not None
                else pd.DataFrame(
                    {
                        "ensembl_gene_id": ["ENSMUSG00000000001"],
                        "gene_symbol": ["Gnai3"],
                    }
                )
            ),
            "load2_harmonized_metadata": pd.DataFrame(
                harmonized
                or {
                    "individualid": ["i1", "i2"],
                    "sex": ["male", "female"],
                    "agedeath": [4.0, 4.5],
                    "genotype": ["geno_hom", "geno_wt"],
                    "tissue": ["right cerebral hemisphere"] * 2,
                }
            ),
            "uniprot_ensembl_map": (
                mapping
                if mapping is not None
                else pd.DataFrame(
                    {
                        "uniprotkb_accession": ["P00001"],
                        "resource_identifier": ["ENSMUSG00000000001"],
                    }
                )
            ),
            data_key: pd.DataFrame(
                data_file
                or {
                    "specimenid": ["c1", "c2"],
                    "individualid": ["i1", "i2"],
                    "gene1|p00001": [1.0, 2.0],
                }
            ),
        }

    def test_synthetic_basic_data(self) -> None:
        """Test the happy path against the golden fixture output.

        Covers the wide-to-long melt, headers with no gene symbol, the harmonized metadata
        join, UniProt to Ensembl mapping, genotype label mapping, tissue mapping, sex
        title-casing, unique_id, the display_symbol fallback to ensembl_gene_id when no gene
        symbol is known, and wildtype exclusion (i3 is dropped).
        """
        input_path = os.path.join(self.data_files_path, "input")
        datasets = {
            "genotype_label_map": pd.read_csv(
                os.path.join(input_path, "synthetic_genotype_label_map.csv")
            ),
            "mouse_gene_metadata": pd.read_csv(
                os.path.join(input_path, "synthetic_mouse_gene_metadata.csv")
            ),
            "load2_harmonized_metadata": pd.read_csv(
                os.path.join(input_path, "synthetic_harmonized_metadata.csv")
            ),
            "uniprot_ensembl_map": pd.read_csv(
                os.path.join(input_path, "synthetic_uniprot_ensembl_map.csv")
            ),
            "synthetic_basic_data": pd.read_csv(
                os.path.join(input_path, "synthetic_basic_data.csv")
            ),
        }
        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected = json.load(f)

        output = transform_protein_de_individual(datasets=datasets)

        assert self._normalize(output) == self._normalize(expected)

    def test_isoform_proteoforms_stay_distinct(self) -> None:
        """Test that a pipeline-mangled isoform header is recovered and kept distinct.

        The pipeline lowercases headers and turns isoform hyphens into underscores. The
        transform must recover the canonical accession (q8c8r3_2 -> Q8C8R3-2), map on the
        base accession (Q8C8R3), and keep the isoform in uniprotid and unique_id.
        """
        datasets = self._build_datasets(
            data_file={
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
                    "gene_symbol": ["Gnai3"],
                }
            ),
        )

        by_uniprot = {
            e["uniprotid"]: e for e in transform_protein_de_individual(datasets)
        }

        assert set(by_uniprot) == {"Q8C8R3", "Q8C8R3-2"}
        isoform = by_uniprot["Q8C8R3-2"]
        assert isoform["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert isoform["unique_id"] == "ENSMUSG00000000001Q8C8R3-2"
        assert isoform["display_symbol"] == "Gnai3 (Q8C8R3-2)"

    def test_unmapped_and_human_proteins_dropped(self) -> None:
        """Test that proteins with no mouse Ensembl mapping are dropped.

        One accession is absent from the mapping and another maps only to a human gene.
        """
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [1.0, 2.0],
                "bad|nomap": [3.0, 4.0],
                "hum|humanp": [5.0, 6.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprotkb_accession": ["P00001", "HUMANP"],
                    "resource_identifier": ["ENSMUSG00000000001", "ENSG00000000001"],
                }
            ),
        )

        output = transform_protein_de_individual(datasets=datasets)

        assert {e["uniprotid"] for e in output} == {"P00001"}

    def test_animals_without_metadata_or_label_map_row_dropped(self) -> None:
        """Test that unjoinable animals are excluded.

        i_unknown is absent from the harmonized metadata; i4 is heterozygous, a genotype
        MG-985 confirmed has no label-map row and must not be displayed.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2", "i4"],
                "sex": ["male", "female", "female"],
                "agedeath": [4.0, 4.5, 5.1],
                "genotype": ["geno_hom", "geno_wt", "geno_het"],
                "tissue": ["right cerebral hemisphere"] * 3,
            },
            data_file={
                "specimenid": ["c1", "c2", "c3", "c4"],
                "individualid": ["i1", "i2", "i4", "i_unknown"],
                "gene1|p00001": [1.0, 2.0, 3.0, 4.0],
            },
        )

        output = transform_protein_de_individual(datasets=datasets)

        assert len(output) == 1
        assert {d["individual_id"] for d in output[0]["data"]} == {"i1", "i2"}
        assert {d["genotype"] for d in output[0]["data"]} == {"LOAD2", "LOAD1"}

    def test_age_bucketing_boundaries(self) -> None:
        """Test the right-closed ageDeath thresholds confirmed with JAX on MG-985.

        Boundary values fall into the lower bucket, and the real 14.2-month animals belong
        to the 12-month group.
        """
        ages = [6.0, 6.1, 10.0, 14.2, 16.0, 16.1, 20.0, 20.1]
        expected = [
            "4 months",
            "8 months",
            "8 months",
            "12 months",
            "12 months",
            "18 months",
            "18 months",
            "24 months",
        ]
        individuals = [f"i{n}" for n in range(len(ages))]
        datasets = self._build_datasets(
            harmonized={
                "individualid": individuals,
                "sex": ["male"] * len(ages),
                "agedeath": ages,
                "genotype": ["geno_hom"] * len(ages),
                "tissue": ["right cerebral hemisphere"] * len(ages),
            },
            data_file={
                "specimenid": individuals,
                "individualid": individuals,
                "gene1|p00001": [float(n) for n in range(len(ages))],
            },
        )

        output = transform_protein_de_individual(datasets=datasets)

        age_by_individual = {
            record["individual_id"]: entry["age"]
            for entry in output
            for record in entry["data"]
        }
        assert age_by_individual == dict(zip(individuals, expected))

    def test_missing_agedeath_raises(self) -> None:
        """Test that an unbucketable ageDeath fails loudly rather than dropping the animal."""
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2"],
                "sex": ["male", "female"],
                "agedeath": [4.0, None],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 2,
            }
        )

        with pytest.raises(ValueError, match="unbucketable ageDeath.*i2"):
            transform_protein_de_individual(datasets=datasets)

    def test_multiple_data_files_are_combined(self) -> None:
        """Test that the two source proteomics files are melted and combined."""
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2"],
                "sex": ["male", "female"],
                "agedeath": [4.0, 24.2],
                "genotype": ["geno_hom", "geno_hom"],
                "tissue": ["right cerebral hemisphere"] * 2,
            },
            data_file={
                "specimenid": ["c1"],
                "individualid": ["i1"],
                "gene1|p00001": [1.0],
            },
            data_key="proteomics_4mo",
        )
        datasets["proteomics_24mo"] = pd.DataFrame(
            {
                "specimenid": ["c2"],
                "individualid": ["i2"],
                "gene1|p00001": [2.0],
            }
        )

        output = transform_protein_de_individual(datasets=datasets)

        assert {e["age"] for e in output} == {"4 months", "24 months"}

    @pytest.mark.parametrize(
        "mutation,error",
        [
            ("drop_required_dataset", "Missing required datasets"),
            ("drop_data_file_id_column", "Missing required columns"),
            ("empty_data_file", "is empty"),
            ("drop_data_file", "No proteomics data files"),
            ("empty_display_label", "not_empty"),
            ("unmatched_genotypes", "No rows remained"),
        ],
    )
    def test_invalid_input_raises(self, mutation: str, error: str) -> None:
        """Test that each invalid-input path raises with an actionable message."""
        datasets = self._build_datasets()

        if mutation == "drop_required_dataset":
            del datasets["uniprot_ensembl_map"]
        elif mutation == "drop_data_file_id_column":
            datasets["proteomics_file"] = datasets["proteomics_file"].drop(
                columns=["specimenid"]
            )
        elif mutation == "empty_data_file":
            datasets["proteomics_file"] = datasets["proteomics_file"].iloc[:0]
        elif mutation == "drop_data_file":
            del datasets["proteomics_file"]
        elif mutation == "empty_display_label":
            datasets["genotype_label_map"]["display_label"] = ["", "LOAD1"]
        elif mutation == "unmatched_genotypes":
            datasets["load2_harmonized_metadata"]["genotype"] = ["unknown", "unknown"]

        with pytest.raises(ValueError, match=error):
            transform_protein_de_individual(datasets=datasets)
