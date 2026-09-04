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
    REQUIRED_INPUT,
    transform_protein_de_individual,
    _build_uniprot_candidates,
    _normalize_tissue,
    _resolve_gene_ids,
)


class TestBuildUniprotCandidates:
    """Unit tests for the UniProt accession to candidate mouse Ensembl gene ids lookup."""

    def test_drops_human_genes_and_keeps_all_mouse_candidates(self) -> None:
        """Test that human genes are excluded and multi-mapped accessions keep every candidate."""
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

        assert _build_uniprot_candidates(mapping) == {
            "P1": ["ENSMUSG00000000002", "ENSMUSG00000000005"],
            "P3": ["ENSMUSG00000000003"],
        }


class TestResolveGeneIds:
    """Unit tests for choosing one gene when an accession maps to several."""

    candidates = {"P1": ["ENSMUSG00000000001", "ENSMUSG00000000009"]}
    gene_symbols = {"ENSMUSG00000000001": "Gm10053", "ENSMUSG00000000009": "Cycs"}

    @staticmethod
    def _long_df(uniprotid: str, header_symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {"uniprotid": [uniprotid], "header_symbol": [header_symbol]}
        )

    @pytest.mark.parametrize(
        "header_symbol,expected",
        [
            # The named gene wins even though it holds the larger Ensembl gene id.
            ("Cycs", "ENSMUSG00000000009"),
            ("cycs", "ENSMUSG00000000009"),
            ("Gm10053", "ENSMUSG00000000001"),
            # Unusable symbols fall back to the smallest id.
            ("", "ENSMUSG00000000001"),
            ("NA", "ENSMUSG00000000001"),
            # A symbol naming neither candidate cannot resolve.
            ("Rps27", "ENSMUSG00000000001"),
            # A symbol naming both candidates is genuinely ambiguous.
            ("Cycs; Gm10053", "ENSMUSG00000000001"),
        ],
    )
    def test_header_symbol_picks_the_gene(
        self, header_symbol: str, expected: str
    ) -> None:
        resolved = _resolve_gene_ids(
            self._long_df("P1", header_symbol), self.candidates, self.gene_symbols, {}
        )

        assert resolved == {"P1": expected}

    def test_multi_gene_header_symbol_resolves_on_one_match(self) -> None:
        """Test that a semicolon-joined symbol still resolves if only one candidate matches."""
        resolved = _resolve_gene_ids(
            self._long_df("P1", "Cycs; Rps27"),
            self.candidates,
            self.gene_symbols,
            {},
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}

    def test_ambiguous_match_stays_within_the_named_genes(self) -> None:
        """Test that a tie between named genes is broken without leaving the named genes.

        No production accession matches several candidates today, so the smallest candidate
        happens to be a named one; this pins the behavior if that ever stops holding.
        """
        resolved = _resolve_gene_ids(
            self._long_df("P3", "H4c1; H4c2"),
            {
                "P3": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000004",
                    "ENSMUSG00000000007",
                ]
            },
            {
                "ENSMUSG00000000001": "Gm10053",
                "ENSMUSG00000000004": "H4c1",
                "ENSMUSG00000000007": "H4c2",
            },
            {},
        )

        assert resolved == {"P3": "ENSMUSG00000000004"}

    def test_alias_resolves_nomenclature_drift(self) -> None:
        """Test the alias fallback when the file uses an older symbol than the metadata.

        The proteomics files still say Srp54 where mouse_gene_metadata says Srp54a.
        """
        resolved = _resolve_gene_ids(
            self._long_df("P2", "Srp54"),
            {"P2": ["ENSMUSG00000000002", "ENSMUSG00000000008"]},
            {"ENSMUSG00000000002": "Srp54b", "ENSMUSG00000000008": "Srp54a"},
            {"ENSMUSG00000000008": {"srp54"}},
        )

        assert resolved == {"P2": "ENSMUSG00000000008"}

    def test_isoform_symbol_resolves_base_accession(self) -> None:
        """Test that an isoform's header symbol resolves the base accession's gene."""
        resolved = _resolve_gene_ids(
            self._long_df("P1-2", "Cycs"), self.candidates, self.gene_symbols, {}
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}


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

    @staticmethod
    def _transform(
        datasets: Dict[str, pd.DataFrame], model_map: Dict[str, str] = None
    ) -> List[Dict[str, Any]]:
        """Run the transform, defaulting every data file to LOAD2.

        Tests that do not care about the model get the single-model case for free; tests
        that do pass model_map explicitly.
        """
        if model_map is None:
            model_map = {key: "LOAD2" for key in datasets if key not in REQUIRED_INPUT}
        return transform_protein_de_individual(datasets=datasets, model_map=model_map)

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
                        "alias": [[]],
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

        output = self._transform(datasets)

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
                    "alias": [[]],
                }
            ),
        )

        by_uniprot = {e["uniprotid"]: e for e in self._transform(datasets)}

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

        output = self._transform(datasets)

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

        output = self._transform(datasets)

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

        output = self._transform(datasets)

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
            self._transform(datasets)

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

        output = self._transform(datasets)

        assert {e["age"] for e in output} == {"4 months", "24 months"}

    def test_synthetic_multimodel_data(self) -> None:
        """Test the multi-model happy path against the golden fixture output.

        Three data files carry three models across two model groups, so every per-model
        output field has to be resolved per group. LOAD2 has two genotypes and Bin1K358R
        has four, which is what makes a globally computed result_order visibly wrong.
        """
        input_path = os.path.join(self.data_files_path, "input")
        datasets = {
            "genotype_label_map": pd.read_csv(
                os.path.join(input_path, "synthetic_multimodel_genotype_label_map.csv")
            ),
            "mouse_gene_metadata": pd.read_csv(
                os.path.join(input_path, "synthetic_mouse_gene_metadata.csv")
            ),
            "load2_harmonized_metadata": pd.read_csv(
                os.path.join(input_path, "synthetic_multimodel_harmonized_metadata.csv")
            ),
            "uniprot_ensembl_map": pd.read_csv(
                os.path.join(input_path, "synthetic_uniprot_ensembl_map.csv")
            ),
        }
        model_map = {
            "synthetic_multimodel_load2_data": "LOAD2",
            "synthetic_multimodel_bin1_data": "Bin1-K358R",
            "synthetic_multimodel_bin1_5xfad_data": "Bin1-K358R.5xFAD",
        }
        for data_key in model_map:
            datasets[data_key] = pd.read_csv(
                os.path.join(input_path, f"{data_key}.csv")
            )
        with open(
            os.path.join(
                self.data_files_path, "output", "synthetic_multimodel_output.json"
            )
        ) as f:
            expected = json.load(f)

        output = self._transform(datasets, model_map=model_map)

        assert self._normalize(output) == self._normalize(expected)

    def test_per_model_group_fields_are_not_shared_across_groups(self) -> None:
        """Test that name, matched_control, and result_order are resolved per model_group.

        Uses the real Model AD shape: LOAD2 is one model in its own group with two
        genotypes, while Bin1K358R is one group fed by two models across two files with
        four genotypes between them. The differing genotype counts mean a result_order
        computed over the whole frame, rather than per group, would be visibly wrong.
        """
        datasets = self._build_datasets(
            label_map={
                "model": [
                    "LOAD2",
                    "LOAD2",
                    "Bin1-K358R",
                    "Bin1-K358R",
                    "Bin1-K358R.5xFAD",
                    "Bin1-K358R.5xFAD",
                ],
                "model_group": ["LOAD2", "LOAD2"] + ["Bin1K358R"] * 4,
                "display_label": [
                    "LOAD2",
                    "LOAD1",
                    "C57BL/6J",
                    "Bin1K358R",
                    "5xFAD",
                    "Bin1K358R.5xFAD",
                ],
                "genotype": [
                    "geno_hom",
                    "geno_wt",
                    "fad_non",
                    "bin1_hom",
                    "fad_car",
                    "fad_car_bin1",
                ],
                "result_order": [2, 1, 1, 2, 3, 4],
            },
            harmonized={
                "individualid": ["i1", "i2", "i3", "i4", "i5", "i6"],
                "sex": ["male"] * 6,
                "agedeath": [4.0] * 6,
                "genotype": [
                    "geno_hom",
                    "geno_wt",
                    "fad_non",
                    "bin1_hom",
                    "fad_car",
                    "fad_car_bin1",
                ],
                "tissue": ["right cerebral hemisphere"] * 6,
            },
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [1.0, 2.0],
            },
            data_key="load2_file",
        )
        datasets["bin1_file"] = pd.DataFrame(
            {
                "specimenid": ["c3", "c4"],
                "individualid": ["i3", "i4"],
                "gene1|p00001": [3.0, 4.0],
            }
        )
        datasets["bin1_fad_file"] = pd.DataFrame(
            {
                "specimenid": ["c5", "c6"],
                "individualid": ["i5", "i6"],
                "gene1|p00001": [5.0, 6.0],
            }
        )

        output = self._transform(
            datasets,
            model_map={
                "load2_file": "LOAD2",
                "bin1_file": "Bin1-K358R",
                "bin1_fad_file": "Bin1-K358R.5xFAD",
            },
        )

        by_group = {entry["model_group"]: entry for entry in output}
        assert set(by_group) == {"LOAD2", "Bin1K358R"}

        load2 = by_group["LOAD2"]
        assert load2["name"] == "LOAD2"
        assert load2["matched_control"] == "LOAD1"
        assert load2["result_order"] == ["LOAD1", "LOAD2"]
        assert {d["individual_id"] for d in load2["data"]} == {"i1", "i2"}

        bin1 = by_group["Bin1K358R"]
        assert bin1["name"] == "Bin1K358R"
        assert bin1["matched_control"] == "C57BL/6J"
        assert bin1["result_order"] == [
            "C57BL/6J",
            "Bin1K358R",
            "5xFAD",
            "Bin1K358R.5xFAD",
        ]
        assert {d["individual_id"] for d in bin1["data"]} == {"i3", "i4", "i5", "i6"}

    @pytest.mark.parametrize(
        "model_map,error",
        [
            # A data file the config forgot to declare.
            ({}, "No model declared"),
            # A config typo naming a file that is not in this dataset.
            (
                {"proteomics_file": "LOAD2", "typo_file": "LOAD2"},
                "not proteomics data files",
            ),
            # A model the label map cannot label, which would otherwise drop every row and
            # surface as the unrelated "No rows remained" error.
            ({"proteomics_file": "LOAD3"}, "absent from the genotype label map"),
        ],
    )
    def test_invalid_model_map_raises(
        self, model_map: Dict[str, str], error: str
    ) -> None:
        """Test that a model_map not matching the data files fails with an actionable message."""
        datasets = self._build_datasets()

        with pytest.raises(ValueError, match=error):
            self._transform(datasets, model_map=model_map)

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
            self._transform(datasets)
