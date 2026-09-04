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
import math
import os
from typing import Any, Dict, List

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_individual import (
    REQUIRED_INPUT,
    transform_protein_de_individual,
    _build_gene_aliases,
    _build_uniprot_candidates,
    _melt_proteomics_file,
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
                "ensembl_gene_id": [
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


class TestBuildGeneAliases:
    """Unit tests for the Ensembl gene id to alias set lookup."""

    def test_aliases_are_case_folded_and_missing_values_skipped(self) -> None:
        """Test the shapes the alias column actually arrives in.

        mouse_gene_metadata is JSON, so alias is a real list per gene, but a gene with no
        aliases can arrive as an empty list or as a null, and a list can hold a null.
        """
        metadata = pd.DataFrame(
            {
                "ensembl_gene_id": [
                    "ENSMUSG00000000001",
                    "ENSMUSG00000000002",
                    "ENSMUSG00000000003",
                    "ENSMUSG00000000004",
                ],
                "alias": [["Gnai-3", "HG1A"], [], [None, "Srp54"], None],
            }
        )

        assert _build_gene_aliases(metadata) == {
            "ENSMUSG00000000001": {"gnai-3", "hg1a"},
            "ENSMUSG00000000002": set(),
            "ENSMUSG00000000003": {"srp54"},
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


class TestMeltProteomicsFile:
    """Unit tests for reshaping one wide proteomics file to long form."""

    data_file = pd.DataFrame(
        {
            "specimenid": ["c1", "c2"],
            "individualid": [51503, 51504],
            "gene1|p00001": [1.0, None],
            "ank2|q8c8r3_2": [2.0, 3.0],
        }
    )

    def test_melts_protein_columns_and_recovers_isoform_accessions(self) -> None:
        """Test the long shape, the isoform accession recovery, and the str individualid cast.

        q8c8r3_2 must come back as Q8C8R3-2, and individualid must be a string because the
        two source files disagree on its dtype.
        """
        long_df = _melt_proteomics_file("proteomics_file", self.data_file, "LOAD2")

        assert list(long_df.columns) == [
            "individualid",
            "model",
            "uniprotid",
            "header_symbol",
            "value",
        ]
        # The null gene1|p00001 measurement for c2 is dropped, leaving 3 of 4.
        assert len(long_df) == 3
        assert set(long_df["uniprotid"]) == {"P00001", "Q8C8R3-2"}
        assert long_df["header_symbol"].tolist() == ["gene1", "ank2", "ank2"]
        assert long_df["individualid"].tolist() == ["51503", "51503", "51504"]
        assert set(long_df["model"]) == {"LOAD2"}

    def test_metadata_only_columns_are_not_melted(self) -> None:
        """Test that a new upstream metadata column cannot become a phantom protein."""
        data_file = self.data_file.assign(sequencing_batch=["b1", "b2"])

        long_df = _melt_proteomics_file("proteomics_file", data_file, "LOAD2")

        assert set(long_df["uniprotid"]) == {"P00001", "Q8C8R3-2"}

    def test_no_protein_columns_raises(self) -> None:
        """Test that a file whose protein columns went missing fails loudly."""
        with pytest.raises(ValueError, match="no protein columns"):
            _melt_proteomics_file(
                "proteomics_file",
                self.data_file[["specimenid", "individualid"]],
                "LOAD2",
            )

    def test_non_numeric_value_names_its_file(self) -> None:
        """Test that an unparseable abundance reports the file it came from."""
        data_file = self.data_file.assign(**{"gene1|p00001": ["1.0", "not_a_number"]})

        with pytest.raises(ValueError, match="'proteomics_file'.*not_a_number"):
            _melt_proteomics_file("proteomics_file", data_file, "LOAD2")


class TestNormalizeTissue:
    """Unit tests for tissue alias mapping."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("right cerebral hemisphere", "Hemibrain"),
            ("Right Cerebral Hemisphere", "Hemibrain"),
            (" right cerebral hemisphere ", "Hemibrain"),
            ("Cortex", "Cortex"),
        ],
    )
    def test_normalize_tissue(self, value: Any, expected: str) -> None:
        assert _normalize_tissue(pd.Series([value])).iloc[0] == expected

    def test_all_null_column_is_left_null(self) -> None:
        """Test that an unpopulated tissue column survives to the caller's check.

        pandas reads an entirely empty column as float, which has no usable str accessor,
        so the cast has to happen before the alias mapping.
        """
        assert _normalize_tissue(pd.Series([None, None])).isna().all()


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
        datasets: Dict[str, pd.DataFrame],
        model_map: Dict[str, str] = None,
        harmonized_metadata: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """Run the transform, defaulting every data file to LOAD2.

        Tests that do not care about the model get the single-model case for free; tests
        that do pass model_map explicitly. Any dataset whose key ends in
        _harmonized_metadata is taken as metadata, mirroring how the config declares it.
        """
        if harmonized_metadata is None:
            harmonized_metadata = [
                key for key in datasets if key.endswith("harmonized_metadata")
            ]
        if model_map is None:
            model_map = {
                key: "LOAD2"
                for key in datasets
                if key not in REQUIRED_INPUT and key not in harmonized_metadata
            }
        return transform_protein_de_individual(
            datasets=datasets,
            model_map=model_map,
            harmonized_metadata=harmonized_metadata,
        )

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
                        "ensembl_gene_id": ["ENSMUSG00000000001"],
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
            # JSON, matching the production format, so alias arrives as a real list and the
            # alias branch of _resolve_gene_ids is reachable.
            "mouse_gene_metadata": pd.read_json(
                os.path.join(input_path, "synthetic_mouse_gene_metadata.json")
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
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
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
                    "ensembl_gene_id": ["ENSMUSG00000000001", "ENSG00000000001"],
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

    def test_mostly_unjoinable_data_file_raises(self) -> None:
        """Test that a data file whose join key stopped matching fails instead of shrinking.

        The animals are still present in both sources, but the individualIDs no longer
        agree, which is what an upstream dtype change looks like. Without the coverage
        check the other file's animals would satisfy every later guard.
        """
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [1.0, 2.0],
            },
            data_key="good_file",
        )
        datasets["stale_file"] = pd.DataFrame(
            {
                "specimenid": ["c3", "c4"],
                "individualid": ["i1.0", "i2.0"],
                "gene1|p00001": [3.0, 4.0],
            }
        )

        with pytest.raises(
            ValueError, match="'stale_file' were found in the harmonized"
        ):
            self._transform(datasets)

    def test_age_bucketing_boundaries(self) -> None:
        """Test the right-closed ageDeath thresholds confirmed with JAX on MG-985.

        Boundary values fall into the lower bucket, and the real 14.2-month animals belong
        to the 12-month group.
        """
        ages = [6.0, 6.1, 10.0, 14.2, 16.0, 16.1, 20.0, 20.1]
        expected = [4, 8, 8, 12, 12, 18, 18, 24]
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
            record["individual_id"]: (entry["age"], entry["age_numeric"])
            for entry in output
            for record in entry["data"]
        }
        assert age_by_individual == {
            individual: (f"{months} months", months)
            for individual, months in zip(individuals, expected)
        }

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

    @pytest.mark.parametrize("tissue", [None, ""])
    def test_missing_tissue_raises(self, tissue: Any) -> None:
        """Test that a blank tissue fails loudly rather than defaulting to Hemibrain.

        MG-985 asked whether tissue had to be hard-coded because the study metadata carried
        none; it does not, so a non-JAX study arriving without one must not be mislabeled.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2"],
                "sex": ["male", "female"],
                "agedeath": [4.0, 4.5],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere", tissue],
            }
        )

        with pytest.raises(ValueError, match="Missing tissue.*i2"):
            self._transform(datasets)

    @pytest.mark.parametrize(
        "source,expected",
        [
            (["male", "female"], {"Male", "Female"}),
            (["Males", "Females"], {"Male", "Female"}),
            (["M", "F"], {"M", "F"}),
        ],
    )
    def test_sex_labels_are_singular_and_title_cased(
        self, source: List[str], expected: set
    ) -> None:
        """Test that both the plural and the lowercase source spellings are normalized.

        The harmonized metadata says male/female, but the RNA data says Males/Females and
        both datasets render on the same page, so the two must agree.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2"],
                "sex": source,
                "agedeath": [4.0, 4.5],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 2,
            }
        )

        output = self._transform(datasets)

        assert {d["sex"] for d in output[0]["data"]} == expected

    def test_all_null_sex_survives(self) -> None:
        """Test that an unpopulated sex column serializes as null rather than raising.

        pandas reads an entirely empty column as float, which has no str accessor.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2"],
                "sex": [None, None],
                "agedeath": [4.0, 4.5],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 2,
            }
        )

        output = self._transform(datasets)

        assert {d["sex"] for d in output[0]["data"]} == {None}

    def test_negative_zero_is_normalized(self) -> None:
        """Test that a small negative abundance does not serialize as -0.0.

        The abundances are batch-regressed and centred on zero, so rounding to 5 places
        turns many of them into negative zero, which json.dumps writes with its sign.
        """
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [-0.000001, -0.0],
            }
        )

        values = [d["value"] for d in self._transform(datasets)[0]["data"]]

        assert values == [0.0, 0.0]
        assert not any(math.copysign(1, value) < 0 for value in values)

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

    def test_duplicate_animal_rows_are_tolerated(self) -> None:
        """Test that an animal with two identical harmonized metadata rows is not fanned out.

        individualID repeats in the harmonized metadata for animals with more than one
        specimen; the duplicate rows agree, so each measurement must still appear once.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i1", "i2"],
                "sex": ["male", "male", "female"],
                "agedeath": [4.0, 4.0, 4.5],
                "genotype": ["geno_hom", "geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 3,
            }
        )

        output = self._transform(datasets)

        assert [d["individual_id"] for d in output[0]["data"]] == ["i1", "i2"]

    def test_conflicting_animal_rows_raise(self) -> None:
        """Test that a harmonized metadata disagreeing with itself about an animal raises.

        Whichever genotype came first would otherwise be picked silently, which for a
        control-versus-carrier disagreement means publishing the wrong group.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i1", "i2"],
                "sex": ["male", "male", "female"],
                "agedeath": [4.0, 4.0, 4.5],
                "genotype": ["geno_hom", "geno_wt", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 3,
            }
        )

        with pytest.raises(ValueError, match="not a many-to-one merge"):
            self._transform(datasets)

    def test_duplicate_model_genotype_in_label_map_raises(self) -> None:
        """Test that a label map with two rows for one (model, genotype) raises.

        The duplicate would fan every one of that genotype's measurements out into two
        rows, doubling the animals reported for the group.
        """
        datasets = self._build_datasets(
            label_map={
                "model": ["LOAD2", "LOAD2", "LOAD2"],
                "model_group": ["LOAD2"] * 3,
                "display_label": ["LOAD2", "LOAD2 dup", "LOAD1"],
                "genotype": ["geno_hom", "geno_hom", "geno_wt"],
                "result_order": [2, 3, 1],
            }
        )

        with pytest.raises(ValueError, match="not a many-to-one merge"):
            self._transform(datasets)

    @pytest.mark.parametrize(
        "model_map,error",
        [
            # A config that forgot the model_map block entirely.
            (None, "No model_map provided"),
            # A data file the config forgot to declare.
            ({}, "No model_map provided"),
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
            transform_protein_de_individual(
                datasets=datasets,
                model_map=model_map,
                harmonized_metadata=["load2_harmonized_metadata"],
            )

    @pytest.mark.parametrize(
        "harmonized_metadata,error",
        [
            # A config that forgot the harmonized_metadata block entirely.
            (None, "No harmonized_metadata provided"),
            ([], "No harmonized_metadata provided"),
            # A config typo naming a file that is not in this dataset.
            (["load2_harmonized_metadta"], "not files in this dataset"),
        ],
    )
    def test_invalid_harmonized_metadata_raises(
        self, harmonized_metadata: List[str], error: str
    ) -> None:
        """Test that an undeclared or mistyped metadata file fails with an actionable message.

        Without its own check the file would fall through to file_list and surface as the
        unrelated "No model declared" error.
        """
        datasets = self._build_datasets()

        with pytest.raises(ValueError, match=error):
            transform_protein_de_individual(
                datasets=datasets,
                model_map={"proteomics_file": "LOAD2"},
                harmonized_metadata=harmonized_metadata,
            )

    def test_second_study_metadata_file_is_combined(self) -> None:
        """Test that a second study's metadata file is concatenated rather than replacing.

        The metadata is study-scoped, so a second study arrives as its own file. Its animals
        must reach the output alongside the first study's.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1"],
                "sex": ["male"],
                "agedeath": [4.0],
                "genotype": ["geno_hom"],
                "tissue": ["right cerebral hemisphere"],
            },
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [1.0, 2.0],
            },
        )
        datasets["uci_harmonized_metadata"] = pd.DataFrame(
            {
                "individualid": ["i2"],
                "sex": ["female"],
                "agedeath": [4.5],
                "genotype": ["geno_wt"],
                "tissue": ["Cortex"],
            }
        )

        output = self._transform(datasets)

        assert {e["tissue"] for e in output} == {"Hemibrain", "Cortex"}
        assert {
            record["individual_id"] for entry in output for record in entry["data"]
        } == {"i1", "i2"}

    def test_metadata_files_disagreeing_about_an_animal_raise(self) -> None:
        """Test that an individualID meaning different animals in two studies raises.

        Model AD studies number their animals in non-overlapping ranges today, and nothing
        structurally prevents a future collision, so it must not resolve to whichever file
        was listed first.
        """
        datasets = self._build_datasets()
        datasets["uci_harmonized_metadata"] = pd.DataFrame(
            {
                "individualid": ["i1"],
                "sex": ["female"],
                "agedeath": [24.0],
                "genotype": ["geno_wt"],
                "tissue": ["Cortex"],
            }
        )

        with pytest.raises(ValueError, match="not a many-to-one merge"):
            self._transform(datasets)

    def test_metadata_files_agreeing_about_an_animal_are_deduplicated(self) -> None:
        """Test that an animal appearing identically in two studies' metadata is not fanned out.

        The two files can disagree on the dtype of the join key, so 51503 and "51503" must
        collapse to one row rather than surviving as two.
        """
        datasets = self._build_datasets(
            harmonized={
                "individualid": [51503, 51504],
                "sex": ["male", "female"],
                "agedeath": [4.0, 4.5],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere"] * 2,
            },
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": [51503, 51504],
                "gene1|p00001": [1.0, 2.0],
            },
        )
        datasets["uci_harmonized_metadata"] = pd.DataFrame(
            {
                "individualid": ["51503"],
                "sex": ["male"],
                "agedeath": [4.0],
                "genotype": ["geno_hom"],
                "tissue": ["right cerebral hemisphere"],
            }
        )

        output = self._transform(datasets)

        assert [d["individual_id"] for d in output[0]["data"]] == ["51503", "51504"]

    def test_undeclared_data_file_raises(self) -> None:
        """Test that a data file missing from a populated model_map is named."""
        datasets = self._build_datasets()
        datasets["second_file"] = datasets["proteomics_file"]

        with pytest.raises(ValueError, match="No model declared.*second_file"):
            self._transform(datasets, model_map={"proteomics_file": "LOAD2"})

    @pytest.mark.parametrize(
        "mutation,error",
        [
            ("drop_required_dataset", "Missing required datasets"),
            ("drop_data_file_id_column", "Missing required columns"),
            # HARMONIZED_COLUMNS and its rules are enforced per declared metadata file
            # rather than under a fixed dataset key.
            ("drop_metadata_column", "Missing required columns"),
            ("empty_metadata_genotype", "not_empty"),
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
        elif mutation == "drop_metadata_column":
            datasets["load2_harmonized_metadata"] = datasets[
                "load2_harmonized_metadata"
            ].drop(columns=["agedeath"])
        elif mutation == "empty_metadata_genotype":
            datasets["load2_harmonized_metadata"]["genotype"] = ["", "geno_wt"]
        elif mutation == "drop_data_file_id_column":
            datasets["proteomics_file"] = datasets["proteomics_file"].drop(
                columns=["individualid"]
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
