"""Tests for the shared Model AD proteomics identity helpers."""

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    build_gene_aliases,
    build_uniprot_candidates,
    pairs_from_protein_ids,
    protein_display_symbol,
    protein_unique_id,
    resolve_gene_ids,
)

CANDIDATES = {"P1": ["ENSMUSG00000000001", "ENSMUSG00000000009"]}
GENE_SYMBOLS = {"ENSMUSG00000000001": "Gm10053", "ENSMUSG00000000009": "Cycs"}


class TestBuildUniprotCandidates:
    def test_drops_human_genes_and_keeps_all_mouse_candidates(self) -> None:
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

        assert build_uniprot_candidates(mapping) == {
            "P1": ["ENSMUSG00000000002", "ENSMUSG00000000005"],
            "P3": ["ENSMUSG00000000003"],
        }


class TestBuildGeneAliases:
    def test_aliases_are_case_folded_and_missing_values_skipped(self) -> None:
        """mouse_gene_metadata is JSON, so alias is a real list per gene.

        A gene with no aliases can arrive as an empty list or as a null, and a
        list can hold a null.
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

        assert build_gene_aliases(metadata) == {
            "ENSMUSG00000000001": {"gnai-3", "hg1a"},
            "ENSMUSG00000000002": set(),
            "ENSMUSG00000000003": {"srp54"},
        }


class TestPairsFromProteinIds:
    def test_splits_symbol_and_canonical_accession(self) -> None:
        pairs = pairs_from_protein_ids(
            pd.Series(["Dync1h1|Q9JHU4", "Ank2|Q8C8R3-2", "ank2|q8c8r3_2"])
        )

        assert set(pairs["uniprotid"]) == {"Q9JHU4", "Q8C8R3-2"}
        assert set(pairs["header_symbol"]) == {"Dync1h1", "Ank2", "ank2"}

    def test_missing_pipe_raises(self) -> None:
        with pytest.raises(ValueError, match="gene_symbol\\|uniprotid"):
            pairs_from_protein_ids(pd.Series(["Q9JHU4"]))


class TestResolveGeneIds:
    @staticmethod
    def _header_pairs(uniprotid: str, header_symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {"uniprotid": [uniprotid], "header_symbol": [header_symbol]}
        )

    @pytest.mark.parametrize(
        "header_symbol,expected",
        [
            ("Cycs", "ENSMUSG00000000009"),
            ("cycs", "ENSMUSG00000000009"),
            ("Gm10053", "ENSMUSG00000000001"),
            ("", "ENSMUSG00000000001"),
            ("NA", "ENSMUSG00000000001"),
            ("Rps27", "ENSMUSG00000000001"),
            ("Cycs;_Gm10053", "ENSMUSG00000000001"),
        ],
    )
    def test_header_symbol_picks_the_gene(
        self, header_symbol: str, expected: str
    ) -> None:
        resolved = resolve_gene_ids(
            self._header_pairs("P1", header_symbol),
            CANDIDATES,
            GENE_SYMBOLS,
            {},
        )

        assert resolved == {"P1": expected}

    def test_multi_gene_header_symbol_resolves_on_one_match(self) -> None:
        resolved = resolve_gene_ids(
            self._header_pairs("P1", "Cycs;_Rps27"),
            CANDIDATES,
            GENE_SYMBOLS,
            {},
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}

    def test_ambiguous_match_stays_within_the_named_genes(self) -> None:
        resolved = resolve_gene_ids(
            self._header_pairs("P3", "H4c1;_H4c2"),
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
        resolved = resolve_gene_ids(
            self._header_pairs("P2", "Srp54"),
            {"P2": ["ENSMUSG00000000002", "ENSMUSG00000000008"]},
            {"ENSMUSG00000000002": "Srp54b", "ENSMUSG00000000008": "Srp54a"},
            {"ENSMUSG00000000008": {"srp54"}},
        )

        assert resolved == {"P2": "ENSMUSG00000000008"}

    def test_isoform_symbol_resolves_base_accession(self) -> None:
        resolved = resolve_gene_ids(
            self._header_pairs("P1-2", "Cycs"), CANDIDATES, GENE_SYMBOLS, {}
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}


class TestProteinIdentityFields:
    def test_unique_id_and_display_symbol(self) -> None:
        ensembl = pd.Series(["ENSMUSG00000000001", "ENSMUSG00000000002"])
        uniprot = pd.Series(["P27144", "Q8C8R3-2"])
        symbol = pd.Series(["Gnai3", ""])

        assert protein_unique_id(ensembl, uniprot).tolist() == [
            "ENSMUSG00000000001P27144",
            "ENSMUSG00000000002Q8C8R3-2",
        ]
        assert protein_display_symbol(symbol, ensembl, uniprot).tolist() == [
            "Gnai3 (P27144)",
            "ENSMUSG00000000002 (Q8C8R3-2)",
        ]
