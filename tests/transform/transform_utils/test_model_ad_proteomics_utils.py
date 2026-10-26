"""Tests for the shared Model AD proteomics identity helpers."""

import logging

import pandas as pd
import pytest

from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    add_protein_identity_columns,
    build_uniprot_to_ensembl,
    canonical_accession,
    log_unmapped_accessions,
    map_accessions_to_genes,
    protein_display_symbol,
    protein_unique_id,
    unmapped_accessions,
)


class TestBuildUniprotToEnsembl:
    def test_drops_human_genes_and_takes_the_smallest_mouse_gene(self) -> None:
        """Human Ensembl ids are dropped; an accession with several mouse genes takes the smallest."""
        mapping = pd.DataFrame(
            {
                "uniprot_id": ["P1", "P1", "P2", "P3"],
                "ensembl_gene_id": [
                    "ENSMUSG00000000005",
                    "ENSMUSG00000000002",
                    "ENSG00000000001",
                    "ENSMUSG00000000003",
                ],
            }
        )

        assert build_uniprot_to_ensembl(mapping) == {
            "P1": "ENSMUSG00000000002",
            "P3": "ENSMUSG00000000003",
        }

    def test_extra_map_column_is_ignored(self) -> None:
        """Extra columns on the UniProt map are ignored."""
        mapping = pd.DataFrame(
            {
                "uniprot_id": ["P00001"],
                "ensembl_gene_id": ["ENSMUSG00000000001"],
                "optional_information": [""],
            }
        )

        assert build_uniprot_to_ensembl(mapping) == {"P00001": "ENSMUSG00000000001"}


class TestUnmappedAccessions:
    def test_isoform_is_mapped_to_base_id(self) -> None:
        """Isoforms should map to their base ID ("-<number>" removed), so the only unmapped accession in this list
        should be P00001, which doesn't exist in the lookup."""
        lookup = {"Q8C8R3": "ENSMUSG00000000001"}
        accessions = pd.Series(["Q8C8R3-2", "P00001", "Q8C8R3"])

        assert unmapped_accessions(accessions, lookup) == ["P00001"]


class TestCanonicalAccession:
    def test_recovers_accession_from_header_and_isoform(self) -> None:
        accessions = canonical_accession(
            pd.Series(["Dync1h1|Q9JHU4", "Ank2|Q8C8R3-2", "ank2|q8c8r3_2"])
        )

        assert accessions.tolist() == ["Q9JHU4", "Q8C8R3-2", "Q8C8R3-2"]


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


class TestMapAccessionsToGenes:
    def test_maps_an_isoform_on_its_base_accession(self) -> None:
        """An isoform is looked up on its base accession but kept as a separate entry from the base"""
        df = pd.DataFrame({"uniprotid": ["Q8C8R3-2", "Q8C8R3"]})

        result = map_accessions_to_genes(
            df, {"Q8C8R3": "ENSMUSG00000000001"}, "in 'file'"
        )

        assert result["uniprotid"].tolist() == ["Q8C8R3-2", "Q8C8R3"]
        assert result["ensembl_gene_id"].tolist() == [
            "ENSMUSG00000000001",
            "ENSMUSG00000000001",
        ]

    def test_drops_unmapped_accessions(self) -> None:
        """Any accession the map does not cover is dropped."""
        df = pd.DataFrame({"uniprotid": ["Q8C8R3", "P00001"]})

        result = map_accessions_to_genes(
            df, {"Q8C8R3": "ENSMUSG00000000001"}, "in 'file'"
        )

        assert result["uniprotid"].tolist() == ["Q8C8R3"]
        assert result["ensembl_gene_id"].tolist() == ["ENSMUSG00000000001"]

    def test_raises_when_nothing_maps(self) -> None:
        """A file whose accessions all miss the map raises instead of emitting an empty result."""
        df = pd.DataFrame({"uniprotid": ["P00001"]})

        with pytest.raises(ValueError, match="No rows remained in 'file'"):
            map_accessions_to_genes(df, {}, "in 'file'")


class TestAddProteinIdentityColumns:
    def test_fills_symbol_unique_id_and_display_symbol(self) -> None:
        """unique_id joins the Ensembl id and accession, and a missing symbol falls back to the Ensembl id."""
        df = pd.DataFrame(
            {
                "ensembl_gene_id": ["ENSMUSG00000000001", "ENSMUSG00000000002"],
                "uniprotid": ["P27144", "Q8C8R3-2"],
            }
        )

        result = add_protein_identity_columns(df, {"ENSMUSG00000000001": "Gnai3"})

        assert result["gene_symbol"].tolist() == ["Gnai3", ""]
        assert result["unique_id"].tolist() == [
            "ENSMUSG00000000001P27144",
            "ENSMUSG00000000002Q8C8R3-2",
        ]
        assert result["display_symbol"].tolist() == [
            "Gnai3 (P27144)",
            "ENSMUSG00000000002 (Q8C8R3-2)",
        ]


class TestLogUnmappedAccessions:
    def test_logs_accessions_the_map_does_not_cover(self, caplog) -> None:
        """Accessions the map does not cover are logged. An isoform whose base maps is not one of them."""
        with caplog.at_level(logging.INFO):
            log_unmapped_accessions(
                pd.Series(["Q8C8R3-2", "P00001"]),
                {"Q8C8R3": "ENSMUSG00000000001"},
            )

        assert "1 UniProt IDs absent from the map" in caplog.text
        assert "P00001" in caplog.text

    def test_silent_when_every_accession_maps(self, caplog) -> None:
        """Nothing is logged when every accession is in the map."""
        with caplog.at_level(logging.INFO):
            log_unmapped_accessions(
                pd.Series(["Q8C8R3"]), {"Q8C8R3": "ENSMUSG00000000001"}
            )

        assert caplog.text == ""
