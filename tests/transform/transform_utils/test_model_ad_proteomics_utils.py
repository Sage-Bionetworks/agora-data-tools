"""Tests for the shared Model AD proteomics identity helpers."""

import pandas as pd

from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    build_uniprot_to_ensembl,
    canonical_accession,
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
