import json
import math
import os
from typing import Any

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_individual import (
    REQUIRED_INPUT,
    transform_protein_de_individual,
    _build_uniprot_candidates,
    _measured_header_pairs,
    _melt_proteomics_file,
    _resolve_gene_ids,
)


CANDIDATES = {"P1": ["ENSMUSG00000000001", "ENSMUSG00000000009"]}
GENE_SYMBOLS = {"ENSMUSG00000000001": "Gm10053", "ENSMUSG00000000009": "Cycs"}


class TestBuildUniprotCandidates:
    def test_drops_human_genes_and_keeps_all_mouse_candidates(self) -> None:
        """Human Ensembl ids are dropped; mouse candidates are kept and sorted."""
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

        assert _build_uniprot_candidates(mapping) == {
            "P1": ["ENSMUSG00000000002", "ENSMUSG00000000005"],
            "P3": ["ENSMUSG00000000003"],
        }


class TestMeasuredHeaderPairs:
    @staticmethod
    def _melted_accessions(datasets: dict[str, pd.DataFrame]) -> set:
        """Return the UniProt accessions produced by melting each data file."""
        return {
            accession
            for name, data_file in datasets.items()
            for accession in _melt_proteomics_file(name, data_file, "LOAD2")[
                "uniprotid"
            ]
        }

    @pytest.mark.parametrize(
        "columns,accessions,symbols",
        [
            (
                {
                    "cycs|p00001": [1.0, 2.0],
                    "srp54|p00002": [None, 3.0],
                    "dead|p00003": [None, None],
                },
                {"P00001", "P00002"},
                {"cycs", "srp54"},
            ),
            ({"ank2|q8c8r3_2": [1.0, 2.0]}, {"Q8C8R3-2"}, {"ank2"}),
            ({"cycs|p00001": [1.0, 2.0]}, {"P00001"}, {"cycs"}),
        ],
    )
    def test_pairs_match_what_the_melt_yields(
        self, columns: dict[str, list], accessions: set, symbols: set
    ) -> None:
        """Header pairs match the accessions and symbols the melt would yield."""
        datasets = {
            "file1": pd.DataFrame({"individualid": [1, 2], **columns}),
            "file2": pd.DataFrame({"individualid": [3, 4], **columns}),
        }

        pairs = _measured_header_pairs(datasets, list(datasets))

        assert set(pairs["uniprotid"]) == self._melted_accessions(datasets)
        assert set(pairs["uniprotid"]) == accessions
        assert set(pairs["header_symbol"]) == symbols
        assert len(pairs) == len(accessions)

    def test_symbols_are_unioned_across_files(self) -> None:
        """Header symbols for one accession are unioned across data files."""
        datasets = {
            "file1": pd.DataFrame({"individualid": [1], "na|p00001": [1.0]}),
            "file2": pd.DataFrame({"individualid": [2], "cycs|p00001": [2.0]}),
        }

        pairs = _measured_header_pairs(datasets, list(datasets))

        assert set(pairs["header_symbol"]) == {"na", "cycs"}
        assert _resolve_gene_ids(pairs, {"P00001": CANDIDATES["P1"]}, GENE_SYMBOLS) == {
            "P00001": "ENSMUSG00000000009"
        }

    def test_dead_isoform_column_cannot_steer_its_base_accession(self) -> None:
        """An all-empty isoform column does not change the gene of its base accession."""
        datasets = {
            "file1": pd.DataFrame(
                {
                    "individualid": [1, 2],
                    "cycs|p00001": [1.0, 2.0],
                    "gm10053|p00001_2": [None, None],
                }
            )
        }

        pairs = _measured_header_pairs(datasets, list(datasets))

        assert set(pairs["uniprotid"]) == self._melted_accessions(datasets)
        assert _resolve_gene_ids(pairs, {"P00001": CANDIDATES["P1"]}, GENE_SYMBOLS) == {
            "P00001": "ENSMUSG00000000009"
        }


class TestResolveGeneIds:
    @staticmethod
    def _header_pairs(uniprotid: str, header_symbol: str) -> pd.DataFrame:
        """Build a one-row header-pairs frame for resolve tests."""
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
        """The header symbol picks among map candidates, else the smallest Ensembl id is used."""
        resolved = _resolve_gene_ids(
            self._header_pairs("P1", header_symbol),
            CANDIDATES,
            GENE_SYMBOLS,
        )

        assert resolved == {"P1": expected}

    def test_multi_gene_header_symbol_resolves_on_one_match(self) -> None:
        """A multi-gene header resolves when exactly one named gene is a candidate."""
        resolved = _resolve_gene_ids(
            self._header_pairs("P1", "Cycs;_Rps27"),
            CANDIDATES,
            GENE_SYMBOLS,
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}

    def test_ambiguous_match_stays_within_the_named_genes(self) -> None:
        """A tie between named genes is broken without leaving those named genes."""
        resolved = _resolve_gene_ids(
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
        )

        assert resolved == {"P3": "ENSMUSG00000000004"}

    def test_isoform_symbol_resolves_base_accession(self) -> None:
        """An isoform header symbol can resolve the base accession."""
        resolved = _resolve_gene_ids(
            self._header_pairs("P1-2", "Cycs"), CANDIDATES, GENE_SYMBOLS
        )

        assert resolved == {"P1": "ENSMUSG00000000009"}


class TestMeltProteomicsFile:
    data_file = pd.DataFrame(
        {
            "specimenid": ["c1", "c2"],
            "individualid": [51503, 51504],
            "gene1|p00001": [1.0, None],
            "ank2|q8c8r3_2": [2.0, 3.0],
        }
    )

    def test_melts_protein_columns_and_recovers_isoform_accessions(self) -> None:
        """Melt keeps protein columns, recovers hyphenated accessions, and casts individualid."""
        long_df = _melt_proteomics_file("proteomics_file", self.data_file, "LOAD2")

        assert list(long_df.columns) == [
            "individualid",
            "model",
            "uniprotid",
            "value",
        ]
        assert len(long_df) == 3
        assert set(long_df["uniprotid"]) == {"P00001", "Q8C8R3-2"}
        assert long_df["individualid"].tolist() == ["51503", "51503", "51504"]
        assert set(long_df["model"]) == {"LOAD2"}

    def test_metadata_only_columns_are_not_melted(self) -> None:
        """Columns without a pipe are not treated as proteins."""
        data_file = self.data_file.assign(sequencing_batch=["b1", "b2"])

        long_df = _melt_proteomics_file("proteomics_file", data_file, "LOAD2")

        assert set(long_df["uniprotid"]) == {"P00001", "Q8C8R3-2"}

    def test_no_protein_columns_raises(self) -> None:
        """A file with no gene_symbol|uniprotid columns raises."""
        with pytest.raises(ValueError, match="no protein columns"):
            _melt_proteomics_file(
                "proteomics_file",
                self.data_file[["specimenid", "individualid"]],
                "LOAD2",
            )

    def test_non_numeric_value_names_its_file(self) -> None:
        """Non-numeric abundances raise and name the file they came from."""
        data_file = self.data_file.assign(**{"gene1|p00001": ["1.0", "not_a_number"]})

        with pytest.raises(ValueError, match="'proteomics_file'.*not_a_number"):
            _melt_proteomics_file("proteomics_file", data_file, "LOAD2")


class TestTransformProteinDeIndividual:
    data_files_path = "tests/test_assets/protein_de_individual"

    @staticmethod
    def _normalize(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Sort inner data lists and the outer list for order-independent comparison."""
        for entry in entries:
            entry["data"] = sorted(entry["data"], key=lambda x: x["individual_id"])
        return sorted(entries, key=lambda x: (x["unique_id"], x["age"]))

    @staticmethod
    def _transform(
        datasets: dict[str, pd.DataFrame], model_map: dict[str, str] = None
    ) -> list[dict[str, Any]]:
        """Run the transform, defaulting unnamed data files to LOAD2."""
        if model_map is None:
            model_map = {
                key: "LOAD2"
                for key in datasets
                if key not in REQUIRED_INPUT and not key.endswith("harmonized_metadata")
            }
        return transform_protein_de_individual(datasets=datasets, model_map=model_map)

    def _build_datasets(
        self,
        harmonized: dict[str, Any] = None,
        data_file: dict[str, Any] = None,
        data_key: str = "proteomics_file",
        label_map: dict[str, Any] = None,
        mapping: pd.DataFrame = None,
        gene_metadata: pd.DataFrame = None,
    ) -> dict[str, pd.DataFrame]:
        """Build a minimal valid datasets dict, allowing any input to be replaced."""
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
                        "uniprot_id": ["P00001"],
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
        """Happy path matches the golden fixture output."""
        input_path = os.path.join(self.data_files_path, "input")
        datasets = {
            "genotype_label_map": pd.read_csv(
                os.path.join(input_path, "synthetic_genotype_label_map.csv")
            ),
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

    def test_hyphenated_accession_stays_a_separate_row(self) -> None:
        """An isoform maps on its base accession but stays a separate output row."""
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "ank2|q8c8r3": [1.0, 2.0],
                "ank2|q8c8r3_2": [3.0, 4.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["Q8C8R3"],
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
        """Unmapped and human proteins are dropped from the output."""
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
                    "uniprot_id": ["P00001", "HUMANP"],
                    "ensembl_gene_id": ["ENSMUSG00000000001", "ENSG00000000001"],
                }
            ),
        )

        output = self._transform(datasets)

        assert {e["uniprotid"] for e in output} == {"P00001"}

    def test_header_symbol_cannot_rescue_accession_absent_from_the_map(self) -> None:
        """A header naming a known gene does not invent a mapping for an absent accession."""
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "gene1|p00001": [1.0, 2.0],
                "Gnai3|nomap": [3.0, 4.0],
            }
        )

        output = self._transform(datasets)

        assert {e["uniprotid"] for e in output} == {"P00001"}

    def test_header_picks_among_map_candidates(self) -> None:
        """The named candidate wins even when it is not the smallest Ensembl id."""
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "Pms2|p54279": [1.0, 2.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["P54279", "P54279"],
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000009",
                    ],
                }
            ),
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000009",
                    ],
                    "gene_symbol": ["Rsph10b", "Pms2"],
                }
            ),
        )

        output = self._transform(datasets)

        assert output[0]["ensembl_gene_id"] == "ENSMUSG00000000009"
        assert output[0]["gene_symbol"] == "Pms2"

    def test_unmatched_header_falls_back_to_smallest_ensembl_id(self) -> None:
        """An unmatched header falls back to the smallest candidate Ensembl id."""
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "unknown|p54279": [1.0, 2.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["P54279", "P54279"],
                    "ensembl_gene_id": [
                        "ENSMUSG00000000009",
                        "ENSMUSG00000000001",
                    ],
                }
            ),
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000009",
                    ],
                    "gene_symbol": ["Rsph10b", "Pms2"],
                }
            ),
        )

        output = self._transform(datasets)

        assert output[0]["ensembl_gene_id"] == "ENSMUSG00000000001"

    def test_extra_map_column_is_ignored(self) -> None:
        """Extra columns on the UniProt map are ignored."""
        datasets = self._build_datasets(
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["P00001"],
                    "ensembl_gene_id": ["ENSMUSG00000000001"],
                    "optional_information": [""],
                }
            )
        )

        output = self._transform(datasets)

        assert {e["uniprotid"] for e in output} == {"P00001"}

    def test_isoform_map_row_is_preferred_over_base(self) -> None:
        """A mapping-file row for the full accession wins over the base accession."""
        datasets = self._build_datasets(
            data_file={
                "specimenid": ["c1", "c2"],
                "individualid": ["i1", "i2"],
                "ank2|q8c8r3": [1.0, 2.0],
                "ank2|q8c8r3_2": [3.0, 4.0],
            },
            mapping=pd.DataFrame(
                {
                    "uniprot_id": ["Q8C8R3", "Q8C8R3-2"],
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000002",
                    ],
                }
            ),
            gene_metadata=pd.DataFrame(
                {
                    "ensembl_gene_id": [
                        "ENSMUSG00000000001",
                        "ENSMUSG00000000002",
                    ],
                    "gene_symbol": ["Gnai3", "Cdc45"],
                }
            ),
        )

        by_uniprot = {e["uniprotid"]: e for e in self._transform(datasets)}

        assert by_uniprot["Q8C8R3"]["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert by_uniprot["Q8C8R3-2"]["ensembl_gene_id"] == "ENSMUSG00000000002"
        assert by_uniprot["Q8C8R3-2"]["unique_id"] == "ENSMUSG00000000002Q8C8R3-2"

    def test_animals_without_metadata_or_label_map_row_dropped(self) -> None:
        """Animals missing metadata or a label-map row are dropped, not a failed run."""
        datasets = self._build_datasets(
            harmonized={
                "individualid": ["i1", "i2", "i4"],
                "sex": ["male", "female", "female"],
                "agedeath": [4.0, 4.5, 5.1],
                "genotype": ["geno_hom", "geno_wt", "geno_het"],
                "tissue": ["right cerebral hemisphere"] * 3,
            },
            data_file={
                "specimenid": [f"c{n}" for n in range(7)],
                "individualid": ["i1", "i2", "i4", "u1", "u2", "u3", "u4"],
                "gene1|p00001": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            },
        )

        output = self._transform(datasets)

        assert len(output) == 1
        assert {d["individual_id"] for d in output[0]["data"]} == {"i1", "i2"}
        assert {d["genotype"] for d in output[0]["data"]} == {"LOAD2", "LOAD1"}

    def test_unjoinable_data_file_raises(self) -> None:
        """A data file whose individualIDs match no metadata raises."""
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
            ValueError,
            match="None of the 2 animals in proteomics data file 'stale_file'",
        ):
            self._transform(datasets)

    def test_age_bucketing_boundaries(self) -> None:
        """ageDeath is bucketed on the right-closed JAX thresholds."""
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
        """A missing ageDeath raises instead of dropping the animal silently."""
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
        """A blank or missing tissue raises."""
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
        self, source: list[str], expected: set
    ) -> None:
        """Plural and lowercase sex labels are mapped to singular title case."""
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
        """An all-missing sex column does not raise."""
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
        """Small negatives and signed zero serialize as 0.0."""
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
        """Measurements from two data files in one model_group are combined."""
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
        """name, matched_control, and result_order are resolved per model_group."""
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
        """Identical duplicate metadata rows for one animal do not fan out the merge."""
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

    def test_duplicate_model_genotype_in_label_map_raises(self) -> None:
        """Two label-map rows for the same model and genotype raise."""
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
            (None, "No model_map provided"),
            (
                {"proteomics_file": "LOAD2", "typo_file": "LOAD2"},
                "not proteomics data files",
            ),
            ({"proteomics_file": "LOAD3"}, "absent from the genotype label map"),
        ],
    )
    def test_invalid_model_map_raises(
        self, model_map: dict[str, str], error: str
    ) -> None:
        """A missing, typo'd, or unlabeled model_map raises."""
        datasets = self._build_datasets()

        with pytest.raises(ValueError, match=error):
            transform_protein_de_individual(datasets=datasets, model_map=model_map)

    def test_data_file_missing_from_model_map_raises(self) -> None:
        """A proteomics file left out of model_map is not read as metadata."""
        datasets = self._build_datasets()
        datasets["second_proteomics_file"] = datasets["proteomics_file"]

        with pytest.raises(ValueError, match="not in model_map") as excinfo:
            self._transform(datasets, model_map={"proteomics_file": "LOAD2"})

        assert "second_proteomics_file" in str(excinfo.value)

    def test_second_study_metadata_file_is_combined(self) -> None:
        """A second leftover metadata file is concatenated with the first."""
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
        """Two metadata files that disagree about one animal raise."""
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
        """Identical animal rows across metadata files collapse to one."""
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

    @pytest.mark.parametrize(
        "mutate,error",
        [
            (lambda d: d.pop("uniprot_ensembl_map"), "Missing required datasets"),
            (
                lambda d: d.update(
                    {
                        "load2_harmonized_metadata": d[
                            "load2_harmonized_metadata"
                        ].drop(columns=["agedeath"])
                    }
                ),
                "Missing required columns",
            ),
            (
                lambda d: d["load2_harmonized_metadata"].__setitem__(
                    "genotype", ["", "geno_wt"]
                ),
                "not_empty",
            ),
            (
                lambda d: d.update(
                    {
                        "proteomics_file": d["proteomics_file"].drop(
                            columns=["individualid"]
                        )
                    }
                ),
                "Missing required columns",
            ),
            (
                lambda d: d.update({"proteomics_file": d["proteomics_file"].iloc[:0]}),
                "is empty",
            ),
            (
                lambda d: d["genotype_label_map"].__setitem__(
                    "display_label", ["", "LOAD1"]
                ),
                "not_empty",
            ),
            (
                lambda d: d["load2_harmonized_metadata"].__setitem__(
                    "genotype", ["unknown", "unknown"]
                ),
                "No rows remained",
            ),
        ],
        ids=[
            "drop_required_dataset",
            "drop_metadata_column",
            "empty_metadata_genotype",
            "drop_data_file_id_column",
            "empty_data_file",
            "empty_display_label",
            "unmatched_genotypes",
        ],
    )
    def test_invalid_input_raises(self, mutate, error: str) -> None:
        """Missing datasets, columns, empty files, and unmatched genotypes raise."""
        datasets = self._build_datasets()
        mutate(datasets)
        with pytest.raises(ValueError, match=error):
            self._transform(datasets)
