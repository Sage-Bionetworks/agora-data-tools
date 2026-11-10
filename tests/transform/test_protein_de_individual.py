import json
import math
import os
from typing import Any

import pandas as pd
import pytest

from agoradatatools.etl.transform.protein_de_individual import (
    STATIC_INPUTS,
    transform_protein_de_individual,
    _build_output,
    _check_metadata_coverage,
    _collapse_duplicate_measurements,
    _measured_accessions,
    _melt_proteomics_file,
    _validate_file_maps,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    prepare_genotype_label_map,
)


class TestMeasuredAccessions:
    @pytest.mark.parametrize(
        "columns,expected_accessions",
        [
            (
                {
                    "cycs|p00001": [1.0, 2.0],
                    "srp54|p00002": [None, 3.0],
                    "dead|p00003": [None, None],  # should get dropped
                },
                {"P00001", "P00002"},
            ),
            ({"ank2|q8c8r3_2": [1.0, 2.0]}, {"Q8C8R3-2"}),
            ({"cycs|p00001": [1.0, 2.0]}, {"P00001"}),
            (
                {
                    "cycs|p00001": [1.0, 2.0],
                    # different gene symbol, same accession
                    "cycs2|p00001": [1.0, 2.0],
                },
                {"P00001"},
            ),
        ],
    )
    def test_accessions_are_extracted_from_protein_columns(
        self, columns: dict[str, list], expected_accessions: set
    ) -> None:
        """The accessions collected from the column headers should be upper-cased and match the expected output."""
        datasets = {
            "file1": pd.DataFrame({"individualid": [1, 2], **columns}),
            "file2": pd.DataFrame({"individualid": [3, 4], **columns}),
        }

        measured = _measured_accessions(datasets, list(datasets))

        assert set(measured) == expected_accessions
        # measured should not have any duplicates
        assert len(measured) == len(expected_accessions)


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
        data_file = self.data_file.copy().assign(sequencing_batch=["b1", "b2"])

        long_df = _melt_proteomics_file("proteomics_file", data_file, "LOAD2")

        assert set(long_df["uniprotid"]) == {"P00001", "Q8C8R3-2"}
        # sequencing_batch should not be included in the columns.
        assert list(long_df.columns) == [
            "individualid",
            "model",
            "uniprotid",
            "value",
        ]

    def test_no_protein_columns_raises(self) -> None:
        """A file with no gene_symbol|uniprotid columns raises."""
        with pytest.raises(ValueError, match="no protein columns"):
            _melt_proteomics_file(
                "proteomics_file",
                self.data_file[["specimenid", "individualid"]],
                "LOAD2",
            )

    def test_non_numeric_values_are_dropped(self) -> None:
        """Non-numeric abundances are dropped the same way missing values are."""
        data_file = self.data_file.copy().assign(
            **{"gene1|p00001": ["1.0", "not_a_number"]}
        )

        long_df = _melt_proteomics_file("proteomics_file", data_file, "LOAD2")

        assert len(long_df) == 3
        assert long_df["value"].tolist() == [1.0, 2.0, 3.0]

    def test_no_numeric_abundances_raises(self) -> None:
        """A file with only missing or non-numeric abundances raises and names the file."""
        data_file = self.data_file.copy().assign(
            **{"gene1|p00001": [None, "not_a_number"], "ank2|q8c8r3_2": [None, None]}
        )

        with pytest.raises(
            ValueError,
            match="Either every abundance value in proteomics data file 'proteomics_file'",
        ):
            _melt_proteomics_file("proteomics_file", data_file, "LOAD2")


class TestBuildOutput:
    """_build_output joins metadata, derives fields, and nests. Inputs are already melted."""

    uniprot_to_ensembl = {
        "P00001": "ENSMUSG00000000001",
        "Q8C8R3": "ENSMUSG00000000001",
    }
    gene_symbols = {"ENSMUSG00000000001": "Gnai3"}

    @staticmethod
    def _label_map() -> pd.DataFrame:
        """A prepared LOAD2 label map, the shape _build_output receives."""
        return prepare_genotype_label_map(
            pd.DataFrame(
                {
                    "model": ["LOAD2", "LOAD2"],
                    "model_group": ["LOAD2", "LOAD2"],
                    "display_label": ["LOAD2", "LOAD1"],
                    "genotype": ["geno_hom", "geno_wt"],
                    "result_order": [2, 1],
                }
            )
        )

    @staticmethod
    def _metadata(
        individuals: tuple | list = ("i1", "i2"),
        sex: tuple | list = ("male", "female"),
        agedeath: tuple | list = (4.0, 4.5),
        genotype: tuple | list = ("geno_hom", "geno_wt"),
        tissue: Any = "right cerebral hemisphere",
    ) -> pd.DataFrame:
        """Harmonized metadata with the model column already stamped, as the transform passes it."""
        individuals = list(individuals)
        if isinstance(tissue, str):
            tissue = [tissue] * len(individuals)
        return pd.DataFrame(
            {
                "individualid": individuals,
                "sex": list(sex),
                "agedeath": list(agedeath),
                "genotype": list(genotype),
                "tissue": list(tissue),
                "model": ["LOAD2"] * len(individuals),
            }
        )

    @staticmethod
    def _long(
        individuals: tuple | list = ("i1", "i2"),
        uniprotid: Any = "P00001",
        values: tuple | list = (1.0, 2.0),
    ) -> pd.DataFrame:
        """One melted proteomics frame. A single accession is repeated for every individual."""
        individuals = list(individuals)
        if isinstance(uniprotid, str):
            uniprotid = [uniprotid] * len(individuals)
        return pd.DataFrame(
            {
                "individualid": individuals,
                "model": ["LOAD2"] * len(individuals),
                "uniprotid": list(uniprotid),
                "value": list(values),
            }
        )

    def _run(
        self,
        long_df: pd.DataFrame,
        metadata: pd.DataFrame = None,
        uniprot_to_ensembl: dict[str, str] = None,
    ) -> list[dict[str, Any]]:
        """Call _build_output with the suite's static inputs, overriding the melted frame."""
        return _build_output(
            "LOAD2",
            long_df,
            self._metadata() if metadata is None else metadata,
            (
                self.uniprot_to_ensembl
                if uniprot_to_ensembl is None
                else uniprot_to_ensembl
            ),
            self.gene_symbols,
            self._label_map(),
        )

    def test_hyphenated_accession_stays_a_separate_row(self) -> None:
        """An isoform maps on its base accession but stays a separate output row."""
        long_df = pd.concat(
            [
                self._long(uniprotid="Q8C8R3", values=(1.0, 2.0)),
                self._long(uniprotid="Q8C8R3-2", values=(3.0, 4.0)),
            ],
            ignore_index=True,
        )

        by_uniprot = {entry["uniprotid"]: entry for entry in self._run(long_df)}

        assert set(by_uniprot) == {"Q8C8R3", "Q8C8R3-2"}
        isoform = by_uniprot["Q8C8R3-2"]
        assert isoform["ensembl_gene_id"] == "ENSMUSG00000000001"
        assert isoform["unique_id"] == "ENSMUSG00000000001Q8C8R3-2"
        assert isoform["display_symbol"] == "Gnai3 (Q8C8R3-2)"

    def test_unmapped_proteins_dropped(self) -> None:
        """An accession absent from the UniProt map is dropped."""
        long_df = pd.concat(
            [
                self._long(uniprotid="P00001", values=(1.0, 2.0)),
                self._long(uniprotid="NOMAP", values=(3.0, 4.0)),
            ],
            ignore_index=True,
        )

        output = self._run(long_df)

        assert {entry["uniprotid"] for entry in output} == {"P00001"}

    def test_animals_without_metadata_or_label_map_row_dropped(self) -> None:
        """Animals missing metadata or a label-map row are dropped, not a failed run."""
        metadata = self._metadata(
            individuals=("i1", "i2", "i4"),
            sex=("male", "female", "female"),
            agedeath=(4.0, 4.5, 5.1),
            genotype=("geno_hom", "geno_wt", "geno_het"),
        )
        long_df = self._long(
            individuals=("i1", "i2", "i4", "u1", "u2", "u3", "u4"),
            values=(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0),
        )

        output = self._run(long_df, metadata=metadata)

        assert len(output) == 1
        assert {record["individual_id"] for record in output[0]["data"]} == {
            "i1",
            "i2",
        }
        assert {record["genotype"] for record in output[0]["data"]} == {
            "LOAD2",
            "LOAD1",
        }

    def test_age_bucketing_boundaries(self) -> None:
        """ageDeath is bucketed on the right-closed JAX thresholds."""
        ages = [6.0, 6.1, 10.0, 14.2, 16.0, 16.1, 20.0, 20.1]
        expected = [4, 8, 8, 12, 12, 18, 18, 24]
        individuals = [f"i{n}" for n in range(len(ages))]
        metadata = self._metadata(
            individuals=individuals,
            sex=["male"] * len(ages),
            agedeath=ages,
            genotype=["geno_hom"] * len(ages),
        )
        long_df = self._long(
            individuals=individuals, values=[float(n) for n in range(len(ages))]
        )

        output = self._run(long_df, metadata=metadata)

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
        metadata = self._metadata(agedeath=(4.0, None))

        with pytest.raises(ValueError, match="unbucketable ageDeath.*i2"):
            self._run(self._long(), metadata=metadata)

    @pytest.mark.parametrize("tissue", [None, ""])
    def test_missing_tissue_raises(self, tissue: Any) -> None:
        """A blank or missing tissue raises."""
        metadata = self._metadata(tissue=["right cerebral hemisphere", tissue])

        with pytest.raises(ValueError, match="Missing tissue.*i2"):
            self._run(self._long(), metadata=metadata)

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
        output = self._run(self._long(), metadata=self._metadata(sex=source))

        assert {record["sex"] for record in output[0]["data"]} == expected

    def test_all_null_sex_survives(self) -> None:
        """An all-missing sex column does not raise."""
        output = self._run(self._long(), metadata=self._metadata(sex=(None, None)))

        assert {record["sex"] for record in output[0]["data"]} == {None}

    def test_negative_zero_is_normalized(self) -> None:
        """Small negatives and signed zero serialize as 0.0."""
        values = [
            record["value"]
            for record in self._run(self._long(values=(-0.000001, -0.0)))[0]["data"]
        ]

        assert values == [0.0, 0.0]
        assert not any(math.copysign(1, value) < 0 for value in values)

    def test_metadata_files_disagreeing_about_an_animal_raise(self) -> None:
        """Two metadata rows that disagree about one animal raise on the join."""
        metadata = pd.DataFrame(
            {
                "individualid": ["i1", "i1"],
                "sex": ["male", "female"],
                "agedeath": [4.0, 24.0],
                "genotype": ["geno_hom", "geno_wt"],
                "tissue": ["right cerebral hemisphere", "Cortex"],
                "model": ["LOAD2", "LOAD2"],
            }
        )

        with pytest.raises(ValueError, match="not a many-to-one merge"):
            self._run(self._long(individuals=("i1",), values=(1.0,)), metadata=metadata)

    def test_disagreement_about_an_excluded_animal_is_ignored(self) -> None:
        """Two files disagreeing about an animal dropped for its genotype do not raise."""
        metadata = self._metadata(
            individuals=("i1", "i2", "i3"),
            sex=("male", "female", "male"),
            agedeath=(4.0, 4.5, 4.2),
            genotype=("geno_hom", "geno_wt", "geno_excluded"),
        )
        long_df = pd.concat(
            [
                self._long(),
                self._long(individuals=("i3", "i3"), values=(3.0, 9.0)),
            ],
            ignore_index=True,
        )

        output = self._run(long_df, metadata=metadata)

        assert [record["individual_id"] for record in output[0]["data"]] == [
            "i1",
            "i2",
        ]


class TestCollapseDuplicateMeasurements:
    def test_animal_measured_in_two_files_appears_once(self) -> None:
        """An animal measured for one protein in two files is kept once, not twice."""
        measured = pd.DataFrame(
            {
                "individualid": ["i1", "i2", "i1"],
                "uniprotid": ["P00001", "P00001", "P00001"],
                "value": [1.0, 2.0, 1.0],
            }
        )

        collapsed = _collapse_duplicate_measurements("LOAD2", measured)

        assert collapsed["individualid"].tolist() == ["i1", "i2"]
        assert collapsed["value"].tolist() == [1.0, 2.0]

    def test_two_files_disagreeing_about_a_measurement_raise(self) -> None:
        """Two files reporting different abundances for one animal and protein raise."""
        measured = pd.DataFrame(
            {
                "individualid": ["i1", "i2", "i1"],
                "uniprotid": ["P00001", "P00001", "P00001"],
                "value": [1.0, 2.0, 9.0],
            }
        )

        with pytest.raises(ValueError, match="disagree about the abundance"):
            _collapse_duplicate_measurements("LOAD2", measured)


class TestCheckMetadataCoverage:
    def test_unjoinable_data_file_raises(self) -> None:
        """A data file whose individualIDs match no metadata raises."""
        with pytest.raises(
            ValueError,
            match="None of the 2 animals in proteomics data file 'stale_file'",
        ):
            _check_metadata_coverage(
                "stale_file",
                pd.Series(["i1.0", "i2.0"]),
                "LOAD2",
                {("i1", "LOAD2"), ("i2", "LOAD2")},
            )


class TestValidateFileMaps:
    known_models = {"LOAD2"}

    @staticmethod
    def _datasets() -> dict[str, pd.DataFrame]:
        """Empty frames named like a valid dataset. _validate_file_maps only checks names."""
        return {
            **{name: pd.DataFrame() for name in STATIC_INPUTS},
            "proteomics_file": pd.DataFrame(),
            "load2_harmonized_metadata": pd.DataFrame(),
        }

    def test_valid_maps_do_not_raise(self) -> None:
        """Claimed files and known models pass."""
        _validate_file_maps(
            self._datasets(),
            {"proteomics_file": "LOAD2"},
            {"load2_harmonized_metadata": "LOAD2"},
            STATIC_INPUTS,
            self.known_models,
        )

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
        with pytest.raises(ValueError, match=error):
            _validate_file_maps(
                self._datasets(),
                model_map,
                {"load2_harmonized_metadata": "LOAD2"},
                STATIC_INPUTS,
                self.known_models,
            )

    @pytest.mark.parametrize(
        "metadata_map,error",
        [
            (None, "No metadata_map provided"),
            ({"typo_metadata": "LOAD2"}, "not metadata files"),
            (
                {"load2_harmonized_metadata": "LOAD3"},
                "absent from the genotype label map",
            ),
        ],
    )
    def test_invalid_metadata_map_raises(
        self, metadata_map: dict[str, str], error: str
    ) -> None:
        """A missing, typo'd, or unlabeled metadata_map raises."""
        with pytest.raises(ValueError, match=error):
            _validate_file_maps(
                self._datasets(),
                {"proteomics_file": "LOAD2"},
                metadata_map,
                STATIC_INPUTS,
                self.known_models,
            )

    def test_file_in_both_maps_raises(self) -> None:
        """A file listed in both model_map and metadata_map raises."""
        with pytest.raises(ValueError, match="both model_map and metadata_map"):
            _validate_file_maps(
                self._datasets(),
                {
                    "proteomics_file": "LOAD2",
                    "load2_harmonized_metadata": "LOAD2",
                },
                {"load2_harmonized_metadata": "LOAD2"},
                STATIC_INPUTS,
                self.known_models,
            )

    def test_data_file_missing_from_model_map_raises(self) -> None:
        """A proteomics file left out of both maps raises."""
        datasets = self._datasets()
        datasets["second_proteomics_file"] = pd.DataFrame()

        with pytest.raises(
            ValueError, match="not in model_map or metadata_map"
        ) as excinfo:
            _validate_file_maps(
                datasets,
                {"proteomics_file": "LOAD2"},
                {"load2_harmonized_metadata": "LOAD2"},
                STATIC_INPUTS,
                self.known_models,
            )

        assert "second_proteomics_file" in str(excinfo.value)


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
        datasets: dict[str, pd.DataFrame],
        model_map: dict[str, str] = None,
        metadata_map: dict[str, str] = None,
    ) -> list[dict[str, Any]]:
        """Run the transform, defaulting unnamed data files and metadata files to LOAD2."""
        if model_map is None:
            model_map = {
                key: "LOAD2"
                for key in datasets
                if key not in STATIC_INPUTS and not key.endswith("harmonized_metadata")
            }
        if metadata_map is None:
            metadata_map = {
                key: "LOAD2" for key in datasets if key.endswith("harmonized_metadata")
            }
        return transform_protein_de_individual(
            datasets=datasets, model_map=model_map, metadata_map=metadata_map
        )

    @classmethod
    def _read_input(cls, name: str) -> pd.DataFrame:
        """Read one fixture from the protein_de_individual input directory."""
        path = os.path.join(cls.data_files_path, "input", name)
        if name.endswith(".json"):
            return pd.read_json(path)
        return pd.read_csv(path)

    @classmethod
    def _load_datasets(cls) -> dict[str, pd.DataFrame]:
        """The shared reference inputs, including the basic proteomics file and its metadata."""
        return {
            "genotype_label_map": cls._read_input("synthetic_genotype_label_map.csv"),
            "mouse_gene_metadata": cls._read_input(
                "synthetic_mouse_gene_metadata.json"
            ),
            "load2_harmonized_metadata": cls._read_input(
                "synthetic_harmonized_metadata.csv"
            ),
            "uniprot_ensembl_map": cls._read_input("synthetic_uniprot_ensembl_map.csv"),
            "synthetic_basic_data": cls._read_input("synthetic_basic_data.csv"),
        }

    @classmethod
    def _load_reference_datasets(cls) -> dict[str, pd.DataFrame]:
        """Reference inputs without a proteomics file or study metadata, for tests that supply their own."""
        datasets = cls._load_datasets()
        del datasets["synthetic_basic_data"]
        del datasets["load2_harmonized_metadata"]
        return datasets

    def test_synthetic_basic_data(self) -> None:
        """Happy path matches the golden fixture output."""
        with open(
            os.path.join(self.data_files_path, "output", "synthetic_basic_output.json")
        ) as f:
            expected = json.load(f)

        output = self._transform(self._load_datasets())

        assert self._normalize(output) == self._normalize(expected)

    def test_multiple_data_files_are_combined(self) -> None:
        """Measurements from two data files in one model_group are combined."""
        datasets = self._load_reference_datasets()
        datasets["load2_harmonized_metadata"] = self._read_input(
            "synthetic_two_file_metadata.csv"
        )
        datasets["synthetic_two_file_4mo"] = self._read_input(
            "synthetic_two_file_4mo.csv"
        )
        datasets["synthetic_two_file_24mo"] = self._read_input(
            "synthetic_two_file_24mo.csv"
        )

        output = self._transform(datasets)

        assert {entry["age"] for entry in output} == {"4 months", "24 months"}

    def test_per_model_group_fields_are_not_shared_across_groups(self) -> None:
        """name, matched_control, and result_order are resolved per model_group."""
        datasets = self._load_reference_datasets()
        datasets["genotype_label_map"] = self._read_input(
            "synthetic_multi_model_group_label_map.csv"
        )
        datasets["load2_harmonized_metadata"] = self._read_input(
            "synthetic_multi_model_load2_metadata.csv"
        )
        datasets["bin1_harmonized_metadata"] = self._read_input(
            "synthetic_multi_model_bin1_metadata.csv"
        )
        datasets["bin1_fad_harmonized_metadata"] = self._read_input(
            "synthetic_multi_model_bin1_fad_metadata.csv"
        )
        datasets["synthetic_multi_model_load2_data"] = self._read_input(
            "synthetic_multi_model_load2_data.csv"
        )
        datasets["synthetic_multi_model_bin1_data"] = self._read_input(
            "synthetic_multi_model_bin1_data.csv"
        )
        datasets["synthetic_multi_model_bin1_fad_data"] = self._read_input(
            "synthetic_multi_model_bin1_fad_data.csv"
        )

        output = self._transform(
            datasets,
            model_map={
                "synthetic_multi_model_load2_data": "LOAD2",
                "synthetic_multi_model_bin1_data": "Bin1-K358R",
                "synthetic_multi_model_bin1_fad_data": "Bin1-K358R.5xFAD",
            },
            metadata_map={
                "load2_harmonized_metadata": "LOAD2",
                "bin1_harmonized_metadata": "Bin1-K358R",
                "bin1_fad_harmonized_metadata": "Bin1-K358R.5xFAD",
            },
        )

        by_group = {entry["model_group"]: entry for entry in output}
        assert set(by_group) == {"LOAD2", "Bin1K358R"}

        load2 = by_group["LOAD2"]
        assert load2["name"] == "LOAD2"
        assert load2["matched_control"] == "LOAD1"
        assert load2["result_order"] == ["LOAD1", "LOAD2"]
        assert {record["individual_id"] for record in load2["data"]} == {"i1", "i2"}

        bin1 = by_group["Bin1K358R"]
        assert bin1["name"] == "Bin1K358R"
        assert bin1["matched_control"] == "C57BL/6J"
        assert bin1["result_order"] == [
            "C57BL/6J",
            "Bin1K358R",
            "5xFAD",
            "Bin1K358R.5xFAD",
        ]
        assert {record["individual_id"] for record in bin1["data"]} == {
            "i3",
            "i4",
            "i5",
            "i6",
        }

    def test_second_study_metadata_file_is_combined(self) -> None:
        """A second metadata file is concatenated with the first."""
        datasets = self._load_datasets()
        datasets["load2_harmonized_metadata"] = self._read_input(
            "synthetic_second_study_primary_metadata.csv"
        )
        datasets["uci_harmonized_metadata"] = self._read_input(
            "synthetic_second_study_metadata.csv"
        )

        output = self._transform(datasets)

        assert {entry["tissue"] for entry in output} == {"Hemibrain", "Cortex"}
        assert {
            record["individual_id"] for entry in output for record in entry["data"]
        } == {"i1", "i2"}

    def test_metadata_files_agreeing_about_an_animal_are_deduplicated(self) -> None:
        """Identical animal rows across metadata files collapse to one.

        The second file is cast to string individualIDs after reading, because one source
        arrives as int64 and the other as object and the two must still collapse.
        """
        datasets = self._load_reference_datasets()
        datasets["load2_harmonized_metadata"] = self._read_input(
            "synthetic_duplicate_metadata_primary.csv"
        )
        datasets["synthetic_duplicate_metadata_data"] = self._read_input(
            "synthetic_duplicate_metadata_data.csv"
        )
        second = self._read_input("synthetic_duplicate_metadata.csv")
        second["individualid"] = second["individualid"].astype(str)
        datasets["uci_harmonized_metadata"] = second

        output = self._transform(datasets)

        assert [record["individual_id"] for record in output[0]["data"]] == [
            "51503",
            "51504",
        ]

    def test_duplicate_animal_rows_are_tolerated(self) -> None:
        """Identical duplicate metadata rows for one animal do not fan out the merge."""
        datasets = self._load_datasets()
        datasets["load2_harmonized_metadata"] = self._read_input(
            "synthetic_duplicate_animal_metadata.csv"
        )

        output = self._transform(datasets)

        assert [record["individual_id"] for record in output[0]["data"]] == [
            "i1",
            "i2",
        ]

    def test_unmatched_genotypes_raises(self) -> None:
        """Genotypes that never appear in the label map leave no rows after the join."""
        datasets = self._load_datasets()
        datasets["load2_harmonized_metadata"]["genotype"] = "unknown"
        with pytest.raises(ValueError, match="No rows remained"):
            self._transform(datasets)
