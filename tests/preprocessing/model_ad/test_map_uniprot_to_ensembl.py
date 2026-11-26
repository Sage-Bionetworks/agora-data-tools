"""
Unit tests for the map_uniprot_to_ensembl preprocessing code. This code relies on queries to an external API (MyGene)
and expects proteomics files to be formatted in a specific way. The unit tests in this file verify that the helper
functions work correctly with the MyGene response and the expected file formats, and test a patched version of the main
function end-to-end.
"""

from unittest.mock import ANY, patch

import numpy as np
import pandas as pd
import pytest

from preprocessing.model_ad import map_uniprot_to_ensembl
from preprocessing.model_ad.map_uniprot_to_ensembl import (
    _extract_ensembl_ids,
    extract_ids_from_file,
    format_mygene_results,
    format_uniprot_ids,
    get_proteomics_file_syn_ids,
    map_accessions,
)


class TestMapUniprotToEnsembl:
    """
    End-to-end test of the map_uniprot_to_ensembl functionality. All calls to external services or on-disk file
    operations are patched. This class tests that the process runs in full on good data, and also tests for errors that
    should raise in the main `map_accessions` function.

    Errors that are raised in helper functions are unit tested elsewhere. It is assumed that if none of the helper
    functions raise errors, then the following is true and do not need testing in this class:
      * There is at least one Synapse ID for each of the proteomics datasets
      * There is at least one valid UniProt ID that was extracted from the proteomics files
      * MyGene returned a response in the expected format, with at least one valid UniProt -> Ensembl mapping
    """

    # Constants to make sure all of the mock inputs and expected outputs have consistent values
    _UNIPROT_IDS = {
        "P12345": "ENSMUSG01",
        "P67890": "ENSMUSG02",
        "Q12345": "ENSMUSG03",
        "Q67890": "ENSMUSG04",
    }
    _SYN_IDS = {
        "ind_file": "synInd.1",
        "agg_file": "synAgg.2",
        "upload_id": "synUpload.1",
    }
    _STAGING_PATH = "/tmp/staging"

    @pytest.fixture(autouse=True)
    def setup_method(self):
        # Doesn't need a return value
        self.patch_syn_login = patch.object(
            map_uniprot_to_ensembl.synapseclient.Synapse, "login"
        ).start()
        # Return value determined by the mock function
        self.patch_get_entity_as_df = patch.object(
            map_uniprot_to_ensembl.extract,
            "get_entity_as_df",
            side_effect=self._get_entity_as_df_mock,
        ).start()
        # Always return the _simple_config() output
        self.patch_get_config = patch.object(
            map_uniprot_to_ensembl.utils,
            "_get_config",
            return_value=self._simple_config(),
        ).start()
        # Doesn't need a return value
        self.patch_create_temp_location = patch.object(
            map_uniprot_to_ensembl.load, "create_temp_location"
        ).start()
        # Always return the _mygene_response() output
        self.patch_query_many = patch.object(
            map_uniprot_to_ensembl.mygene.MyGeneInfo,
            "querymany",
            return_value=self._mygene_response(),
        ).start()
        # Always return "/tmp/staging/file.csv"
        self.patch_df_to_csv = patch.object(
            map_uniprot_to_ensembl.load,
            "df_to_csv",
            return_value=f"{self._STAGING_PATH}/file.csv",
        ).start()
        # Always return ("syn1234", 5)
        self.patch_load = patch.object(
            map_uniprot_to_ensembl.load, "load", return_value=("syn1234", 5)
        ).start()
        yield
        patch.stopall()

    def _simple_config(self):
        """
        Creates a barebones config file with only the fields needed for testing.
        """
        return {
            "staging_path": self._STAGING_PATH,
            "files": [
                {"name": "ind_file", "id": self._SYN_IDS["ind_file"]},
                {"name": "agg_file", "id": self._SYN_IDS["agg_file"]},
            ],
            "datasets": [
                {
                    "protein_de_individual": {
                        "model_map": {"ind_file": "LOAD2"},
                    },
                    "protein_de_aggregate": {
                        "file_metadata": {"agg_file": {"model": "LOAD2"}}
                    },
                }
            ],
        }

    def _mygene_response(self):
        """
        Creates a mock response from MyGeneInfo for the given UniProt to Ensembl mapping.
        """
        return {
            "out": [
                {"query": uniprot, "ensembl": {"gene": ensembl}}
                for uniprot, ensembl in self._UNIPROT_IDS.items()
            ]
        }

    def _get_entity_as_df_mock(self, syn_id, source, syn):
        """
        Mock for get_entity_as_df. Returns an individual- or aggregate-formatted DataFrame based on the synapse ID.
        """
        # Create the ID strings for the mock DataFrame columns
        uniprot_ids = list(self._UNIPROT_IDS.keys())

        # Individual file
        if syn_id == self._SYN_IDS["ind_file"]:
            return pd.DataFrame(
                {
                    "individualID": ["ind1"],
                    "specimenID": ["spec1"],
                    f"Gene1|{uniprot_ids[0]}": [1],
                    f"Gene2|{uniprot_ids[1]}": [2],
                }
            )
        # Aggregate file
        if syn_id == self._SYN_IDS["agg_file"]:
            return pd.DataFrame(
                {
                    "Protein ID": [
                        f"Gene3|{uniprot_ids[2]}",
                        f"Gene4|{uniprot_ids[3]}",
                    ],
                    "Value": [1, 2],
                }
            )
        return pd.DataFrame()

    def test_map_accessions_passes(self):
        """
        End-to-end test for the map_accessions function to ensure it correctly maps UniProt IDs to Ensembl gene IDs and
        interacts with Synapse and MyGene as expected. The input is simple error-free data to test the happy path.
        """
        # This function doesn't return anything
        map_accessions(
            config_file="config.yml",
            output_file="file.csv",
            upload_to_synapse=True,
            upload_synid=self._SYN_IDS["upload_id"],
        )

        expected_mapping_df = pd.DataFrame(
            {
                "uniprot_id": list(self._UNIPROT_IDS.keys()),
                "ensembl_gene_id": list(self._UNIPROT_IDS.values()),
            }
        ).sort_values(by=["uniprot_id", "ensembl_gene_id"])

        self.patch_syn_login.assert_called_once()
        # Was each proteomics file read in?
        self.patch_get_entity_as_df.assert_any_call(
            syn_id=self._SYN_IDS["ind_file"], source="csv", syn=ANY
        )
        self.patch_get_entity_as_df.assert_any_call(
            syn_id=self._SYN_IDS["agg_file"], source="csv", syn=ANY
        )
        # Was the config_file argument passed correctly to _get_config? This function is called twice, but both calls
        # should have the same argument.
        self.patch_get_config.assert_called_with(config_path="config.yml")
        # Did the staging path get determined correctly?
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=self._STAGING_PATH
        )
        # Was MyGene queried with the right arguments?
        self.patch_query_many.assert_called_once_with(
            set(self._UNIPROT_IDS.keys()),
            scopes="uniprot",
            fields=["ensembl.gene"],
            species="mouse",
            returnall=True,
        )
        # Was the mapping dataframe created as expected, and was the output file written to the correct staging path
        # and filename? We have to use ANY for the df argument and check DataFrame equivalence separately because this
        # assert can't compare DataFrames directly.
        self.patch_df_to_csv.assert_called_once_with(
            ANY, staging_path=self._STAGING_PATH, filename="file.csv"
        )
        pd.testing.assert_frame_equal(
            expected_mapping_df, self.patch_df_to_csv.call_args[0][0]
        )
        # Was the file "uploaded" to Synapse with the correct provenance and destination? We have to check provenance
        # separately because the IDs can appear in any order in the list.
        self.patch_load.assert_called_once_with(
            f"{self._STAGING_PATH}/file.csv",
            provenance=ANY,
            destination=self._SYN_IDS["upload_id"],
            syn=ANY,
        )
        assert sorted(self.patch_load.call_args.kwargs["provenance"]) == sorted(
            [self._SYN_IDS["ind_file"], self._SYN_IDS["agg_file"]]
        )

    def test_map_accessions_does_not_upload_when_upload_disabled(self):
        """
        Tests that map_accessions does not attempt to upload the output file to Synapse when the upload_to_synapse flag
        is set to False. We don't check that all functions are called, instead we check that `df_to_csv` is called but
        `load` is not.
        """
        map_accessions(
            config_file="config.yml",
            output_file="file.csv",
            upload_to_synapse=False,
            upload_synid=None,
        )

        self.patch_df_to_csv.assert_called_once_with(
            ANY, staging_path=self._STAGING_PATH, filename="file.csv"
        )
        self.patch_load.assert_not_called()

    def test_map_accessions_raises_when_no_upload_synid(self):
        """
        Tests that map_accessions raises an error when upload_to_synapse is True but no upload_synid is provided.
        """
        with pytest.raises(ValueError, match="upload_synid must be provided"):
            map_accessions(
                config_file="config.yml",
                output_file="file.csv",
                upload_to_synapse=True,
                upload_synid=None,
            )


class TestGetProteomicsFileSynIds:
    """
    Tests for get_proteomics_file_syn_ids. The config read is patched so no file is read from disk.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.patch_get_config = patch.object(
            map_uniprot_to_ensembl.utils, "_get_config"
        ).start()
        yield
        patch.stopall()

    def _simple_config(self):
        """
        Returns a very simple config file with two individual and two aggregate proteomics files. It also includes an
        unrelated dataset with the same key structure, to test that it is ignored. Unnecessary/unused fields like
        "format" are omitted for readability.
        """
        return {
            "files": [
                {"name": "ind_file1", "id": "syn1.1"},
                {"name": "ind_file2", "id": "syn2.2"},
                {"name": "agg_file1", "id": "syn3.3"},
                {"name": "agg_file2", "id": "syn4.4"},
                {"name": "unrelated_file1", "id": "syn99.1"},
                {"name": "unrelated_file2", "id": "syn99.2"},
            ],
            "datasets": [
                {
                    "protein_de_individual": {
                        "model_map": {
                            "ind_file1": "LOAD2",
                            "ind_file2": "LOAD2",
                        },
                    }
                },
                {
                    "protein_de_aggregate": {
                        "file_metadata": {
                            "agg_file1": {"model": "LOAD2"},
                            "agg_file2": {"model": "LOAD2"},
                        },
                    }
                },
                {
                    "unrelated_dataset": {
                        "model_map": {"unrelated_file1": "LOAD2"},
                        "file_metadata": {"unrelated_file2": {"model": "LOAD2"}},
                    }
                },
            ],
        }

    def _replace_dataset_files(self, config, dataset, new_files):
        """
        Replaces the files listed in a specific dataset within the config. The "datasets" section is a list of
        dictionaries, each with one key (the dataset name) and a value that is another dictionary containing "model_map"
        and/or "file_metadata". `new_files` should be a dictionary of {model_map: ...}, {file_metadata: ...}, both, or
        neither, depending on the needs of the test. `new_files` completely replaces all existing entries for that
        dataset. If the dataset doesn't already exist in the config, an entry will be created for it.
        """
        # Find which dataset in the list we should edit
        index = [
            x
            for x in range(0, len(config["datasets"]))
            if dataset in config["datasets"][x]
        ]
        if not index:
            # Create the dataset if it doesn't exist
            config["datasets"].append({dataset: {}})
            index = len(config["datasets"]) - 1
        else:
            index = index[0]

        config["datasets"][index][dataset] = new_files
        return config

    def test_returns_syn_ids_for_all_proteomics_files(self):
        """
        Synapse IDs for every file listed in model_map and file_metadata should be returned, and nothing else
        (metadata files, mapping files, and unrelated files are excluded).
        """
        self.patch_get_config.return_value = self._simple_config()

        result = get_proteomics_file_syn_ids("some/config.yaml")

        assert sorted(result) == ["syn1.1", "syn2.2", "syn3.3", "syn4.4"]
        self.patch_get_config.assert_called_once_with(config_path="some/config.yaml")

    def test_returns_unique_syn_ids_with_duplicate_files_list(self):
        """
        A file listed twice in the config["files"] list should only appear once in the output.
        """
        # Add a duplicate file with the same name to the files list
        config = self._simple_config()
        config["files"].append(config["files"][0])
        self.patch_get_config.return_value = config

        result = get_proteomics_file_syn_ids("some/config.yaml")

        assert sorted(result) == ["syn1.1", "syn2.2", "syn3.3", "syn4.4"]

    def test_returns_unique_syn_ids_with_shared_file(self):
        """
        A file shared across multiple datasets should only appear once in the output. This is not a scenario that
        happens in the real data but is tested for completeness.
        """
        # Add a file that is shared between the individual and aggregate datasets
        config = self._simple_config()
        config["files"].append({"name": "shared_file", "id": "syn_shared"})

        ind_map = {"model_map": {"ind_file1": "LOAD2", "shared_file": "LOAD2"}}
        agg_map = {
            "file_metadata": {
                "agg_file1": {"model": "LOAD2"},
                "shared_file": {"model": "LOAD2"},
            }
        }

        config = self._replace_dataset_files(config, "protein_de_individual", ind_map)
        config = self._replace_dataset_files(config, "protein_de_aggregate", agg_map)
        self.patch_get_config.return_value = config

        result = get_proteomics_file_syn_ids("some/config.yaml")
        assert sorted(result) == ["syn1.1", "syn3.3", "syn_shared"]

    def test_file_names_not_in_files_list_are_ignored(self):
        """
        File names in the dataset config that have no matching entry in config["files"] are skipped.
        """
        # Replace the individual proteomics data set files with one valid file and one file that doesn't exit in
        # config["files"]
        config = self._simple_config()
        new_map = {"model_map": {"ind_file1": "LOAD2", "not_a_real_file": "LOAD2"}}
        config = self._replace_dataset_files(config, "protein_de_individual", new_map)

        self.patch_get_config.return_value = config

        result = get_proteomics_file_syn_ids("some/config.yaml")

        # We replaced ind_file2 with a non-existent file, so syn2.2 should be excluded from the result
        assert sorted(result) == ["syn1.1", "syn3.3", "syn4.4"]

    @pytest.mark.parametrize(
        "missing_dataset",
        ["protein_de_individual", "protein_de_aggregate"],
        ids=["protein_de_individual", "protein_de_aggregate"],
    )
    def test_missing_dataset_raises(self, missing_dataset):
        """
        A ValueError should be raised if either proteomics dataset is missing from the config.
        """
        config = self._simple_config()

        # Remove the dataset from the config
        config["datasets"] = [d for d in config["datasets"] if missing_dataset not in d]
        self.patch_get_config.return_value = config

        with pytest.raises(
            ValueError, match=f"Missing {missing_dataset} dataset in the config"
        ):
            get_proteomics_file_syn_ids("some/config.yaml")

    @pytest.mark.parametrize(
        "dataset_name,file_map",
        [
            ("protein_de_individual", "model_map"),
            ("protein_de_aggregate", "file_metadata"),
        ],
        ids=["protein_de_individual", "protein_de_aggregate"],
    )
    def test_empty_file_map_raises(self, dataset_name, file_map):
        """
        A ValueError should be raised if a proteomics dataset's model_map / file_metadata section is empty.
        """
        # Create an file map section with no files
        config = self._simple_config()
        config = self._replace_dataset_files(config, dataset_name, {file_map: {}})

        self.patch_get_config.return_value = config

        with pytest.raises(
            ValueError,
            match=f"No proteomics data files found in {dataset_name}/{file_map}",
        ):
            get_proteomics_file_syn_ids("some/config.yaml")

    @pytest.mark.parametrize(
        "dataset_name",
        ["protein_de_individual", "protein_de_aggregate"],
        ids=["protein_de_individual", "protein_de_aggregate"],
    )
    def test_missing_file_map_section_raises(self, dataset_name):
        """
        A ValueError should be raised if the file map section is absent entirely from a proteomics dataset.
        """
        # Remove the file map section for the given dataset entirely
        config = self._simple_config()
        config = self._replace_dataset_files(config, dataset_name, {})
        self.patch_get_config.return_value = config

        with pytest.raises(
            ValueError, match=f"No proteomics data files found in {dataset_name}"
        ):
            get_proteomics_file_syn_ids("some/config.yaml")


class TestExtractIdsFromFile:
    """
    Tests for extract_ids_from_file.
    """

    def test_individual_proteomics_returns_column_headers(self):
        """
        Individual proteomics data should return all column names except "specimenID" and "individualID", in order.
        """
        df = pd.DataFrame(
            {
                "individualID": ["ind1", "ind2"],
                "specimenID": ["spec1", "spec2"],
                "Gene1|P12345": [1.0, 2.0],
                "Gene2|Q67890-2": [3.0, 4.0],
            }
        )

        assert extract_ids_from_file(df) == ["Gene1|P12345", "Gene2|Q67890-2"]

    def test_agg_proteomics_returns_protein_id_column(self):
        """
        Aggregate proteomics data should return the values of the "Protein ID" column, in order.
        """
        df = pd.DataFrame(
            {
                "Protein ID": ["Gene1|P12345", "Gene2|Q67890-2"],
                "log2FC": [0.1, -0.2],
            }
        )

        assert extract_ids_from_file(df) == ["Gene1|P12345", "Gene2|Q67890-2"]

    def test_individual_format_takes_precedence(self):
        """
        If a dataframe has both individual-format columns and a "Protein ID" column, it is treated as individual
        format. This is not a scenario that happens in the real data but is tested for completeness. We assume that if
        the file has specimenID values, this must be individual data.
        """
        df = pd.DataFrame(
            {
                "individualID": ["ind1"],
                "specimenID": ["spec1"],
                "Protein ID": ["Gene1|P12345"],
                "Other column": ["value1"],
            }
        )

        assert extract_ids_from_file(df) == ["Protein ID", "Other column"]

    @pytest.mark.parametrize(
        "columns",
        [
            ["individualID", "Gene1|P12345"],
            ["specimenID", "Gene1|P12345"],
            ["protein_id", "log2FC"],
            [],
        ],
    )
    def test_unrecognized_format_raises(self, columns):
        """
        A ValueError should be raised if the dataframe has neither ("specimenID" and "individualID") nor a
        "Protein ID" column.
        """
        df = pd.DataFrame(columns=columns)

        with pytest.raises(ValueError, match="Unrecognized dataframe format"):
            extract_ids_from_file(df)


class TestFormatUniprotIds:
    """
    Tests for format_uniprot_ids.
    """

    @staticmethod
    def _sorted(df: pd.DataFrame) -> pd.DataFrame:
        # The input is a set, so output row order is not deterministic
        return df.sort_values("original_uniprot_id").reset_index(drop=True)

    def test_splits_ids_and_removes_isoforms(self):
        """
        IDs should be split into gene symbol and UniProt ID, and isoform suffixes removed from "uniprot_id" while
        "original_uniprot_id" keeps them.
        """
        header_ids = {"Gene1|P12345", "Gene2|Q67890-2"}

        result = format_uniprot_ids(header_ids)

        expected = pd.DataFrame(
            {
                "gene_symbol": ["Gene1", "Gene2"],
                "original_uniprot_id": ["P12345", "Q67890-2"],
                "uniprot_id": ["P12345", "Q67890"],
            }
        )

        pd.testing.assert_frame_equal(self._sorted(result), self._sorted(expected))

    def test_isoforms_of_same_protein_are_kept_as_separate_rows(self):
        """
        Different isoforms of the same protein should each get their own row, with the same base UniProt ID.
        """
        header_ids = {"Gene1|P12345-1", "Gene1|P12345-2"}

        result = format_uniprot_ids(header_ids)
        expected = pd.DataFrame(
            {
                "gene_symbol": ["Gene1", "Gene1"],
                "original_uniprot_id": ["P12345-1", "P12345-2"],
                "uniprot_id": ["P12345", "P12345"],
            }
        )

        pd.testing.assert_frame_equal(self._sorted(result), self._sorted(expected))

    def test_invalid_ids_are_removed(self, capsys):
        """
        None, NaN, empty strings, and strings without a "|" separator, and non-strings should be dropped, and the number
        dropped should be printed as a warning.
        """
        header_ids = {
            "Gene1|P12345",
            # All of these should be removed
            None,
            np.nan,
            "",
            "no_separator",
            123,
            True,
            ("tuple|1", "tuple|2"),
        }

        result = format_uniprot_ids(header_ids)
        expected = pd.DataFrame(
            {
                "gene_symbol": ["Gene1"],
                "original_uniprot_id": ["P12345"],
                "uniprot_id": ["P12345"],
            }
        )
        pd.testing.assert_frame_equal(self._sorted(result), self._sorted(expected))
        assert "7 NA values found in `header_ids`" in capsys.readouterr().out

    def test_no_warning_when_all_ids_valid(self, capsys):
        """
        No warning should be printed if no IDs are removed.
        """
        format_uniprot_ids({"Gene1|P12345"})

        assert "NA values found" not in capsys.readouterr().out

    @pytest.mark.parametrize(
        "header_ids",
        [set(), {None, np.nan, "", "no_separator"}],
        ids=["empty_set", "all_invalid"],
    )
    def test_no_valid_ids_raises(self, header_ids):
        """
        If there are no valid IDs, either because the input is empty or because every ID is removed, the function should
        raise a ValueError
        """
        with pytest.raises(ValueError, match="No valid header IDs found"):
            format_uniprot_ids(header_ids)


class TestExtractEnsemblIds:
    """
    Tests for _extract_ensembl_ids.
    """

    def test_ids_extracted_from_dict(self):
        """
        Tests that the function extracts Ensembl IDs from a dictionary with a "gene" key and returns a 1-item list.
        """
        data = {"gene": "ENSMUSG01"}
        assert _extract_ensembl_ids(data) == ["ENSMUSG01"]

    def test_ids_extracted_from_list_of_dicts(self):
        """
        Tests that the function extracts Ensembl IDs from a list of dictionaries with "gene" keys.
        """
        item = [{"gene": "ENSMUSG01"}, {"gene": "ENSMUSG02"}]

        assert _extract_ensembl_ids(item) == ["ENSMUSG01", "ENSMUSG02"]

    def test_empty_list_returns_empty_list(self):
        """
        Tests that an empty list input returns an empty list.
        """
        assert _extract_ensembl_ids([]) == []

    @pytest.mark.parametrize("item", [np.nan, None])
    def test_na_input_returns_empty_list(self, item):
        """
        Tests that NaN or None input returns an empty list.
        """
        assert _extract_ensembl_ids(item) == []


class TestFormatMygeneResults:
    """
    Tests for format_mygene_results. The structure of results returned from mygene is:
    {
        "out": [
            # "ensembl" may be a dictionary with a "gene" key or a list of such dictionaries
            {"query": "<uniprot_id>", "ensembl": {"gene": "<ensembl_id>"}},
            {"query": "<uniprot_id>", "ensembl": [{"gene": "<ensembl_id1>"}, {"gene": "<ensembl_id2>"}]},
            ...
        ],
        "dup": [...], # Unused: list of duplicated UniProt IDs
        "missing": [...] # Unused: list of UniProt IDs with no Ensembl mapping
    }

    The mygene service can also return a structure with an "error" key or without the "out" key if there was an error
    during the query.
    """

    def test_results_sorted_by_uniprot_then_ensembl(self):
        """
        The results should be sorted by UniProt ID and then by Ensembl ID. The input data frame has both fields out
        of sort order.
        """
        results = {
            "out": [
                {"query": "Q00002", "ensembl": {"gene": "ENSMUSG02"}},
                {"query": "Q00001", "ensembl": {"gene": "ENSMUSG02"}},
                {"query": "Q00001", "ensembl": {"gene": "ENSMUSG01"}},
            ]
        }

        result = format_mygene_results(results).reset_index(drop=True)

        expected = pd.DataFrame(
            {
                "uniprot_id": ["Q00001", "Q00001", "Q00002"],
                "ensembl_gene_id": [
                    "ENSMUSG01",
                    "ENSMUSG02",
                    "ENSMUSG02",
                ],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_invalid_ids_removed(self):
        """
        Missing, None, NA, and non-Mus musculus Ensembl IDs should be removed from the results.
        """
        results = {
            "out": [
                {"query": "Q00001", "ensembl": {"gene": "ENSMUSG01"}},
                # All of these should be removed
                {"query": "Q00002", "ensembl": {"gene": "ENSNotValid01"}},
                {"query": "Q00003", "ensembl": {"gene": None}},
                {"query": "Q00004", "ensembl": {"gene": np.nan}},
                {"query": "Q00005", "ensembl": {"gene": ""}},
                {"query": "Q00006", "ensembl": []},
            ]
        }

        result = format_mygene_results(results).reset_index(drop=True)

        expected = pd.DataFrame(
            {
                "uniprot_id": ["Q00001"],
                "ensembl_gene_id": ["ENSMUSG01"],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_same_ensembl_id_for_multiple_uniprot_ids(self):
        """
        An Ensembl ID that maps to more than one UniProt ID should appear once per UniProt ID.
        """
        results = {
            "out": [
                {"query": "Q00002", "ensembl": {"gene": "ENSMUSG01"}},
                {"query": "Q00001", "ensembl": {"gene": "ENSMUSG01"}},
            ]
        }

        result = format_mygene_results(results).reset_index(drop=True)

        expected = pd.DataFrame(
            {
                "uniprot_id": ["Q00001", "Q00002"],
                "ensembl_gene_id": ["ENSMUSG01", "ENSMUSG01"],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_multiple_ensembl_ids_for_single_uniprot_id(self):
        """
        A single UniProt ID that maps to multiple Ensembl IDs should have each mapping represented.
        """
        results = {
            "out": [
                {
                    "query": "Q00001",
                    "ensembl": [{"gene": "ENSMUSG01"}, {"gene": "ENSMUSG02"}],
                }
            ]
        }

        result = format_mygene_results(results).reset_index(drop=True)

        expected = pd.DataFrame(
            {
                "uniprot_id": ["Q00001", "Q00001"],
                "ensembl_gene_id": ["ENSMUSG01", "ENSMUSG02"],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_no_matches_returns_empty_dataframe(self):
        """
        If no UniProt ID maps to an ENSMUSG ID, an empty dataframe with the expected columns should be returned.
        """
        results = {
            "out": [
                {"query": "Q00001", "notfound": True},
                {"query": "Q00002", "ensembl": {"gene": "ENSNotValid01"}},
            ]
        }

        result = format_mygene_results(results)

        assert result.empty
        assert list(result.columns) == ["uniprot_id", "ensembl_gene_id"]

    def test_error_key_raises(self):
        """
        If the results dictionary contains an "error" key, an exception should be raised and the error message should be
        included in the exception message.
        """
        results = {"error": "Some error occurred"}

        with pytest.raises(
            ValueError,
            match="The MyGene service produced an error: Some error occurred",
        ):
            format_mygene_results(results)

    def test_missing_out_key_raises(self):
        """
        If the results dictionary does not contain the "out" key, an exception should be raised.
        """
        results = {"not_out": []}

        with pytest.raises(
            ValueError, match="The MyGene service produced an unknown error."
        ):
            format_mygene_results(results)

    def test_malformed_results_raises(self):
        """
        If the results dictionary is malformed and does not contain the "query" key, an exception should be raised.
        """
        results = {"out": [{"not_query": "Q00001", "ensembl": {"gene": "ENSMUSG01"}}]}

        with pytest.raises(ValueError, match="MyGene returned malformed results"):
            format_mygene_results(results)

    def test_missing_ensembl_key_raises(self):
        """
        If the results dictionary does not contain the "ensembl" key, an exception should be raised.
        """
        results = {"out": [{"query": "Q00001"}]}

        with pytest.raises(
            ValueError, match="MyGene did not find any matching Ensembl IDs"
        ):
            format_mygene_results(results)
