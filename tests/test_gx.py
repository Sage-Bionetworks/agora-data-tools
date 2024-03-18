import os
import shutil
import json
from unittest import mock
from unittest.mock import patch

import pandas as pd

from agoradatatools.errors import ADTDataValidationError

import pytest
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from synapseclient import Activity, File

from agoradatatools.gx import GreatExpectationsRunner


class TestGreatExpectationsRunner:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.good_runner = GreatExpectationsRunner(
            syn=syn,
            dataset_path="./tests/test_assets/gx/metabolomics.json",
            dataset_name="metabolomics",
            upload_folder="test_folder",
            nested_columns=None,
        )
        self.bad_runner = GreatExpectationsRunner(
            syn=syn,
            dataset_path="./tests/test_assets/gx/not_supported_dataset.json",
            dataset_name="not_supported_dataset",
            upload_folder="test_folder",
            nested_columns=None,
        )
        self.passed_checkpoint_result = CheckpointResult(
            **json.load(open("./tests/test_assets/gx/checkpoint_result_pass.json"))
        )
        self.failed_checkpoint_result = CheckpointResult(
            **json.load(open("./tests/test_assets/gx/checkpoint_result_fail.json"))
        )

    def test_that_an_initialized_runner_has_the_attributes_it_should(self, syn):
        assert (
            self.good_runner.gx_project_dir
            == self.good_runner._get_data_context_location()
        )
        assert self.good_runner.syn == syn
        assert (
            self.good_runner.dataset_path == "./tests/test_assets/gx/metabolomics.json"
        )
        assert self.good_runner.expectation_suite_name == "metabolomics"
        assert self.good_runner.upload_folder == "test_folder"
        assert isinstance(self.good_runner.context, FileDataContext)
        assert (
            self.good_runner.validations_path
            == self.good_runner.gx_project_dir
            + "/gx/uncommitted/data_docs/local_site/validations"
        )

    def test_that_get_data_context_location_returns_the_path_to_the_gx_directory(
        self,
    ):
        expected_end = os.path.join("agoradatatools", "great_expectations")
        result = self.good_runner._get_data_context_location()
        assert result.endswith(expected_end)

    def test_check_if_expectation_suite_exists_returns_false_when_the_expectation_suite_does_not_exist(
        self,
    ):
        assert self.bad_runner._check_if_expectation_suite_exists() is False

    def test_check_if_expectation_suite_exists_returns_true_when_the_expectation_suite_exists(
        self,
    ):
        assert self.good_runner._check_if_expectation_suite_exists() is True

    def test_get_results_path(self):
        expected = (
            self.good_runner.validations_path
            + f"/test/path/to/{self.good_runner.expectation_suite_name}.html"
        )
        mocked_checkpoint_result = mock.create_autospec(CheckpointResult)
        mocked_validation_result_identifier = mock.create_autospec(
            ValidationResultIdentifier(
                expectation_suite_identifier="test_expectation_suite_identifier",
                run_id="test_run_id",
                batch_identifier="test_batch_identifier",
            )
        )
        mocked_checkpoint_result.list_validation_result_identifiers.return_value = [
            mocked_validation_result_identifier
        ]
        with patch.object(
            mocked_validation_result_identifier,
            "to_tuple",
            return_value=("test", "path", "to", "file"),
        ) as patch_list_validation_result_identifiers, patch.object(
            shutil, "copy"
        ) as patch_copy:
            result = self.good_runner._get_results_path(mocked_checkpoint_result)
            patch_list_validation_result_identifiers.assert_called_once()
            patch_copy.assert_called_once_with(
                self.good_runner.validations_path + "/test/path/to/file.html",
                self.good_runner.validations_path
                + f"/test/path/to/{self.good_runner.expectation_suite_name}.html",
            )
            assert result == expected

    def test_upload_results_file_to_synapse(self):
        with patch.object(self.good_runner.syn, "store") as patch_syn_store:
            self.good_runner._upload_results_file_to_synapse("test_path")
            patch_syn_store.assert_called_once_with(
                File(path="test_path", parent=self.good_runner.upload_folder),
                activity=Activity(
                    name=f"Great Expectations {self.good_runner.expectation_suite_name} results",
                    executed="https://github.com/Sage-Bionetworks/agora-data-tools",
                ),
                forceVersion=True,
            )

    def test_that_convert_nested_columns_to_json_converts_nested_columns_to_json(self):
        df = pd.DataFrame({"a": [[1, 2, 3]], "b": [[4, 5, 6]]})
        expected = pd.DataFrame({"a": [[1, 2, 3]], "b": ["[4, 5, 6]"]})
        result = self.good_runner.convert_nested_columns_to_json(df, ["b"])
        assert json.loads(result["b"][0]) == [4, 5, 6]
        pd.testing.assert_frame_equal(result, expected)

    def test_that_convert_nested_columns_to_json_does_nothing_if_no_nested_columns(
        self,
    ):
        df = pd.DataFrame({"a": [[1, 2, 3]], "b": [[4, 5, 6]]})
        result = self.good_runner.convert_nested_columns_to_json(df, [])
        pd.testing.assert_frame_equal(result, df)

    def test_get_failed_expectations_with_failed_checkpoint_result(self):
        expected = (
            "Great Expectations data validation has failed: "
            "ensembl_gene_id has failed expectations expect_column_value_lengths_to_equal, "
            "expect_column_values_to_match_regex"
        )
        fail_message = self.good_runner.get_failed_expectations(
            self.failed_checkpoint_result
        )
        assert fail_message == expected

    def test_run_when_expectation_suite_exists_and_nested_columns(
        self,
    ):
        with patch.object(
            self.good_runner, "_check_if_expectation_suite_exists", return_value=True
        ), patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "_get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "_upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.passed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "get_failed_expectations", return_value="test"
        ) as patch_get_failed_expectations:
            self.good_runner.nested_columns = ["a"]
            self.good_runner.run()
            patch_read_json.assert_called_once_with(
                self.good_runner.dataset_path,
            )
            patch_convert_nested_columns_to_json.assert_called_once()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
            patch_checkpoint_run.assert_called_once()
            patch_get_failed_expectations.assert_not_called()

    def test_run_when_expectation_suite_exists_and_no_nested_columns(
        self,
    ):
        with patch.object(
            self.good_runner, "_check_if_expectation_suite_exists", return_value=True
        ), patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "_get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "_upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.passed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "get_failed_expectations", return_value="test"
        ) as patch_get_failed_expectations:
            self.good_runner.run()
            patch_read_json.assert_called_once_with(
                self.good_runner.dataset_path,
            )
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
            patch_checkpoint_run.assert_called_once()
            patch_get_failed_expectations.assert_not_called()

    def test_that_run_does_not_complete_when_check_if_expectation_suite_exists_is_false(
        self,
    ):
        with patch.object(
            self.good_runner, "_check_if_expectation_suite_exists", return_value=False
        ), patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "_get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "_upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "get_failed_expectations", return_value="test"
        ) as patch_get_failed_expectations:
            self.good_runner.run()
            patch_read_json.assert_not_called()
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_get_results_path.assert_not_called()
            patch_upload_results_file_to_synapse.assert_not_called()
            patch_checkpoint_run.assert_not_called()
            patch_get_failed_expectations.assert_not_called()

    def test_run_raises_error_when_validation_fails(
        self,
    ):
        with patch.object(
            self.good_runner, "_check_if_expectation_suite_exists", return_value=True
        ), patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "_get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "_upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.failed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "get_failed_expectations", return_value="test"
        ) as patch_get_failed_expectations:
            with pytest.raises(ADTDataValidationError, match="test"):
                self.good_runner.run()
                patch_read_json.assert_called_once_with(self.good_runner.dataset_path)
                patch_convert_nested_columns_to_json.assert_not_called()
                patch_get_results_path.assert_called_once()
                patch_upload_results_file_to_synapse.assert_called_once_with(
                    "test_path"
                )
                patch_checkpoint_run.assert_called_once()
                patch_get_failed_expectations.assert_called_once_with(
                    patch_upload_results_file_to_synapse.return_value
                )
