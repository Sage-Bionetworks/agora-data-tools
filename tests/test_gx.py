import os
import shutil
import json
from unittest import mock
from unittest.mock import patch

import pandas as pd

from agoradatatools.reporter import DatasetReport

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
        assert self.good_runner.failures is False
        assert self.good_runner.failure_message is None
        assert self.good_runner.warnings is False
        assert self.good_runner.warning_message is None
        assert self.good_runner.report_file is None
        assert self.good_runner.report_version is None
        assert self.good_runner.report_link is None

        assert self.good_runner.syn == syn
        assert (
            self.good_runner.dataset_path == "./tests/test_assets/gx/metabolomics.json"
        )
        assert self.good_runner.expectation_suite_name == "metabolomics"
        assert self.good_runner.upload_folder == "test_folder"
        assert self.good_runner.nested_columns is None
        assert (
            self.good_runner.validations_path
            == self.good_runner.gx_project_dir
            + "/gx/uncommitted/data_docs/local_site/validations"
        )
        assert (
            self.good_runner.gx_project_dir
            == self.good_runner._get_data_context_location()
        )
        assert isinstance(self.good_runner.context, FileDataContext)

    def test_that_get_data_context_location_returns_the_path_to_the_gx_directory(
        self,
    ):
        expected_end = os.path.join("agoradatatools", "great_expectations")
        result = self.good_runner._get_data_context_location()
        assert result.endswith(expected_end)

    def test_check_if_expectation_suite_exists_returns_false_when_the_expectation_suite_does_not_exist(
        self,
    ):
        assert self.bad_runner.check_if_expectation_suite_exists() is False

    def test_check_if_expectation_suite_exists_returns_true_when_the_expectation_suite_exists(
        self,
    ):
        assert self.good_runner.check_if_expectation_suite_exists() is True

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
            result = self.good_runner.get_results_path(mocked_checkpoint_result)
            patch_list_validation_result_identifiers.assert_called_once()
            patch_copy.assert_called_once_with(
                self.good_runner.validations_path + "/test/path/to/file.html",
                self.good_runner.validations_path
                + f"/test/path/to/{self.good_runner.expectation_suite_name}.html",
            )
            assert result == expected

    def test_upload_results_file_to_synapse(self):
        with patch.object(
            self.good_runner.syn,
            "store",
            return_value=File(parent="syn456", id="syn123", versionNumber=1),
        ) as patch_syn_store:
            self.good_runner.upload_results_file_to_synapse("test_path")
            patch_syn_store.assert_called_once_with(
                File(path="test_path", parent=self.good_runner.upload_folder),
                activity=Activity(
                    name=f"Great Expectations {self.good_runner.expectation_suite_name} results",
                    executed="https://github.com/Sage-Bionetworks/agora-data-tools",
                ),
                forceVersion=True,
            )
            assert self.good_runner.report_file == "syn123"
            assert self.good_runner.report_version == 1
            assert self.good_runner.report_link == DatasetReport.format_link(
                syn_id="syn123", version=1
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

    def test_generate_message_returns_formatted_strings_as_expected(self):
        result_dict = {
            "test_suite": {"test_column": ["expect_column_values_to_be_unique"]}
        }
        test_warn_message, test_warn_status = self.good_runner._generate_message(
            result_dict, "warnings"
        )
        assert (
            test_warn_message
            == "Great Expectations data validation has the following warnings: "
            "In the test_suite dataset, 'test_column' has failed values for "
            "expectations expect_column_values_to_be_unique"
        )
        assert test_warn_status is True

        test_fail_message, test_fail_status = self.good_runner._generate_message(
            result_dict, "failures"
        )
        assert (
            test_fail_message
            == "Great Expectations data validation has the following failures: "
            "In the test_suite dataset, 'test_column' has failed values for "
            "expectations expect_column_values_to_be_unique"
        )
        assert test_fail_status is True

    def test_generate_message_returns_none_if_no_messages(self):
        result_dict = {}
        test_warn_message, test_warn_status = self.good_runner._generate_message(
            result_dict, "warnings"
        )
        assert test_warn_message is None
        assert test_warn_status is False

        test_fail_message, test_fail_status = self.good_runner._generate_message(
            result_dict, "failures"
        )
        assert test_fail_message is None
        assert test_fail_status is False

    def test_set_warnings_and_failures_changes_no_attributes_if_everything_passes(self):
        self.good_runner.set_warnings_and_failures(self.passed_checkpoint_result)
        assert self.good_runner.warnings is False
        assert self.good_runner.warning_message is None
        assert self.good_runner.failures is False
        assert self.good_runner.failure_message is None

    def test_set_warnings_and_failures_changes_attributes_if_there_are_warnings_and_failures(
        self,
    ):
        self.good_runner.set_warnings_and_failures(self.failed_checkpoint_result)
        assert self.good_runner.warnings is True
        assert (
            self.good_runner.warning_message
            == "Great Expectations data validation has the following warnings: "
            "In the metabolomics dataset, 'ensembl_gene_id' has failed values for "
            "expectations expect_column_value_lengths_to_equal"
        )
        assert self.good_runner.failures is True
        assert self.good_runner.failure_message == (
            "Great Expectations data validation has the following failures: "
            "In the metabolomics dataset, 'ensembl_gene_id' has failed values for "
            "expectations expect_column_values_to_match_regex"
        )

    def test_run_when_expectation_suite_exists_and_nested_columns(
        self,
    ):
        with patch.object(
            self.good_runner, "check_if_expectation_suite_exists", return_value=True
        ) as patch_check_if_expectation_suite_exists, patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.passed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "set_warnings_and_failures"
        ) as patch_set_warnings_and_failures:
            self.good_runner.nested_columns = ["a"]
            self.good_runner.run()
            patch_check_if_expectation_suite_exists.assert_called_once()
            patch_read_json.assert_called_once_with(
                self.good_runner.dataset_path,
            )
            patch_convert_nested_columns_to_json.assert_called_once()
            patch_checkpoint_run.assert_called_once()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
            patch_set_warnings_and_failures.assert_called_once_with(
                patch_checkpoint_run.return_value
            )

    def test_run_when_expectation_suite_exists_and_no_nested_columns(
        self,
    ):
        with patch.object(
            self.good_runner, "check_if_expectation_suite_exists", return_value=True
        ) as patch_check_if_expectation_suite_exists, patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.passed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "set_warnings_and_failures"
        ) as patch_set_warnings_and_failures:
            self.good_runner.run()
            patch_check_if_expectation_suite_exists.assert_called_once()
            patch_read_json.assert_called_once_with(
                self.good_runner.dataset_path,
            )
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_checkpoint_run.assert_called_once()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
            patch_set_warnings_and_failures.assert_called_once_with(
                self.passed_checkpoint_result
            )

    def test_that_run_does_not_complete_when_check_if_expectation_suite_exists_is_false(
        self,
    ):
        with patch.object(
            self.good_runner, "check_if_expectation_suite_exists", return_value=False
        ) as patch_check_if_expectation_suite_exists, patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "set_warnings_and_failures"
        ) as patch_set_warnings_and_failures:
            self.good_runner.run()
            patch_check_if_expectation_suite_exists.assert_called_once()
            patch_read_json.assert_not_called()
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_checkpoint_run.assert_not_called()
            patch_get_results_path.assert_not_called()
            patch_upload_results_file_to_synapse.assert_not_called()
            patch_set_warnings_and_failures.assert_not_called()

    def test_run_when_validation_fails_and_has_warnings(
        self,
    ):
        with patch.object(
            self.good_runner, "check_if_expectation_suite_exists", return_value=True
        ) as patch_check_if_expectation_suite_exists, patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.failed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "set_warnings_and_failures"
        ) as patch_set_warnings_and_failures:
            self.good_runner.run()
            patch_check_if_expectation_suite_exists.assert_called_once()
            patch_read_json.assert_called_once_with(self.good_runner.dataset_path)
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_checkpoint_run.assert_called_once()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
            patch_set_warnings_and_failures.assert_called_once_with(
                patch_checkpoint_run.return_value
            )

    def test_that_that_files_are_not_uploaded_when_upload_folder_is_none(
        self,
    ):
        with patch.object(
            self.good_runner, "check_if_expectation_suite_exists", return_value=True
        ) as patch_check_if_expectation_suite_exists, patch.object(
            pd, "read_json", return_value=pd.DataFrame()
        ) as patch_read_json, patch.object(
            self.good_runner,
            "convert_nested_columns_to_json",
            return_value=pd.DataFrame(),
        ) as patch_convert_nested_columns_to_json, patch.object(
            self.good_runner, "get_results_path", return_value="test_path"
        ) as patch_get_results_path, patch.object(
            self.good_runner, "upload_results_file_to_synapse", return_value=None
        ) as patch_upload_results_file_to_synapse, patch.object(
            Checkpoint,
            "run",
            return_value=self.passed_checkpoint_result,
        ) as patch_checkpoint_run, patch.object(
            self.good_runner, "set_warnings_and_failures"
        ) as patch_set_warnings_and_failures:
            self.good_runner.upload_folder = None
            self.good_runner.run()
            patch_check_if_expectation_suite_exists.assert_called_once()
            patch_read_json.assert_called_once_with(self.good_runner.dataset_path)
            patch_convert_nested_columns_to_json.assert_not_called()
            patch_checkpoint_run.assert_called_once()
            patch_get_results_path.assert_called_once()
            patch_upload_results_file_to_synapse.assert_not_called()
            patch_set_warnings_and_failures.assert_called_once_with(
                self.passed_checkpoint_result
            )
