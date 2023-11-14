from unittest import mock
from unittest.mock import patch

import shutil
import os

from great_expectations.data_context import FileDataContext
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

from synapseclient import File

from agoradatatools.gx import GreatExpectationsRunner

good_dataset_path = "./tests/test_assets/gx/metabolomics.json"
bad_dataset_path = "./tests/test_assets/gx/not_supported_dataset.json"


def test_that_an_initialized_runner_has_the_attributes_it_should(syn):
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=good_dataset_path,
        dataset_name="metabolomics",
        upload_folder="test_folder",
    )
    assert test_runner.gx_project_dir == "./great_expectations"
    assert test_runner.syn == syn
    assert test_runner.dataset_path == good_dataset_path
    assert test_runner.expectation_suite_name == "metabolomics"
    assert test_runner.upload_folder == "test_folder"
    assert isinstance(test_runner.context, FileDataContext)
    assert (
        test_runner.validations_relative_path
        == "./great_expectations/gx/uncommitted/data_docs/local_site/validations"
    )


def test_check_if_expectation_suite_exists_returns_false_when_the_expectation_suite_does_not_exist(
    syn,
):
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=bad_dataset_path,
        dataset_name="not_supported_dataset",
        upload_folder="test_folder",
    )
    assert test_runner._check_if_expectation_suite_exists() is False


def test_check_if_expectation_suite_exists_returns_true_when_the_expectation_suite_exists(
    syn,
):
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=good_dataset_path,
        dataset_name="metabolomics",
        upload_folder="test_folder",
    )
    assert test_runner._check_if_expectation_suite_exists() is True


def test_get_results_path(syn):
    expected = "./great_expectations/gx/uncommitted/data_docs/local_site/validations/test/path/to/to.html"
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=good_dataset_path,
        dataset_name="metabolomics",
        upload_folder="test_folder",
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
        result = test_runner._get_results_path(mocked_checkpoint_result)
        patch_list_validation_result_identifiers.assert_called_once()
        patch_copy.assert_called_once_with(
            test_runner.validations_relative_path + "/test/path/to/file.html",
            test_runner.validations_relative_path + "/test/path/to/to.html",
        )
        assert result == expected


def test_upload_results_file_to_synapse(syn):
    with patch.object(syn, "store") as patch_syn_store:
        test_runner = GreatExpectationsRunner(
            syn=syn,
            dataset_path=good_dataset_path,
            dataset_name="metabolomics",
            upload_folder="test_folder",
        )
        test_runner._upload_results_file_to_synapse("test_path")
        patch_syn_store.assert_called_once_with(
            File(path="test_path", parent=test_runner.upload_folder)
        )


def test_that_run_completes_successfully_when_check_if_expectation_suite_exists_is_true(
    syn,
):
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=good_dataset_path,
        dataset_name="metabolomics",
        upload_folder="test_folder",
    )
    with patch.object(
        test_runner, "_check_if_expectation_suite_exists", return_value=True
    ), patch.object(
        test_runner, "_get_results_path", return_value="test_path"
    ) as patch_get_results_path, patch.object(
        test_runner, "_upload_results_file_to_synapse", return_value=None
    ) as patch_upload_results_file_to_synapse:
        test_runner.run()
        patch_get_results_path.assert_called_once()
        patch_upload_results_file_to_synapse.assert_called_once_with("test_path")


def test_that_run_does_not_complete_when_check_if_expectation_suite_exists_is_false(
    syn,
):
    test_runner = GreatExpectationsRunner(
        syn=syn,
        dataset_path=good_dataset_path,
        dataset_name="metabolomics",
        upload_folder="test_folder",
    )
    with patch.object(
        test_runner, "_check_if_expectation_suite_exists", return_value=False
    ), patch.object(
        test_runner, "_get_results_path", return_value="test_path"
    ) as patch_get_results_path, patch.object(
        test_runner, "_upload_results_file_to_synapse", return_value=None
    ) as patch_upload_results_file_to_synapse:
        test_runner.run()
        patch_get_results_path.assert_not_called()
        patch_upload_results_file_to_synapse.assert_not_called()
