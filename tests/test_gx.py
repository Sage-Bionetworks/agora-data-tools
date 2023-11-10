from unittest.mock import patch


from great_expectations.data_context import FileDataContext
from synapseclient import File

from agoradatatools.gx import GreatExpectationsRunner

good_dataset_path = "./tests/test_assets/gx/metabolomics.json"
bad_dataset_path = "./tests/test_assets/gx/not_supported_dataset.json"


def test_that_an_initialized_runner_has_the_attributes_it_should(syn):
    test_runner = GreatExpectationsRunner(syn=syn, dataset_path=good_dataset_path)
    assert test_runner.gx_project_dir == "./great_expectations"
    assert test_runner.syn == syn
    assert test_runner.dataset_path == good_dataset_path
    assert test_runner.expectation_suite_name == "metabolomics"
    assert isinstance(test_runner.context, FileDataContext)
    assert (
        test_runner.validations_relative_path
        == "./great_expectations/gx/uncommitted/data_docs/local_site/validations"
    )


def test_check_if_expectation_suite_exists_returns_false_when_the_expectation_suite_does_not_exist(
    syn,
):
    test_runner = GreatExpectationsRunner(syn=syn, dataset_path=bad_dataset_path)
    assert test_runner.check_if_expectation_suite_exists() is False


def test_check_if_expectation_suite_exists_returns_true_when_the_expectation_suite_exists(
    syn,
):
    test_runner = GreatExpectationsRunner(syn=syn, dataset_path=good_dataset_path)
    assert test_runner.check_if_expectation_suite_exists() is True


def test_get_results_path(syn):
    ...


def test_upload_results_file_to_synapse(syn):
    with patch.object(syn, "store") as patch_syn_store:
        test_runner = GreatExpectationsRunner(syn=syn, dataset_path=good_dataset_path)
        test_runner._upload_results_file_to_synapse("test_path")
        patch_syn_store.assert_called_once_with(
            File(
                path="test_path",
                parent=test_runner.synapse_folder_dict[
                    test_runner.expectation_suite_name
                ],
            )
        )


def test_that_run_completes_successfully(syn):
    test_runner = GreatExpectationsRunner(syn=syn, dataset_path=good_dataset_path)
    with patch.object(
        test_runner, "_get_results_path", return_value="test_path"
    ) as patch_get_results_path, patch.object(
        test_runner, "_upload_results_file_to_synapse", return_value=None
    ) as patch_upload_results_file_to_synapse:
        test_runner.run()
        patch_get_results_path.assert_called_once()
        patch_upload_results_file_to_synapse.assert_called_once_with("test_path")
