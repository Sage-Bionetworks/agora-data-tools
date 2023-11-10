import logging
import os
import shutil

from synapseclient import File, Synapse

import great_expectations as gx

logger = logging.getLogger(__name__)
# Disable GX INFO logging
logging.getLogger("great_expectations").setLevel(logging.WARNING)


class GreatExpectationsRunner:
    """Class to run great expectations on a dataset and upload the results to Synapse"""

    def __init__(self, syn: Synapse, dataset_path: str):
        """Initialize the class"""
        self.syn = syn
        self.dataset_path = dataset_path
        self.expectation_suite_name = dataset_path.split("/")[-1].split(".")[0]
        self.context = gx.get_context(project_root_dir="./great_expectations")
        from expectations.expect_column_values_to_have_list_length import (
            ExpectColumnValuesToHaveListLength,
        )
        from expectations.expect_column_values_to_have_list_members import (
            ExpectColumnValuesToHaveListMembers,
        )
        from expectations.expect_column_values_to_have_list_members_of_type import (
            ExpectColumnValuesToHaveListMembersOfType,
        )

    def check_if_expectation_suite_exists(self):
        """Checks if the expectation suite exists in the great_expectations workspace"""
        return (
            self.expectation_suite_name in self.context.list_expectation_suite_names()
        )

    def _get_results_path(self):
        """Gets the path to the results file, copies it to a Synapse-API friendly name, and returns the new path"""
        results_path_items = list(
            self.checkpoint_result.list_validation_result_identifiers()[0].to_tuple()
        )
        results_path_items[-1] = results_path_items[-1] + ".html"
        original_results_path = os.path.join(
            "./great_expectations/gx/uncommitted/data_docs/local_site/validations",
            *results_path_items,
        )
        timestamp_file_name = results_path_items[-2] + ".html"
        new_results_path_items = results_path_items
        new_results_path_items[-1] = timestamp_file_name
        new_results_path = os.path.join(
            "./great_expectations/gx/uncommitted/data_docs/local_site/validations",
            *new_results_path_items,
        )

        shutil.copy(original_results_path, new_results_path)
        return new_results_path

    def _upload_results_file_to_synapse(self):
        """Uploads the results file to Synapse"""
        results_path = self._get_results_path()
        self.syn.store(File(results_path, parentId="syn52928363"))

    def run(self):
        """Run great expectations on a dataset and upload the results to Synapse"""

        validator = self.context.sources.pandas_default.read_json(
            self.dataset_path,
        )
        expectation_suite = self.context.get_expectation_suite(
            self.expectation_suite_name
        )
        validator.expectation_suite = expectation_suite
        validator.validate()
        checkpoint = self.context.add_or_update_checkpoint(
            name=self.expectation_suite_name,
            validator=validator,
        )
        self.checkpoint_result = checkpoint.run()
        logger.info(
            f"Data validation complete for {self.expectation_suite_name}. Uploading results to Synapse."
        )
        self._upload_results_file_to_synapse()
