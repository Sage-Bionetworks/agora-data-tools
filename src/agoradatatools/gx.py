import json
import logging
import os
import shutil
import typing
from typing import Optional

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from synapseclient import Activity, File, Synapse

from agoradatatools.reporter import DatasetReport

logger = logging.getLogger(__name__)
logging.getLogger("great_expectations").setLevel(logging.WARNING)


class GreatExpectationsRunner:
    """Class to run great expectations on a dataset and upload the HTML report to Synapse"""

    failures: bool = False
    failure_message: Optional[str] = None
    warnings: bool = False
    warning_message: Optional[str] = None
    report_file: Optional[str] = None
    report_version: Optional[int] = None
    report_link: Optional[str] = None

    def __init__(
        self,
        syn: Synapse,
        dataset_path: str,
        dataset_name: str,
        upload_folder: str = None,
        nested_columns: typing.List[str] = None,
    ):
        """Initialize the class"""
        self.syn = syn
        self.dataset_path = dataset_path
        self.expectation_suite_name = dataset_name
        self.upload_folder = upload_folder
        self.nested_columns = nested_columns
        self.gx_project_dir = self._get_data_context_location()

        self.context = gx.get_context(project_root_dir=self.gx_project_dir)
        self.validations_path = os.path.join(
            self.gx_project_dir, "gx/uncommitted/data_docs/local_site/validations"
        )
        from expectations.expect_column_values_to_have_list_length import (
            ExpectColumnValuesToHaveListLength,
        )
        from expectations.expect_column_values_to_have_list_length_in_range import (
            ExpectColumnValuesToHaveListLengthInRange,
        )
        from expectations.expect_column_values_to_have_list_members import (
            ExpectColumnValuesToHaveListMembers,
        )
        from expectations.expect_column_values_to_have_list_members_of_type import (
            ExpectColumnValuesToHaveListMembersOfType,
        )
        from expectations.expect_column_values_to_have_list_of_dict_with_expected_values import (
            ExpectColumnValuesToHaveListOfDictWithExpectedValues,
        )

    def _get_data_context_location(self) -> str:
        """Gets the path to the great_expectations directory"""
        script_dir = os.path.dirname(os.path.realpath(__file__))
        gx_directory = os.path.join(script_dir, "great_expectations")
        return gx_directory

    def check_if_expectation_suite_exists(self) -> bool:
        """Checks if the expectation suite exists in the great_expectations workspace"""
        exists = (
            self.expectation_suite_name in self.context.list_expectation_suite_names()
        )
        if not exists:
            logger.info(
                f"Expectation suite {self.expectation_suite_name} does not exist. Data validation will not be performed."
            )
        return exists

    def get_results_path(self, checkpoint_result: CheckpointResult) -> str:
        """Gets the path to the most recent HTML report for a checkpoint,
        copies it to a Synapse-API friendly name, and returns the new path

        Args:
            checkpoint_result (CheckpointResult): CheckpointResult object from GX validation run.
        """
        validation_results = checkpoint_result.list_validation_result_identifiers()
        latest_validation_result = validation_results[0]

        original_results_path_items = list(latest_validation_result.to_tuple())
        original_results_path_items[-1] = original_results_path_items[-1] + ".html"
        original_results_path = os.path.join(
            self.validations_path,
            *original_results_path_items,
        )

        expectation_suite_name = self.expectation_suite_name + ".html"
        new_results_path_items = original_results_path_items
        new_results_path_items[-1] = expectation_suite_name
        new_results_path = os.path.join(
            self.validations_path,
            *new_results_path_items,
        )

        shutil.copy(original_results_path, new_results_path)
        return new_results_path

    def upload_results_file_to_synapse(self, results_path: str) -> None:
        """Uploads a results file to Synapse. Assigns class attributes associated
        with the report file.

        Args:
            results_path (str): Path to the GX report file.
        """
        file = self.syn.store(
            File(
                results_path,
                parentId=self.upload_folder,
            ),
            activity=Activity(
                name=f"Great Expectations {self.expectation_suite_name} results",
                executed="https://github.com/Sage-Bionetworks/agora-data-tools",
            ),
            forceVersion=True,
        )
        self.report_file = file.id
        self.report_version = file.versionNumber
        self.report_link = DatasetReport.format_link(
            syn_id=file.id, version=file.versionNumber
        )

    @staticmethod
    def convert_nested_columns_to_json(
        df: pd.DataFrame, nested_columns: typing.List[str]
    ) -> pd.DataFrame:
        """Converts nested columns in a DataFrame to JSON-parseable strings

        Args:
            df (pd.DataFrame): DataFrame
            nested_columns (typing.List[str]): List of nested columns

        Returns:
            df (pd.DataFrame): DataFrame with nested columns converted to JSON-parseable strings
        """
        df = df.copy()
        for column in nested_columns:
            df[column] = df[column].apply(json.dumps)
        return df

    def set_warnings_and_failures(self, checkpoint_result: CheckpointResult) -> None:
        """Sets class attributes for warnings and failures given a CheckpointResult

        Args:
            checkpoint_result (CheckpointResult): CheckpointResult object
        """
        warning_dict = {self.expectation_suite_name: {}}
        fail_dict = {self.expectation_suite_name: {}}
        expectation_results = checkpoint_result.list_validation_results()[0]["results"]

        for result in expectation_results:
            column = result["expectation_config"]["kwargs"].get(
                "column",
                "/".join(result["expectation_config"]["kwargs"].get("column_list", [])),
            )
            expectation = result["expectation_config"]["expectation_type"]
            if result["success"]:
                if result["result"].get("partial_unexpected_list", None):
                    warning_dict[self.expectation_suite_name].setdefault(
                        column, []
                    ).append(expectation)
            else:
                fail_dict[self.expectation_suite_name].setdefault(column, []).append(
                    expectation
                )

        self.warning_message, self.warnings = self._generate_message(
            warning_dict, "warnings"
        )
        self.failure_message, self.failures = self._generate_message(
            fail_dict, "failures"
        )

    def _generate_message(
        self, result_dict: dict, message_type: str
    ) -> tuple[str, bool]:
        """Generate message and status for warnings or failures."""
        messages = []
        for suite_name, fields_dict in result_dict.items():
            for field, expectations in fields_dict.items():
                messages.append(
                    f"In the {suite_name} dataset, '{field}' has failed values for expectations {', '.join(expectations)}"
                )
        message = (
            (f"Great Expectations data validation has the following {message_type}: ")
            + "; ".join(messages)
            if messages
            else None
        )
        return message, bool(message)

    def run(self) -> None:
        """Run great expectations on a dataset and upload the results to Synapse."""

        if not self.check_if_expectation_suite_exists():
            return

        logger.info(f"Running data validation on {self.expectation_suite_name}")

        gx_df = pd.read_json(self.dataset_path)
        if self.nested_columns:
            gx_df = self.convert_nested_columns_to_json(
                df=gx_df, nested_columns=self.nested_columns
            )

        validator = self.context.sources.pandas_default.read_dataframe(gx_df)
        expectation_suite = self.context.get_expectation_suite(
            self.expectation_suite_name
        )
        validator.expectation_suite = expectation_suite
        validator.validate()
        checkpoint = self.context.add_or_update_checkpoint(
            name=self.expectation_suite_name,
            validator=validator,
        )
        checkpoint_result = checkpoint.run()
        logger.info(
            f"Data validation complete for {self.expectation_suite_name}. Uploading results to Synapse."
        )
        latest_reults_path = self.get_results_path(checkpoint_result)

        self.set_warnings_and_failures(checkpoint_result)

        if self.upload_folder:
            self.upload_results_file_to_synapse(latest_reults_path)
