import pytest
import datetime

from unittest.mock import patch, Mock

import pandas as pd
from synapseclient import Synapse

import agoradatatools.reporter

from agoradatatools.reporter import ADTGXReporter, DatasetReport
from agoradatatools.constants import Platform


class TestDatasetReport:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.test_report = DatasetReport()

    def test_set_attribute(self):
        self.test_report.set_attributes(run_id="test", data_set="test")

        assert self.test_report.run_id == "test"
        assert self.test_report.data_set == "test"

    def test_format_link(self):
        expected = "https://www.synapse.org/Synapse:syn123.1"
        result = self.test_report.format_link("syn123", 1)

        assert result == expected


class TestADTGXReporter:
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn):
        self.test_reporter = ADTGXReporter(
            syn=syn,
            platform=Platform.GITHUB,
            run_id="test_run_id",
            table_id="syn123",
            upload=True,
            data_manifest_file="syn456",
            data_manifest_version=1,
            data_manifest_link="test_link",
        )
        self.test_reporter_no_upload = ADTGXReporter(
            syn=syn,
            platform=Platform.GITHUB,
            run_id="test_run_id",
            table_id="syn123",
            upload=False,
        )
        self.test_report = DatasetReport()
        self.upload_report = DatasetReport(
            timestamp="test_timestamp",
            platform=Platform.GITHUB.value,
            run_id="test_run_id",
            data_manifest_file="syn456",
            data_manifest_version=1,
            data_manifest_link="test_link",
        )

    def test_add_report(self):
        self.test_reporter.add_report(self.test_report)

        assert len(self.test_reporter.reports) == 1
        assert self.test_reporter.reports[0] == self.test_report

    @patch(f"{agoradatatools.reporter.__name__}.datetime", wraps=datetime)
    def test_update_reports_before_upload(self, mock_datetime):
        mock_now = Mock()
        mock_now.strftime.return_value = "test_timestamp"
        mock_datetime.datetime.now.return_value = mock_now  #

        self.test_reporter.reports = [self.test_report]
        self.test_reporter._update_reports_before_upload()

        mock_datetime.datetime.now.return_value.strftime.assert_called_once()
        assert self.test_reporter.reports[0] == self.upload_report

    def test_update_table_when_upload_is_true_and_reports_not_empty(
        self, syn: Synapse
    ) -> None:
        mock_table_instance = Mock()
        with patch.object(
            agoradatatools.reporter, "Table", return_value=mock_table_instance
        ) as mock_table_class, patch.object(
            self.test_reporter, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter.reports = [self.test_report]
            self.test_reporter.update_table()

            mock_table_class.assert_called_once_with(id="syn123")
            mock_table_instance.store_rows.assert_called_once()
            store_rows_kwargs = mock_table_instance.store_rows.call_args.kwargs
            assert store_rows_kwargs["synapse_client"] is syn
            assert isinstance(store_rows_kwargs["values"], pd.DataFrame)
            assert len(store_rows_kwargs["values"]) == 1
            mock_update_reports_before_upload.assert_called_once()

    def test_update_table_when_upload_is_true_and_reports_empty(
        self, syn: Synapse
    ) -> None:
        with patch.object(
            agoradatatools.reporter, "Table"
        ) as mock_table_class, patch.object(
            self.test_reporter, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter.update_table()

            mock_table_class.assert_not_called()
            mock_update_reports_before_upload.assert_not_called()

    def test_update_table_when_upload_is_false_and_reports_not_empty(
        self, syn: Synapse
    ) -> None:
        with patch.object(
            agoradatatools.reporter, "Table"
        ) as mock_table_class, patch.object(
            self.test_reporter_no_upload, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter_no_upload.reports = [self.test_report]
            self.test_reporter_no_upload.update_table()

            mock_table_class.assert_not_called()
            mock_update_reports_before_upload.assert_not_called()

    def test_update_table_when_upload_is_false_and_reports_empty(
        self, syn: Synapse
    ) -> None:
        with patch.object(
            agoradatatools.reporter, "Table"
        ) as mock_table_class, patch.object(
            self.test_reporter_no_upload, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter_no_upload.update_table()

            mock_table_class.assert_not_called()
            mock_update_reports_before_upload.assert_not_called()
