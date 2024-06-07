import pytest
import datetime

from unittest.mock import patch

import agoradatatools.reporter

from agoradatatools.reporter import ADTGXReporter, DatasetReport
from agoradatatools.run_platform import Platform


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
            data_manifest_file="syn456",
            data_manifest_version=1,
            data_manifest_link="test_link",
        )
        self.test_reporter_local = ADTGXReporter(
            syn=syn,
            platform=Platform.LOCAL,
            run_id="test_run_id",
            table_id="syn123",
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
        mock_datetime.datetime.now.return_value.strftime.return_value = "test_timestamp"

        self.test_reporter.reports = [self.test_report]
        self.test_reporter._update_reports_before_upload()

        mock_datetime.datetime.now.return_value.strftime.assert_called_once()
        assert self.test_reporter.reports[0] == self.upload_report

    def test_update_table(self, syn):
        with patch.object(syn, "store") as mock_store, patch.object(
            self.test_reporter, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter.update_table()

            mock_store.assert_called_once()
            mock_update_reports_before_upload.assert_called_once()

    def test_update_table_when_platform_is_local(self, syn):
        with patch.object(syn, "store") as mock_store, patch.object(
            self.test_reporter_local, "_update_reports_before_upload"
        ) as mock_update_reports_before_upload:
            self.test_reporter_local.update_table()

            mock_store.assert_not_called()
            mock_update_reports_before_upload.assert_not_called()
