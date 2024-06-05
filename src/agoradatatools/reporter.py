import datetime
from dataclasses import dataclass, field
from typing import List, Optional

import synapseclient

from agoradatatools.platform_testing import Platform


@dataclass
class DatasetReport:
    """
    This class will be used to collect all of the data needed to populate one row of the Synapse table.

    Attributes:
        timestamp: Timestamp when the processing run took place.
        platform: The platform where the processing was run.
        run_id: The id of the processing run. This will be passed from the `process` CLI command.
        data_set: The name of the dataset being processed.
        gx_report_file: Synapse ID of the GX report file.
        gx_report_version: Version number of the GX report file.
        gx_report_link: URL of the specific version of the GX report file.
        gx_failures: Whether or not the GX run had any failed expectations.
        gx_failure_message: Message of the GX run if any expectations failed.
        gx_warnings: Whether or not the GX run had any warnings on from the expectations.
        adt_output_file: Synapse ID of the ADT output file.
        adt_output_version: Version number of the ADT output file.
        adt_output_link: URL of the specific version of the ADT output file.
        data_manifest_file: Synapse ID of the data manifest file.
        data_manifest_version: Version number of the data manifest file.
        data_manifest_link: URL of the specific version of the data manifest file.
    """

    timestamp: Optional[datetime.datetime] = field(default=None)
    platform: Optional[Platform] = field(default=None)
    run_id: Optional[str] = field(default=None)
    data_set: Optional[str] = field(default=None)
    gx_report_file: Optional[str] = field(default=None)
    gx_report_version: Optional[int] = field(default=None)
    gx_report_link: Optional[str] = field(default=None)
    gx_failures: Optional[bool] = field(default=False)
    gx_failure_message: Optional[str] = field(default=None)
    gx_warnings: Optional[bool] = field(default=False)
    adt_output_file: Optional[str] = field(default=None)
    adt_output_version: Optional[int] = field(default=None)
    adt_output_link: Optional[str] = field(default=None)
    data_manifest_file: Optional[str] = field(default=None)
    data_manifest_version: Optional[int] = field(default=None)
    data_manifest_link: Optional[str] = field(default=None)

    def set_attributes(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def format_link(cls, syn_id: str, version: int) -> str:
        return f"https://www.synapse.org/Synapse:{syn_id}.{version}"

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "platform": self.platform.value,
            "run_id": self.run_id,
            "data_set": self.data_set,
            "gx_report_file": self.gx_report_file,
            "gx_report_version": self.gx_report_version,
            "gx_report_link": self.gx_report_link,
            "gx_failures": self.gx_failures,
            "gx_failure_message": self.gx_failure_message,
            "gx_warnings": self.gx_warnings,
            "adt_output_file": self.adt_output_file,
            "adt_output_version": self.adt_output_version,
            "adt_output_link": self.adt_output_link,
            "data_manifest_file": self.data_manifest_file,
            "data_manifest_version": self.data_manifest_version,
            "data_manifest_link": self.data_manifest_link,
        }


@dataclass
class ADTGXReporter:
    """
    This class will be used to collect all of the data needed to populate the Synapse table.

    Attributes:
        syn: Synapse session object.
        platform: The platform where the processing was run.
        run_id: The id of the processing run. This will be passed from the `process` CLI command.
        table_id: Synapse ID of the Synapse table to be updated.
        adt_manifest_file: Synapse ID of the ADT manifest file.
        adt_manifest_version: Version number of the ADT manifest file.
        adt_manifest_link: URL of the specific version of the ADT manifest file.
        reports: List of DatasetReport objects to be added to the table.
    """

    syn: synapseclient.Synapse
    platform: Platform
    run_id: str
    table_id: str
    adt_manifest_file: Optional[str] = field(default=None)
    adt_manifest_version: Optional[int] = field(default=None)
    adt_manifest_link: Optional[str] = field(default=None)
    reports: Optional[List[DatasetReport]] = field(default_factory=list)

    def generate_report(self):
        return DatasetReport(platform=self.platform, run_id=self.run_id)

    def add_report(self, report: DatasetReport):
        self.reports.append(report)
        print(self.reports)
        breakpoint()

    def update_table(self):
        timestamp = datetime.datetime.now()
        for report in self.reports:
            report.timestamp = timestamp
            report.platform = self.platform.value
            report.run_id = self.run_id
            report.adt_manifest_file = self.adt_manifest_file
            report.adt_manifest_version = self.adt_manifest_version
            report.adt_manifest_link = self.adt_manifest_link
        rows = []
        for report in self.reports:
            row = report.to_dict()
            rows.append(row.values())
        print(rows)
        breakpoint()
        self.syn.store(
            synapseclient.Table(
                self.table_id,
                rows,
            )
        )
