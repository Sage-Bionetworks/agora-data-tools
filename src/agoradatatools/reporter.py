import datetime
from dataclasses import dataclass, field, asdict
from typing import List, Optional

import synapseclient

from agoradatatools.constants import Platform


@dataclass
class DatasetReport:
    """
    Holds all of the data needed to populate one row of the ADT GX Reporting Synapse table.

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
    adt_output_file: Optional[str] = field(default=None)
    adt_output_version: Optional[int] = field(default=None)
    adt_output_link: Optional[str] = field(default=None)
    data_manifest_file: Optional[str] = field(default=None)
    data_manifest_version: Optional[int] = field(default=None)
    data_manifest_link: Optional[str] = field(default=None)

    def set_attributes(self, **kwargs) -> None:
        """Set attributes for the DatasetReport object.

        Args:
            **kwargs: Keyword arguments for the DatasetReport object.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def format_link(cls, syn_id: str, version: int) -> str:
        """Generates a link to a specific version of a synapse entity

        Args:
            syn_id (str): Synapse ID of the entity.
            version (int): Version number of the entity.

        Returns:
            str: Formatted link to the specific version of the entity.
        """
        return f"https://www.synapse.org/Synapse:{syn_id}.{version}"


@dataclass
class ADTGXReporter:
    """
    Holds all DatasetReport objects needed to update the Synapse table for an ADT run.
    Includes attributes necessary to execute the upload to the table, as well as data which is common
    to all DatasetReport objects.

    Attributes:
        syn: Synapse session object.
        platform: The platform where the processing was run.
        run_id: The id of the processing run. This will be passed from the `process` CLI command.
        table_id: Synapse ID of the Synapse table to be updated.
        data_manifest_file: Synapse ID of the ADT manifest file.
        data_manifest_version: Version number of the ADT manifest file.
        data_manifest_link: URL of the specific version of the ADT manifest file.
        reports: List of DatasetReport objects to be added to the table.
    """

    syn: synapseclient.Synapse
    platform: Platform
    run_id: str
    table_id: str
    data_manifest_file: Optional[str] = field(default=None)
    data_manifest_version: Optional[int] = field(default=None)
    data_manifest_link: Optional[str] = field(default=None)
    reports: Optional[List[DatasetReport]] = field(default_factory=list)

    def add_report(self, report: DatasetReport) -> None:
        """Adds a DatasetReport object to the list of reports.

        Args:
            report (DatasetReport): DatasetReport object to be added to the list.
        """
        self.reports.append(report)

    def _update_reports_before_upload(self) -> None:
        """Updates the DatasetReport objects with common attributes before uploading to the table."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for report in self.reports:
            report.set_attributes(
                timestamp=timestamp,
                platform=self.platform.value,
                run_id=self.run_id,
                data_manifest_file=self.data_manifest_file,
                data_manifest_version=self.data_manifest_version,
                data_manifest_link=self.data_manifest_link,
            )

    def update_table(self) -> None:
        """Updates the Synapse table adding one new row for each DatasetReport object if the platform is not LOCAL."""
        if self.platform != Platform.LOCAL:
            self._update_reports_before_upload()

            self.syn.store(
                synapseclient.Table(
                    self.table_id,
                    [asdict(report).values() for report in self.reports],
                )
            )
