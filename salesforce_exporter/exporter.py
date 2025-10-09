"""Main Salesforce exporting logic."""

from __future__ import annotations

import csv
import logging
from datetime import datetime

import pandas as pd
from simple_salesforce import Salesforce

from .config import AppConfig, QueryConfig
from .s3_uploader import upload_to_s3

LOGGER = logging.getLogger(__name__)


class SalesforceExporter:
    """Export data from Salesforce and upload it to S3."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.sf = Salesforce(
            username=config.salesforce.username,
            password=config.salesforce.password,
            security_token=config.salesforce.security_token,
            domain=config.salesforce.domain,
        )

    def run(self) -> None:
        LOGGER.info("Starting Salesforce export for %d query(ies)", len(self.config.queries))
        self.config.csv.output_directory.mkdir(parents=True, exist_ok=True)
        if self.config.csv.archive_directory:
            self.config.csv.archive_directory.mkdir(parents=True, exist_ok=True)

        for query_config in self.config.queries:
            self._export_query(query_config)

    def _export_query(self, query_config: QueryConfig) -> None:
        query = query_config.build_query()
        LOGGER.info("Running query for %s", query_config.name)

        results = self.sf.query_all(query)
        records = results.get("records", [])
        for record in records:
            record.pop("attributes", None)

        if not records:
            LOGGER.warning("Query %s returned no data", query_config.name)
            return

        df = pd.DataFrame(records)

        timestamp = datetime.now(self.config.timezone).strftime("%Y%m%d%H%M%S")
        output_name = query_config.output_file or query_config.name
        local_filename = f"{output_name}_{timestamp}.csv"
        local_path = self.config.csv.output_directory / local_filename
        LOGGER.info("Writing %d rows to %s", len(df.index), local_path)
        df.to_csv(local_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

        remote_filename = f"{self.config.s3.file_name_prefix}{local_filename}"
        uploaded = upload_to_s3(local_path, self.config.s3, remote_filename)

        if uploaded and self.config.csv.archive_directory:
            destination = self.config.csv.archive_directory / local_filename
            LOGGER.info("Moving %s to %s", local_path, destination)
            local_path.replace(destination)


__all__ = ["SalesforceExporter"]
