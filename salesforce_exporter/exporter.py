"""Main Salesforce exporting logic."""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from itertools import product
from typing import Dict, Iterable, List, Optional

import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

from .config import AppConfig, QueryConfig
from .s3_uploader import upload_to_s3

LOGGER = logging.getLogger(__name__)


class SalesforceExporter:
    """Export data from Salesforce and upload it to S3."""

    
    def __init__(self, config: AppConfig) -> None:
        self.config = config

        login_kwargs = {
            "username": config.salesforce.username,
            "password": config.salesforce.password,
            "domain": config.salesforce.domain,
        }
        if config.salesforce.security_token:
            login_kwargs["security_token"] = config.salesforce.security_token

        session_id, instance = SalesforceLogin(**login_kwargs)
        self.sf = Salesforce(instance=instance, session_id=session_id)


    def run(self) -> None:
        LOGGER.info("Starting Salesforce export for %d query(ies)", len(self.config.queries))
        self.config.csv.output_directory.mkdir(parents=True, exist_ok=True)
        if self.config.csv.archive_directory:
            self.config.csv.archive_directory.mkdir(parents=True, exist_ok=True)

        results_cache: Dict[str, pd.DataFrame] = {}
        for query_config in self.config.queries:
            df = self._export_query(query_config, results_cache)
            results_cache[query_config.name] = df

    def _export_query(
        self,
        query_config: QueryConfig,
        results_cache: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        batches = self._build_relationship_batches(query_config, results_cache)

        dataframes: List[pd.DataFrame] = []
        if batches is None:
            batches_to_process = [()]
            chunked = False
        elif not batches:
            LOGGER.info(
                "Skipping query %s because no related records were found",
                query_config.name,
            )
            return pd.DataFrame()
        else:
            batches_to_process = product(*batches)
            chunked = True

        for index, additional_conditions in enumerate(batches_to_process, start=1):
            batch_index = index if chunked else None
            df = self._run_single_query(
                query_config, additional_conditions, batch_index=batch_index
            )
            if not df.empty:
                dataframes.append(df)

        if not dataframes:
            LOGGER.warning("Query %s returned no data", query_config.name)
            return pd.DataFrame()

        combined = pd.concat(dataframes, ignore_index=True)
        self._write_output(query_config, combined)
        return combined

    def _build_relationship_batches(
        self,
        query_config: QueryConfig,
        results_cache: Dict[str, pd.DataFrame],
    ) -> Optional[List[List[str]]]:
        if not query_config.relationship_filters:
            return None

        batches: List[List[str]] = []
        for filter_config in query_config.relationship_filters:
            source_df = results_cache.get(filter_config.source_query)
            if source_df is None:
                raise ValueError(
                    f"Query '{query_config.name}' depends on '{filter_config.source_query}' "
                    "which has not been executed yet"
                )

            if source_df.empty:
                return []

            if filter_config.source_field not in source_df.columns:
                raise ValueError(
                    f"Field '{filter_config.source_field}' not found in results of "
                    f"query '{filter_config.source_query}'"
                )

            values_series = source_df[filter_config.source_field].dropna()
            if values_series.empty:
                return []

            values = [str(value) for value in values_series.tolist() if str(value)]
            if not values:
                return []

            unique_values = list(dict.fromkeys(values))
            chunked_conditions: List[str] = []
            for start in range(0, len(unique_values), filter_config.chunk_size):
                chunk = unique_values[start : start + filter_config.chunk_size]
                condition = self._build_in_condition(filter_config.target_field, chunk)
                chunked_conditions.append(condition)
            batches.append(chunked_conditions)

        return batches

    @staticmethod
    def _build_in_condition(field: str, values: Iterable[str]) -> str:
        formatted = ", ".join(SalesforceExporter._quote(value) for value in values)
        return f"{field} IN ({formatted})"

    @staticmethod
    def _quote(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _run_single_query(
        self,
        query_config: QueryConfig,
        additional_conditions: Iterable[str],
        batch_index: Optional[int] = None,
    ) -> pd.DataFrame:
        query = query_config.build_query(additional_conditions)
        if batch_index is None:
            LOGGER.info("Running query for %s", query_config.name)
        else:
            LOGGER.info(
                "Running query for %s (batch %d)", query_config.name, batch_index
            )
        LOGGER.debug("SOQL: %s", query)

        results = self.sf.query_all(query)
        records = results.get("records", [])
        for record in records:
            record.pop("attributes", None)

        if not records:
            return pd.DataFrame()

        return pd.DataFrame(records)

    def _write_output(self, query_config: QueryConfig, df: pd.DataFrame) -> None:
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
