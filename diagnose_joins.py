"""Utility to diagnose missing join keys in Salesforce exports."""

from __future__ import annotations

import argparse
import logging
from itertools import product
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

from salesforce_exporter.config import AppConfig, QueryConfig, QueryJoinConfig

LOGGER = logging.getLogger(__name__)


class DiagnosticRunner:
    """Run Salesforce queries and validate join keys."""

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

    def run_queries(self) -> Dict[str, pd.DataFrame]:
        results_cache: Dict[str, pd.DataFrame] = {}
        for query_config in self.config.queries:
            df = self._run_query_with_relationships(query_config, results_cache)
            results_cache[query_config.name] = df
        return results_cache

    def _run_query_with_relationships(
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
            LOGGER.warning(
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
        formatted = ", ".join(DiagnosticRunner._quote(value) for value in values)
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


def _summarize_columns(df: pd.DataFrame) -> str:
    if df.empty and df.columns.empty:
        return "(no columns)"
    return ", ".join(df.columns)


def _log_query_summary(name: str, df: pd.DataFrame) -> None:
    LOGGER.info(
        "Query %s returned %d rows with %d columns",
        name,
        len(df.index),
        len(df.columns),
    )
    LOGGER.info("Query %s columns: %s", name, _summarize_columns(df))
    if "Id" not in df.columns:
        LOGGER.warning("Query %s is missing required column: Id", name)


def _validate_join(
    combined_name: str,
    result_df: pd.DataFrame,
    other_df: Optional[pd.DataFrame],
    join: QueryJoinConfig,
) -> Tuple[List[str], List[str]]:
    missing_left = [field for field in join.left_on if field not in result_df.columns]

    if other_df is None:
        missing_right = list(join.right_on)
    else:
        missing_right = [
            field for field in join.right_on if field not in other_df.columns
        ]

    if missing_left or missing_right:
        LOGGER.warning(
            "Combined output %s join with %s is missing keys: left=%s right=%s",
            combined_name,
            join.source_query,
            missing_left or "-",
            missing_right or "-",
        )
    return missing_left, missing_right


def _validate_joins(
    config: AppConfig, results_cache: Dict[str, pd.DataFrame]
) -> None:
    for combined in config.combined_outputs:
        result_df = results_cache.get(combined.base_query)
        if result_df is None:
            LOGGER.warning(
                "Combined output %s base query %s has no cached results",
                combined.name,
                combined.base_query,
            )
            continue

        for join in combined.joins:
            other_df = results_cache.get(join.source_query)
            _validate_join(combined.name, result_df, other_df, join)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose missing join keys in Salesforce export configuration."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    config = AppConfig.load(args.config)
    runner = DiagnosticRunner(config)

    results_cache = runner.run_queries()
    for name, df in results_cache.items():
        _log_query_summary(name, df)

    _validate_joins(config, results_cache)


if __name__ == "__main__":
    main()
