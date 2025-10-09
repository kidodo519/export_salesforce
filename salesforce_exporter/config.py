"""Configuration loading utilities for the Salesforce exporter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 not supported here
    from backports.zoneinfo import ZoneInfo  # type: ignore


@dataclass
class S3Info:
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    file_name_prefix: str


@dataclass
class CsvConfig:
    output_directory: Path
    archive_directory: Optional[Path] = None


@dataclass
class IncrementalConfig:
    field: str
    where_template: str
    window_days: int = 1
    end_offset_days: int = 1

    def render_where_clause(self, tz: ZoneInfo) -> str:
        """Render the WHERE clause using the configured template."""

        now = datetime.now(tz)
        end_time = now - timedelta(days=self.end_offset_days)
        start_time = end_time - timedelta(days=self.window_days)

        replacements = {
            "field": self.field,
            "start_iso": start_time.isoformat(),
            "end_iso": end_time.isoformat(),
            "start_date": start_time.date().isoformat(),
            "end_date": end_time.date().isoformat(),
        }
        return self.where_template.format(**replacements)


@dataclass
class QueryConfig:
    name: str
    soql: str
    where: Optional[str] = None
    output_file: Optional[str] = None

    def build_query(self) -> str:
        if self.where:
            if " where " in self.soql.lower():
                return f"{self.soql} AND {self.where}"
            return f"{self.soql} WHERE {self.where}"
        return self.soql


@dataclass
class SalesforceAuth:
    username: str
    password: str
    security_token: Optional[str] = None
    domain: str = "login"


@dataclass
class AppConfig:
    s3: S3Info
    csv: CsvConfig
    salesforce: SalesforceAuth
    queries: List[QueryConfig]
    timezone: ZoneInfo
    incremental: Optional[IncrementalConfig] = None

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        with path.open("r", encoding="utf-8") as fp:
            raw_config: Dict[str, Any] = yaml.safe_load(fp)

        base_dir = path.parent

        s3_info = S3Info(
            bucket_name=raw_config["s3_info"]["bucket_name"],
            access_key_id=raw_config["s3_info"]["access_key_id"],
            secret_access_key=raw_config["s3_info"]["secret_access_key"],
            file_name_prefix=raw_config["s3_info"]["file_name"],
        )

        csv_config_raw = raw_config.get("csv", {})
        output_directory = Path(csv_config_raw.get("output_directory", "output")).expanduser()
        if not output_directory.is_absolute():
            output_directory = (base_dir / output_directory).resolve()
        archive_directory = csv_config_raw.get("archive_directory")
        archive_path = None
        if archive_directory:
            archive_path = Path(archive_directory).expanduser()
            if not archive_path.is_absolute():
                archive_path = (base_dir / archive_path).resolve()

        csv_config = CsvConfig(
            output_directory=output_directory,
            archive_directory=archive_path,
        )

        tz_name = raw_config.get("timezone", "UTC")
        timezone = ZoneInfo(tz_name)

        incremental_raw = raw_config.get("incremental")
        incremental = None
        if incremental_raw:
            incremental = IncrementalConfig(
                field=incremental_raw["field"],
                where_template=incremental_raw["where_template"],
                window_days=int(incremental_raw.get("window_days", 1)),
                end_offset_days=int(incremental_raw.get("end_offset_days", 1)),
            )

        salesforce_raw = raw_config["salesforce"]
        security_token = salesforce_raw.get("security_token")
        if security_token == "":
            security_token = None

        salesforce_auth = SalesforceAuth(
            username=salesforce_raw["username"],
            password=salesforce_raw["password"],
            security_token=security_token,
            domain=salesforce_raw.get("domain", "login"),
        )

        queries_raw: Iterable[Dict[str, Any]] = raw_config.get("queries", [])
        queries = [
            QueryConfig(
                name=query_raw["name"],
                soql=query_raw["soql"],
                where=query_raw.get("where"),
                output_file=query_raw.get("output_file"),
            )
            for query_raw in queries_raw
        ]

        if incremental:
            rendered_where = incremental.render_where_clause(timezone)
            for query in queries:
                if query.where is None:
                    query.where = rendered_where

        return cls(
            s3=s3_info,
            csv=csv_config,
            salesforce=salesforce_auth,
            queries=queries,
            timezone=timezone,
            incremental=incremental,
        )


__all__ = [
    "AppConfig",
    "CsvConfig",
    "IncrementalConfig",
    "QueryConfig",
    "S3Info",
    "SalesforceAuth",
]
