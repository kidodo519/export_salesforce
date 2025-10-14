"""Configuration loading utilities for the Salesforce exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
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

    def render_where_clause(
        self,
        tz: ZoneInfo,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render the WHERE clause using the configured template."""

        window_days = self.window_days
        end_offset_days = self.end_offset_days
        field = self.field
        where_template = self.where_template

        if overrides:
            if "window_days" in overrides and overrides["window_days"] is not None:
                window_days = int(overrides["window_days"])
            if "end_offset_days" in overrides and overrides["end_offset_days"] is not None:
                end_offset_days = int(overrides["end_offset_days"])
            if "field" in overrides and overrides["field"]:
                field = overrides["field"]
            if "where_template" in overrides and overrides["where_template"]:
                where_template = overrides["where_template"]

        now = datetime.now(tz)
        end_time = now - timedelta(days=end_offset_days)
        start_time = end_time - timedelta(days=window_days)

        replacements = {
            "field": field,
            "start_iso": start_time.isoformat(),
            "end_iso": end_time.isoformat(),
            "start_date": start_time.date().isoformat(),
            "end_date": end_time.date().isoformat(),
        }
        return where_template.format(**replacements)


@dataclass
class QueryIncrementalConfig:
    disabled: bool = False
    field: Optional[str] = None
    where_template: Optional[str] = None
    window_days: Optional[int] = None
    end_offset_days: Optional[int] = None

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryIncrementalConfig":
        if raw is None or raw is True:
            return cls()
        if raw is False:
            return cls(disabled=True)
        if not isinstance(raw, dict):
            raise ValueError("Query incremental configuration must be a mapping or boolean")

        return cls(
            field=raw.get("field"),
            where_template=raw.get("where_template"),
            window_days=raw.get("window_days"),
            end_offset_days=raw.get("end_offset_days"),
        )

    def overrides(self) -> Dict[str, Any]:
        overrides: Dict[str, Any] = {}
        if self.field:
            overrides["field"] = self.field
        if self.where_template:
            overrides["where_template"] = self.where_template
        if self.window_days is not None:
            overrides["window_days"] = self.window_days
        if self.end_offset_days is not None:
            overrides["end_offset_days"] = self.end_offset_days
        return overrides


@dataclass
class QueryRelationshipFilter:
    source_query: str
    source_field: str
    target_field: str
    chunk_size: int = 200

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryRelationshipFilter":
        if not isinstance(raw, dict):
            raise ValueError("Relationship filter configuration must be a mapping")

        try:
            source_query = raw["source_query"]
            source_field = raw["source_field"]
            target_field = raw["target_field"]
        except KeyError as exc:  # pragma: no cover - validated at runtime
            raise ValueError(
                "Relationship filter requires source_query, source_field, and target_field"
            ) from exc

        chunk_size_raw = raw.get("chunk_size")
        chunk_size = int(chunk_size_raw) if chunk_size_raw is not None else 200

        return cls(
            source_query=source_query,
            source_field=source_field,
            target_field=target_field,
            chunk_size=chunk_size,
        )


@dataclass
class QueryConfig:
    name: str
    soql: str
    where: Optional[str] = None
    output_file: Optional[str] = None
    incremental: Optional[QueryIncrementalConfig] = None
    relationship_filters: List[QueryRelationshipFilter] = field(default_factory=list)

    def build_query(self, additional_conditions: Iterable[str] = ()) -> str:
        conditions = []
        if self.where:
            conditions.append(self.where.strip())
        conditions.extend(cond.strip() for cond in additional_conditions if cond and cond.strip())

        if not conditions:
            return self.soql.strip()

        base_soql = self.soql.strip()
        lowered = base_soql.lower()
        if " where " in lowered or lowered.endswith(" where") or "\nwhere " in lowered:
            return f"{base_soql} AND {' AND '.join(conditions)}"
        return f"{base_soql} WHERE {' AND '.join(conditions)}"


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
        queries: List[QueryConfig] = []
        for query_raw in queries_raw:
            incremental_override = (
                QueryIncrementalConfig.from_raw(query_raw.get("incremental"))
                if "incremental" in query_raw
                else None
            )

            relationship_filters_raw = query_raw.get("relationship_filters", [])
            relationship_filters: List[QueryRelationshipFilter] = []
            for filter_raw in relationship_filters_raw:
                relationship_filters.append(
                    QueryRelationshipFilter.from_raw(filter_raw)
                )

            query = QueryConfig(
                name=query_raw["name"],
                soql=query_raw["soql"],
                where=query_raw.get("where"),
                output_file=query_raw.get("output_file"),
                incremental=incremental_override,
                relationship_filters=relationship_filters,
            )
            queries.append(query)

        for query in queries:
            if query.where is not None:
                continue

            if query.incremental and query.incremental.disabled:
                continue

            overrides = query.incremental.overrides() if query.incremental else {}
            if incremental:
                query.where = incremental.render_where_clause(
                    timezone, overrides=overrides or None
                )
            elif overrides:
                missing_keys = {
                    key
                    for key in ("field", "where_template")
                    if not overrides.get(key)
                }
                if missing_keys:
                    missing = ", ".join(sorted(missing_keys))
                    raise ValueError(
                        "Per-query incremental configuration requires "
                        f"{missing} when no global incremental settings are defined"
                    )

                temp_incremental = IncrementalConfig(
                    field=overrides["field"],
                    where_template=overrides["where_template"],
                    window_days=int(overrides.get("window_days", 1)),
                    end_offset_days=int(overrides.get("end_offset_days", 1)),
                )
                query.where = temp_incremental.render_where_clause(timezone)

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
    "QueryIncrementalConfig",
    "QueryConfig",
    "QueryRelationshipFilter",
    "S3Info",
    "SalesforceAuth",
]

