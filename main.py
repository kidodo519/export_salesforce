"""Command line entry point for exporting Salesforce data to S3."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from salesforce_exporter.config import AppConfig, FacilityExportConfig
from salesforce_exporter.exporter import SalesforceExporter

LOGGER = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )


def default_config_path() -> Path:
    base_dir = Path(__file__).parent
    facility_config = base_dir / "config_facility.yaml"
    if facility_config.exists():
        return facility_config
    return base_dir / "config.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Salesforce data to S3")
    parser.add_argument(
        "--config",
        type=Path,
        default=default_config_path(),
        help="Path to config_facility.yaml or a single facility YAML file",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def run_single_config(config_path: Path) -> None:
    config = AppConfig.load(config_path)
    exporter = SalesforceExporter(config)
    exporter.run()


def run_facility_configs(config_path: Path) -> None:
    facility_config = FacilityExportConfig.load(config_path)
    enabled_facilities = facility_config.enabled_facilities()
    if not enabled_facilities:
        LOGGER.warning("No facilities are enabled in %s", config_path)
        return

    for facility in enabled_facilities:
        if not facility.config_path.exists():
            raise FileNotFoundError(
                f"Facility config '{facility.config_path}' was not found "
                f"for {facility.name} ({facility.key})"
            )

        LOGGER.info(
            "Starting export for %s (%s) with %s",
            facility.name,
            facility.key,
            facility.config_path,
        )
        app_config = AppConfig.load(
            facility.config_path,
            facility_name=facility.name,
            facility_key=facility.key,
        )
        exporter = SalesforceExporter(app_config)
        exporter.run()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    if not args.config.exists():
        raise FileNotFoundError(f"Configuration file '{args.config}' was not found")

    if FacilityExportConfig.is_facility_config(args.config):
        run_facility_configs(args.config)
    else:
        run_single_config(args.config)


if __name__ == "__main__":
    main()
