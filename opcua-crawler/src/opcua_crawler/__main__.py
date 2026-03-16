"""Command-line interface for the OPC UA crawler.

Usage::

    python -m opcua_crawler opc.tcp://localhost:4840 namespace.duckdb

Or via the installed console script::

    opcua-crawler opc.tcp://localhost:4840 namespace.duckdb
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from opcua_crawler.crawler import OpcUaCrawler


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="opcua-crawler",
        description="Crawl an OPC UA namespace and build a DuckDB graph database.",
    )
    parser.add_argument(
        "endpoint",
        help="OPC UA server endpoint URL (e.g. opc.tcp://localhost:4840)",
    )
    parser.add_argument(
        "database",
        help="Path to the DuckDB database file (use ':memory:' for in-memory)",
    )
    parser.add_argument(
        "--skip-type",
        action="append",
        default=[],
        dest="skip_types",
        metavar="NODEID",
        help=(
            "NodeId of a type to skip during traversal.  "
            "May be specified multiple times."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    crawler = OpcUaCrawler(
        endpoint_url=args.endpoint,
        db_path=args.database,
        skip_types=set(args.skip_types),
    )
    asyncio.run(crawler.crawl())
    logging.getLogger(__name__).info("Done – database written to %s", args.database)


if __name__ == "__main__":
    main()
