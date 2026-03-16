"""OPC UA namespace crawler that builds a graph database in DuckDB."""

from opcua_crawler.schema import create_schema
from opcua_crawler.crawler import OpcUaCrawler

__all__ = ["create_schema", "OpcUaCrawler"]
