"""DuckDB schema definitions for the OPC UA graph database.

Creates two tables:
- ``nodes``: vertices representing OPC UA nodes with all standard attributes.
- ``edges``: directed edges representing OPC UA references between nodes.
"""

from __future__ import annotations

import duckdb

NODES_DDL = """
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY,
    nodeid VARCHAR,
    nodeclass INTEGER,
    browsename VARCHAR,
    displayname VARCHAR,
    "description" VARCHAR,
    writemask INTEGER,
    isabstract INTEGER,
    "symmetric" INTEGER,
    inversename VARCHAR,
    containsnoloops INTEGER,
    eventnotifier INTEGER,
    datavalue BLOB,
    datatype VARCHAR,
    valuerank INTEGER,
    arraydimensions VARCHAR,
    accesslevel INTEGER,
    minimumsamplinginterval INTEGER,
    historizing INTEGER,
    executable INTEGER,
    itemname VARCHAR,
    aclid INTEGER,
    serialized INTEGER,
    valsrc INTEGER,
    eusrc VARCHAR,
    defsrc VARCHAR,
    typeid INTEGER,
    parentid INTEGER,
    properties JSON,
    descendants INTEGER[]
);
"""

EDGES_DDL = """
CREATE TABLE IF NOT EXISTS edges (
    parentid INTEGER,
    childid INTEGER,
    referenceid INTEGER
);
"""


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the nodes and edges tables if they do not already exist."""
    conn.execute(NODES_DDL)
    conn.execute(EDGES_DDL)
