"""Tests for the DuckDB schema creation."""

import duckdb
import pytest

from opcua_crawler.schema import create_schema


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


class TestCreateSchema:
    """Verify that ``create_schema`` creates the expected tables."""

    def test_tables_exist(self) -> None:
        conn = _connect()
        create_schema(conn)
        tables = {
            r[0] for r in conn.execute("SHOW TABLES").fetchall()
        }
        assert "nodes" in tables
        assert "edges" in tables

    def test_nodes_columns(self) -> None:
        conn = _connect()
        create_schema(conn)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'nodes'"
            ).fetchall()
        }
        expected = {
            "id",
            "nodeid",
            "nodeclass",
            "browsename",
            "displayname",
            "description",
            "writemask",
            "isabstract",
            "symmetric",
            "inversename",
            "containsnoloops",
            "eventnotifier",
            "datavalue",
            "datatype",
            "valuerank",
            "arraydimensions",
            "accesslevel",
            "minimumsamplinginterval",
            "historizing",
            "executable",
            "itemname",
            "aclid",
            "serialized",
            "valsrc",
            "eusrc",
            "defsrc",
            "typeid",
            "parentid",
            "properties",
            "descendants",
        }
        assert expected.issubset(cols)

    def test_edges_columns(self) -> None:
        conn = _connect()
        create_schema(conn)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'edges'"
            ).fetchall()
        }
        assert cols == {"parentid", "childid", "referenceid"}

    def test_idempotent(self) -> None:
        conn = _connect()
        create_schema(conn)
        create_schema(conn)  # calling twice should not raise
        tables = {
            r[0] for r in conn.execute("SHOW TABLES").fetchall()
        }
        assert "nodes" in tables
        assert "edges" in tables

    def test_insert_and_query_node(self) -> None:
        conn = _connect()
        create_schema(conn)
        conn.execute(
            """
            INSERT INTO nodes (id, nodeid, nodeclass, browsename,
                               displayname, description)
            VALUES (1, 'i=85', 1, '0:Objects', 'Objects', 'The Objects folder')
            """
        )
        row = conn.execute("SELECT * FROM nodes WHERE id = 1").fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "i=85"

    def test_insert_and_query_edge(self) -> None:
        conn = _connect()
        create_schema(conn)
        conn.execute("INSERT INTO edges VALUES (1, 2, 3)")
        row = conn.execute("SELECT * FROM edges").fetchone()
        assert row == (1, 2, 3)

    def test_descendants_column_accepts_list(self) -> None:
        conn = _connect()
        create_schema(conn)
        conn.execute(
            "INSERT INTO nodes (id, nodeid, descendants) "
            "VALUES (1, 'i=85', [2, 3, 4])"
        )
        row = conn.execute(
            "SELECT descendants FROM nodes WHERE id = 1"
        ).fetchone()
        assert row is not None
        assert list(row[0]) == [2, 3, 4]

    def test_properties_column_accepts_json(self) -> None:
        conn = _connect()
        create_schema(conn)
        import json

        props = json.dumps({"Speed": {"nodeid": "i=100", "value": 42}})
        conn.execute(
            "INSERT INTO nodes (id, nodeid, properties) VALUES (1, 'i=85', $1)",
            [props],
        )
        row = conn.execute(
            "SELECT properties FROM nodes WHERE id = 1"
        ).fetchone()
        assert row is not None
        parsed = json.loads(str(row[0]))
        assert parsed["Speed"]["value"] == 42
