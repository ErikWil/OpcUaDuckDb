"""Tests for the crawler logic.

These tests verify the post-processing phases (descendants computation,
transitive closure) and record construction *without* connecting to a real
OPC UA server.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from opcua_crawler.crawler import OpcUaCrawler, _json_safe
from opcua_crawler.schema import create_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _populated_db() -> tuple[duckdb.DuckDBPyConnection, OpcUaCrawler]:
    """Return a DuckDB connection with a small hand-crafted graph and a
    crawler wired to that connection.

    Graph::

        1 ──(ref 10)──▸ 2 ──(ref 10)──▸ 3
        │                               ▲
        └───────(ref 11)──▸ 4 ──(ref 10)┘
    """
    conn = duckdb.connect(":memory:")
    create_schema(conn)

    for nid in (1, 2, 3, 4):
        conn.execute(
            "INSERT INTO nodes (id, nodeid) VALUES ($1, $2)",
            [nid, f"i={nid}"],
        )

    conn.execute("INSERT INTO edges VALUES (1, 2, 10)")
    conn.execute("INSERT INTO edges VALUES (2, 3, 10)")
    conn.execute("INSERT INTO edges VALUES (1, 4, 11)")
    conn.execute("INSERT INTO edges VALUES (4, 3, 10)")

    crawler = OpcUaCrawler.__new__(OpcUaCrawler)
    crawler._conn = conn
    return conn, crawler


# ---------------------------------------------------------------------------
# Tests: descendants
# ---------------------------------------------------------------------------


class TestDescendants:
    def test_compute_descendants(self) -> None:
        conn, crawler = _populated_db()
        crawler._compute_descendants()

        def _desc(nid: int) -> list[int]:
            row = conn.execute(
                "SELECT descendants FROM nodes WHERE id = $1", [nid]
            ).fetchone()
            val = row[0] if row else None
            return sorted(val) if val else []

        assert _desc(1) == [2, 3, 4]
        assert _desc(2) == [3]
        assert _desc(3) == []
        assert _desc(4) == [3]

    def test_leaf_nodes_have_empty_descendants(self) -> None:
        conn, crawler = _populated_db()
        crawler._compute_descendants()

        row = conn.execute(
            "SELECT descendants FROM nodes WHERE id = 3"
        ).fetchone()
        # Leaf node: either NULL or empty list
        val = row[0]
        assert val is None or list(val) == []


# ---------------------------------------------------------------------------
# Tests: transitive closure
# ---------------------------------------------------------------------------


class TestClosure:
    def test_closure_adds_missing_edges(self) -> None:
        conn, crawler = _populated_db()
        crawler._compute_closure()

        edges = set(
            conn.execute("SELECT parentid, childid FROM edges").fetchall()
        )
        # Original edges
        assert (1, 2) in edges
        assert (2, 3) in edges
        assert (1, 4) in edges
        assert (4, 3) in edges

        # Closure edges
        assert (1, 3) in edges  # 1 → 2 → 3

    def test_closure_uses_most_common_reftype(self) -> None:
        conn, crawler = _populated_db()
        crawler._compute_closure()

        # ref 10 appears 3 times vs ref 11 once  → closure edges get ref 10
        closure_edges = conn.execute(
            "SELECT referenceid FROM edges WHERE parentid = 1 AND childid = 3"
        ).fetchall()
        assert closure_edges
        assert closure_edges[0][0] == 10

    def test_closure_no_duplicates(self) -> None:
        conn, crawler = _populated_db()
        crawler._compute_closure()

        dupes = conn.execute(
            "SELECT parentid, childid, COUNT(*) AS c "
            "FROM edges GROUP BY parentid, childid HAVING c > 1"
        ).fetchall()
        assert dupes == []


# ---------------------------------------------------------------------------
# Tests: node record construction
# ---------------------------------------------------------------------------


class TestMakeNodeRecord:
    def test_basic_record(self) -> None:
        crawler = OpcUaCrawler.__new__(OpcUaCrawler)
        attrs = {
            "nodeclass": 1,
            "browsename": "0:Objects",
            "displayname": "Objects",
            "description": "The Objects folder",
            "writemask": 0,
            "value": 42,
        }
        rec = crawler._make_node_record(
            internal_id=1,
            nodeid="i=85",
            attrs=attrs,
            typeid=None,
            parentid=None,
            properties={"Speed": {"nodeid": "i=100", "value": 10}},
        )
        assert rec["id"] == 1
        assert rec["nodeid"] == "i=85"
        assert rec["nodeclass"] == 1
        assert rec["browsename"] == "0:Objects"
        assert rec["displayname"] == "Objects"
        assert rec["typeid"] is None
        assert rec["parentid"] is None

        props = json.loads(rec["properties"])
        assert props["Speed"]["value"] == 10

    def test_empty_properties_stored_as_none(self) -> None:
        crawler = OpcUaCrawler.__new__(OpcUaCrawler)
        rec = crawler._make_node_record(1, "i=1", {}, None, None, {})
        assert rec["properties"] is None


# ---------------------------------------------------------------------------
# Tests: _json_safe helper
# ---------------------------------------------------------------------------


class TestJsonSafe:
    def test_primitives(self) -> None:
        assert _json_safe(None) is None
        assert _json_safe(42) == 42
        assert _json_safe(3.14) == 3.14
        assert _json_safe("hello") == "hello"
        assert _json_safe(True) is True

    def test_bytes_to_hex(self) -> None:
        assert _json_safe(b"\xde\xad") == "dead"

    def test_list(self) -> None:
        assert _json_safe([1, "a", None]) == [1, "a", None]

    def test_dict(self) -> None:
        assert _json_safe({"k": 1}) == {"k": 1}

    def test_object_with_to_string(self) -> None:
        obj = MagicMock()
        obj.to_string.return_value = "i=42"
        assert _json_safe(obj) == "i=42"

    def test_object_with_text_attr(self) -> None:
        obj = MagicMock(spec=[])
        obj.Text = "hello"
        assert _json_safe(obj) == "hello"

    def test_fallback_to_str(self) -> None:
        class Custom:
            def __str__(self) -> str:
                return "custom_value"

        assert _json_safe(Custom()) == "custom_value"


# ---------------------------------------------------------------------------
# Tests: CLI argument parsing
# ---------------------------------------------------------------------------


class TestCli:
    def test_parse_args(self) -> None:
        from opcua_crawler.__main__ import _parse_args

        args = _parse_args(["opc.tcp://localhost:4840", "out.duckdb"])
        assert args.endpoint == "opc.tcp://localhost:4840"
        assert args.database == "out.duckdb"
        assert args.skip_types == []
        assert args.verbose is False

    def test_parse_args_with_skip_types(self) -> None:
        from opcua_crawler.__main__ import _parse_args

        args = _parse_args([
            "opc.tcp://localhost:4840",
            "out.duckdb",
            "--skip-type", "i=58",
            "--skip-type", "i=62",
            "-v",
        ])
        assert set(args.skip_types) == {"i=58", "i=62"}
        assert args.verbose is True


# ---------------------------------------------------------------------------
# Tests: flush
# ---------------------------------------------------------------------------


class TestFlush:
    def test_flush_inserts_nodes_and_edges(self) -> None:
        conn = duckdb.connect(":memory:")
        create_schema(conn)

        crawler = OpcUaCrawler.__new__(OpcUaCrawler)
        crawler._conn = conn
        crawler._nodes_buf = [
            {
                "id": 1,
                "nodeid": "i=85",
                "nodeclass": 1,
                "browsename": "0:Objects",
                "displayname": "Objects",
                "description": None,
                "writemask": None,
                "isabstract": None,
                "symmetric": None,
                "inversename": None,
                "containsnoloops": None,
                "eventnotifier": None,
                "datavalue": None,
                "datatype": None,
                "valuerank": None,
                "arraydimensions": None,
                "accesslevel": None,
                "minimumsamplinginterval": None,
                "historizing": None,
                "executable": None,
                "itemname": None,
                "aclid": None,
                "serialized": None,
                "valsrc": None,
                "eusrc": None,
                "defsrc": None,
                "typeid": None,
                "parentid": None,
                "properties": None,
                "descendants": None,
            },
        ]
        crawler._edges_buf = [{"parentid": 1, "childid": 2, "referenceid": 10}]

        crawler._flush()

        nodes = conn.execute("SELECT * FROM nodes").fetchall()
        assert len(nodes) == 1
        assert nodes[0][0] == 1
        assert nodes[0][1] == "i=85"

        edges = conn.execute("SELECT * FROM edges").fetchall()
        assert len(edges) == 1
        assert edges[0] == (1, 2, 10)
