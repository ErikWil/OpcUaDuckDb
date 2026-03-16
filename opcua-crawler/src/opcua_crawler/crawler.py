"""Core crawler that walks the OPC UA address space and stores it in DuckDB.

The crawler performs a depth-first traversal starting at the *Types* folder
(``i=86``) and then the *Objects* folder (``i=85``).  During traversal it:

* Collects all standard OPC UA node attributes.
* Stores *HasProperty* sub-nodes in the parent's ``properties`` JSON column
  instead of creating separate rows in the ``nodes`` table.
* Records *HasTypeDefinition* (and its sub-types) in the node's ``typeid``
  column instead of adding rows to the ``edges`` table.
* Supports skipping entire sub-trees whose type definition matches a
  caller-supplied set.
* After the traversal, computes a ``descendants`` list for every node and
  adds *transitive-closure* edges whose ``referenceid`` equals the most
  common reference type already present in ``edges``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter, deque
from typing import Any, Dict, List, Optional, Set

import duckdb
from asyncua import Client, ua

from opcua_crawler.schema import create_schema

logger = logging.getLogger(__name__)

# Well-known OPC UA node identifiers
TYPES_FOLDER = "i=86"
OBJECTS_FOLDER = "i=85"

# Standard reference-type NodeIds (numeric namespace 0)
_HAS_PROPERTY = 46
_HAS_TYPE_DEFINITION = 40
_HAS_SUBTYPE = 45


class OpcUaCrawler:
    """Crawl an OPC UA server and persist the namespace graph in DuckDB.

    Parameters
    ----------
    endpoint_url:
        OPC UA endpoint, e.g. ``"opc.tcp://localhost:4840"``.
    db_path:
        Path to the DuckDB database file.  Use ``":memory:"`` for an
        in-memory database.
    skip_types:
        Optional set of OPC UA NodeId strings (e.g. ``{"i=58"}``).  Nodes
        whose *HasTypeDefinition* target is in this set are skipped during
        traversal together with their descendants.
    """

    def __init__(
        self,
        endpoint_url: str,
        db_path: str = ":memory:",
        skip_types: Optional[Set[str]] = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.db_path = db_path
        self.skip_types: Set[str] = skip_types or set()

        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._client: Optional[Client] = None

        # Internal bookkeeping
        self._id_counter: int = 0
        self._nodeid_to_id: Dict[str, int] = {}
        self._nodes_buf: List[Dict[str, Any]] = []
        self._edges_buf: List[Dict[str, Any]] = []

        # Set of reference-type NodeIds (ns=0 numeric) that are considered
        # "type-definition" references (HasTypeDefinition + sub-types).
        self._type_def_ref_ids: Set[int] = {_HAS_TYPE_DEFINITION}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def crawl(self) -> duckdb.DuckDBPyConnection:
        """Connect, browse, persist and return the DuckDB connection."""
        self._conn = duckdb.connect(self.db_path)
        create_schema(self._conn)

        self._client = Client(url=self.endpoint_url)
        await self._client.connect()
        try:
            await self._collect_type_def_subtypes()
            await self._browse_recursive(TYPES_FOLDER, parent_id=None)
            await self._browse_recursive(OBJECTS_FOLDER, parent_id=None)
            self._flush()
            self._compute_descendants()
            self._compute_closure()
        finally:
            await self._client.disconnect()

        return self._conn

    # ------------------------------------------------------------------
    # Browsing
    # ------------------------------------------------------------------

    async def _collect_type_def_subtypes(self) -> None:
        """Walk the HasTypeDefinition reference-type node to find sub-types."""
        assert self._client is not None
        await self._walk_subtypes(
            self._client.get_node(ua.NodeId(_HAS_TYPE_DEFINITION, 0))
        )

    async def _walk_subtypes(self, node: Any) -> None:
        """Recursively collect sub-types of a reference-type node."""
        assert self._client is not None
        try:
            refs = await node.get_references(
                refs=ua.ObjectIds.HasSubtype,
                direction=ua.BrowseDirection.Forward,
                includesubtypes=False,
            )
        except Exception:
            return

        for ref in refs:
            nid = ref.NodeId
            if nid.NamespaceIndex == 0 and isinstance(nid.Identifier, int):
                self._type_def_ref_ids.add(nid.Identifier)
            child = self._client.get_node(ref.NodeId)
            await self._walk_subtypes(child)

    async def _browse_recursive(
        self,
        node_id_str: str,
        parent_id: Optional[int],
    ) -> None:
        """Depth-first browse starting at *node_id_str*."""
        if node_id_str in self._nodeid_to_id:
            return  # already visited

        assert self._client is not None
        node = self._client.get_node(node_id_str)

        # Read the basic attributes we need for filtering before insertion.
        attrs = await self._read_attributes(node)

        # --- skip check ------------------------------------------------
        type_def_nodeid = attrs.get("_type_def_nodeid")
        if type_def_nodeid and type_def_nodeid in self.skip_types:
            logger.debug("Skipping %s (type %s)", node_id_str, type_def_nodeid)
            return

        # Assign an internal id and mark as visited.
        internal_id = self._next_id()
        self._nodeid_to_id[node_id_str] = internal_id

        # --- references ------------------------------------------------
        try:
            refs = await node.get_references(
                refs=ua.ObjectIds.References,
                direction=ua.BrowseDirection.Forward,
                includesubtypes=True,
            )
        except Exception:
            refs = []

        type_def_id: Optional[int] = None
        properties: Dict[str, Any] = {}
        children_refs: List[Any] = []

        for ref in refs:
            target_str = ref.NodeId.to_string()
            ref_ns = ref.ReferenceTypeId.NamespaceIndex
            ref_ident = ref.ReferenceTypeId.Identifier

            # HasTypeDefinition (or sub-type) → store in typeid column
            if ref_ns == 0 and isinstance(ref_ident, int) and ref_ident in self._type_def_ref_ids:
                type_def_id = self._nodeid_to_id.get(target_str)
                continue

            # HasProperty → store in the parent's properties JSON
            if ref_ns == 0 and ref_ident == _HAS_PROPERTY:
                prop_value = await self._read_property_value(ref)
                properties[ref.BrowseName.Name] = {
                    "nodeid": target_str,
                    "value": prop_value,
                }
                continue

            children_refs.append(ref)

        # --- insert node -----------------------------------------------
        self._nodes_buf.append(
            self._make_node_record(
                internal_id,
                node_id_str,
                attrs,
                type_def_id,
                parent_id,
                properties,
            )
        )

        # --- recurse into children -------------------------------------
        for ref in children_refs:
            target_str = ref.NodeId.to_string()
            await self._browse_recursive(target_str, internal_id)

            child_id = self._nodeid_to_id.get(target_str)
            if child_id is not None:
                ref_type_internal = self._nodeid_to_id.get(
                    ref.ReferenceTypeId.to_string()
                )
                self._edges_buf.append(
                    {
                        "parentid": internal_id,
                        "childid": child_id,
                        "referenceid": ref_type_internal,
                    }
                )

    # ------------------------------------------------------------------
    # Attribute reading helpers
    # ------------------------------------------------------------------

    async def _read_attributes(self, node: Any) -> Dict[str, Any]:
        """Read standard OPC UA attributes from *node*.

        Returns a plain dict keyed by column name.  The special key
        ``_type_def_nodeid`` carries the raw NodeId string of the type
        definition (used for the skip-type check before the node has an
        internal id).
        """
        attrs: Dict[str, Any] = {}

        # Map of (column_name, AttributeId)
        attribute_ids = [
            ("nodeclass", ua.AttributeIds.NodeClass),
            ("browsename", ua.AttributeIds.BrowseName),
            ("displayname", ua.AttributeIds.DisplayName),
            ("description", ua.AttributeIds.Description),
            ("writemask", ua.AttributeIds.WriteMask),
            ("isabstract", ua.AttributeIds.IsAbstract),
            ("symmetric", ua.AttributeIds.Symmetric),
            ("inversename", ua.AttributeIds.InverseName),
            ("containsnoloops", ua.AttributeIds.ContainsNoLoops),
            ("eventnotifier", ua.AttributeIds.EventNotifier),
            ("value", ua.AttributeIds.Value),
            ("datatype", ua.AttributeIds.DataType),
            ("valuerank", ua.AttributeIds.ValueRank),
            ("arraydimensions", ua.AttributeIds.ArrayDimensions),
            ("accesslevel", ua.AttributeIds.AccessLevel),
            ("minimumsamplinginterval", ua.AttributeIds.MinimumSamplingInterval),
            ("historizing", ua.AttributeIds.Historizing),
            ("executable", ua.AttributeIds.Executable),
        ]

        for col, attr_id in attribute_ids:
            try:
                val = await node.read_attribute(attr_id)
                attrs[col] = self._coerce_attribute(col, val.Value.Value)
            except Exception:
                attrs[col] = None

        # Resolve type definition via HasTypeDefinition references.
        try:
            refs = await node.get_references(
                refs=ua.ObjectIds.HasTypeDefinition,
                direction=ua.BrowseDirection.Forward,
                includesubtypes=True,
            )
            if refs:
                attrs["_type_def_nodeid"] = refs[0].NodeId.to_string()
        except Exception:
            pass

        return attrs

    @staticmethod
    def _coerce_attribute(col: str, value: Any) -> Any:
        """Convert asyncua attribute values to Python/DuckDB-friendly types."""
        if value is None:
            return None
        if col == "nodeclass":
            return int(value) if not isinstance(value, int) else value
        if col in ("browsename",):
            return str(value)
        if col in ("displayname", "description", "inversename"):
            # LocalizedText → plain string
            if hasattr(value, "Text"):
                return value.Text
            return str(value) if value else None
        if col in ("isabstract", "symmetric", "containsnoloops", "historizing", "executable"):
            return int(value) if value is not None else None
        if col in ("writemask", "eventnotifier", "accesslevel", "valuerank", "minimumsamplinginterval"):
            return int(value) if value is not None else None
        if col == "datatype":
            return str(value) if value else None
        if col == "arraydimensions":
            if value:
                return str(value)
            return None
        if col == "value":
            # Stored in datavalue column as blob; skip for now.
            return value
        return value

    async def _read_property_value(self, ref: Any) -> Any:
        """Read the value of a property reference target."""
        assert self._client is not None
        try:
            prop_node = self._client.get_node(ref.NodeId)
            val = await prop_node.read_value()
            return _json_safe(val)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Record construction
    # ------------------------------------------------------------------

    def _make_node_record(
        self,
        internal_id: int,
        nodeid: str,
        attrs: Dict[str, Any],
        typeid: Optional[int],
        parentid: Optional[int],
        properties: Dict[str, Any],
    ) -> Dict[str, Any]:
        datavalue = attrs.get("value")
        datavalue_blob: Optional[bytes] = None
        if datavalue is not None:
            try:
                datavalue_blob = json.dumps(_json_safe(datavalue)).encode("utf-8")
            except Exception:
                datavalue_blob = str(datavalue).encode("utf-8")

        return {
            "id": internal_id,
            "nodeid": nodeid,
            "nodeclass": attrs.get("nodeclass"),
            "browsename": attrs.get("browsename"),
            "displayname": attrs.get("displayname"),
            "description": attrs.get("description"),
            "writemask": attrs.get("writemask"),
            "isabstract": attrs.get("isabstract"),
            "symmetric": attrs.get("symmetric"),
            "inversename": attrs.get("inversename"),
            "containsnoloops": attrs.get("containsnoloops"),
            "eventnotifier": attrs.get("eventnotifier"),
            "datavalue": datavalue_blob,
            "datatype": attrs.get("datatype"),
            "valuerank": attrs.get("valuerank"),
            "arraydimensions": attrs.get("arraydimensions"),
            "accesslevel": attrs.get("accesslevel"),
            "minimumsamplinginterval": attrs.get("minimumsamplinginterval"),
            "historizing": attrs.get("historizing"),
            "executable": attrs.get("executable"),
            "itemname": None,
            "aclid": None,
            "serialized": None,
            "valsrc": None,
            "eusrc": None,
            "defsrc": None,
            "typeid": typeid,
            "parentid": parentid,
            "properties": json.dumps(properties) if properties else None,
            "descendants": None,  # computed after full traversal
        }

    # ------------------------------------------------------------------
    # Flushing to DuckDB
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        """Insert buffered nodes and edges into DuckDB."""
        assert self._conn is not None

        for rec in self._nodes_buf:
            self._conn.execute(
                """
                INSERT INTO nodes VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                    $21,$22,$23,$24,$25,$26,$27,$28,$29,$30
                )
                """,
                [
                    rec["id"],
                    rec["nodeid"],
                    rec["nodeclass"],
                    rec["browsename"],
                    rec["displayname"],
                    rec["description"],
                    rec["writemask"],
                    rec["isabstract"],
                    rec["symmetric"],
                    rec["inversename"],
                    rec["containsnoloops"],
                    rec["eventnotifier"],
                    rec["datavalue"],
                    rec["datatype"],
                    rec["valuerank"],
                    rec["arraydimensions"],
                    rec["accesslevel"],
                    rec["minimumsamplinginterval"],
                    rec["historizing"],
                    rec["executable"],
                    rec["itemname"],
                    rec["aclid"],
                    rec["serialized"],
                    rec["valsrc"],
                    rec["eusrc"],
                    rec["defsrc"],
                    rec["typeid"],
                    rec["parentid"],
                    rec["properties"],
                    rec["descendants"],
                ],
            )

        for rec in self._edges_buf:
            self._conn.execute(
                "INSERT INTO edges VALUES ($1, $2, $3)",
                [rec["parentid"], rec["childid"], rec["referenceid"]],
            )

    # ------------------------------------------------------------------
    # Post-processing: descendants & closure
    # ------------------------------------------------------------------

    def _compute_descendants(self) -> None:
        """For every node compute the list of all transitively reachable
        child node ids and store them in the ``descendants`` column.
        """
        assert self._conn is not None

        # Build adjacency list from edges.
        rows = self._conn.execute("SELECT parentid, childid FROM edges").fetchall()
        children_map: Dict[int, List[int]] = {}
        for parent, child in rows:
            children_map.setdefault(parent, []).append(child)

        all_ids = [r[0] for r in self._conn.execute("SELECT id FROM nodes").fetchall()]

        for node_id in all_ids:
            desc = self._descendants_of(node_id, children_map)
            if desc:
                self._conn.execute(
                    "UPDATE nodes SET descendants = $1 WHERE id = $2",
                    [desc, node_id],
                )

    @staticmethod
    def _descendants_of(
        start: int,
        children_map: Dict[int, List[int]],
    ) -> List[int]:
        """BFS to collect all descendants of *start*."""
        visited: Set[int] = set()
        queue: deque[int] = deque()
        for child in children_map.get(start, []):
            if child not in visited:
                visited.add(child)
                queue.append(child)
        while queue:
            current = queue.popleft()
            for child in children_map.get(current, []):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
        return sorted(visited)

    def _compute_closure(self) -> None:
        """Add transitive-closure edges.

        For every pair ``(A, C)`` where a directed path ``A → … → C``
        exists but no direct edge ``A → C`` is present, a new row is
        inserted into ``edges``.  The ``referenceid`` of every such closure
        edge is set to the most frequently occurring ``referenceid`` in the
        existing edges table.
        """
        assert self._conn is not None

        # Find the most common reference type.
        row = self._conn.execute(
            """
            SELECT referenceid, COUNT(*) AS cnt
            FROM edges
            WHERE referenceid IS NOT NULL
            GROUP BY referenceid
            ORDER BY cnt DESC
            LIMIT 1
            """
        ).fetchone()

        if row is None:
            return
        closure_ref_id = row[0]

        # Build adjacency list.
        rows = self._conn.execute("SELECT parentid, childid FROM edges").fetchall()
        children_map: Dict[int, Set[int]] = {}
        existing_edges: Set[tuple] = set()
        for parent, child in rows:
            children_map.setdefault(parent, set()).add(child)
            existing_edges.add((parent, child))

        all_ids = [r[0] for r in self._conn.execute("SELECT id FROM nodes").fetchall()]

        new_edges: List[tuple] = []
        for start in all_ids:
            reachable = self._reachable(start, children_map)
            for target in reachable:
                if (start, target) not in existing_edges:
                    new_edges.append((start, target, closure_ref_id))
                    existing_edges.add((start, target))

        if new_edges:
            self._conn.executemany(
                "INSERT INTO edges VALUES ($1, $2, $3)", new_edges
            )

    @staticmethod
    def _reachable(
        start: int,
        children_map: Dict[int, Set[int]],
    ) -> Set[int]:
        """Return all nodes reachable from *start* (excluding *start*)."""
        visited: Set[int] = set()
        queue: deque[int] = deque()
        for child in children_map.get(start, set()):
            if child not in visited:
                visited.add(child)
                queue.append(child)
        while queue:
            current = queue.popleft()
            for child in children_map.get(current, set()):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
        return visited

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter


def _json_safe(value: Any) -> Any:
    """Best-effort conversion to a JSON-serialisable Python primitive."""
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    # asyncua NodeId, QualifiedName, LocalizedText, etc.
    if hasattr(value, "to_string"):
        return value.to_string()
    if hasattr(value, "Text"):
        return value.Text
    return str(value)
