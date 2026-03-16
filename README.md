# OpcUaDuckDb

A Rust workspace providing OPC UA client functionality, Python bindings, a DuckDB extension, and a Python namespace crawler.

## Project Structure

```
OpcUaDuckDb/
├── Cargo.toml               # Workspace root
├── opcua-client/             # Core OPC UA client library
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs            # Client implementation
│       └── types.rs          # VQT, OpcValue, error types
├── opcua-python/             # Python bindings (PyO3)
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/
│       └── lib.rs            # Python module
├── opcua-duckdb/             # DuckDB loadable extension
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs            # Table function definitions
├── opcua-crawler/            # OPC UA namespace crawler (Python)
│   ├── pyproject.toml
│   ├── src/
│   │   └── opcua_crawler/
│   │       ├── __init__.py
│   │       ├── schema.py     # DuckDB schema definitions
│   │       ├── crawler.py    # Core crawling logic
│   │       └── __main__.py   # CLI entry point
│   └── tests/
│       ├── test_schema.py
│       └── test_crawler.py
└── examples/
    ├── python/
    │   └── example.py        # Python usage example
    └── nodesets/
        ├── type_library.xml  # OPC UA type definitions (Nodeset2)
        └── instance.xml      # OPC UA instance data (Nodeset2)
```

## The OPC UA Crate (`opcua-client`)

A Rust library that exposes five methods and a constructor:

| Method | Signature |
|--------|-----------|
| **Constructor** | `OpcUaClient::new(&OpcUaConnectionConfig) → Result<OpcUaClient>` |
| **read_values** | `read_values(&[&str]) → Result<Vec<Vqt>>` |
| **write_value** | `write_value(&str, &Vqt) → Result<()>` |
| **read_history** | `read_history(&[&str], from, to, resample, aggregation) → Result<Vec<(String, Vec<Vqt>)>>` |
| **write_history** | `write_history(&str, &[Vqt]) → Result<()>` |
| **browse** | `browse(&str, callback) → Result<bool>` — callback returns `false` if target already explored |

### OpcUaConnectionConfig

```rust
pub struct OpcUaConnectionConfig {
    pub endpoint_url: String,              // e.g. "opc.tcp://localhost:4840"
    pub security_policy: Option<String>,   // e.g. "Basic256Sha256"
    pub security_mode: Option<String>,     // "None", "Sign", or "SignAndEncrypt"
    pub certificate_path: Option<String>,  // Path to client certificate
    pub private_key_path: Option<String>,  // Path to client private key
    pub auth_token: Option<String>,        // Authentication token
    pub username: Option<String>,          // Username for user/pass auth
    pub password: Option<String>,          // Password for user/pass auth
}
```

### VQT (Value-Quality-Timestamp)

```rust
pub struct Vqt {
    pub value: OpcValue,    // Boolean, Int32, Double, String, etc.
    pub quality: u32,       // OPC UA StatusCode (0 = Good)
    pub timestamp: DateTime<Utc>,
}
```

### Building

```bash
cargo build -p opcua-client
cargo test -p opcua-client
```

## The Python Module (`opcua-python`)

Wraps the OPC UA crate in a Python module using [PyO3](https://pyo3.rs/) and [maturin](https://www.maturin.rs/).

### Installation

```bash
cd opcua-python
pip install maturin
maturin develop
```

### Usage

```python
from opcua_python import Connection, Vqt

# Connect with minimal parameters
conn = Connection("opc.tcp://localhost:4840")

# Connect with security and authentication
conn = Connection(
    "opc.tcp://server:4840",
    security_policy="Basic256Sha256",
    security_mode="SignAndEncrypt",
    certificate_path="/path/to/cert.pem",
    private_key_path="/path/to/key.pem",
    username="admin",
    password="secret",
)

# Read values
values = conn.read_values(["ns=2;s=Pump01.Speed", "ns=2;s=TempSensor01.Temperature"])
for v in values:
    print(f"value={v.value}, quality={v.quality}, ts={v.timestamp}")

# Write a value
conn.write_value("ns=2;s=Pump01.Speed", Vqt(1500.0))

# Read history
import time
history = conn.read_history(
    ["ns=2;s=TempSensor01.Temperature"],
    from_ts=time.time() - 3600,
    to_ts=time.time(),
    resample=10.0,
    aggregation="Average",
)

# Browse
visited = set()
def on_ref(ref_type, target):
    if target in visited:
        return False
    visited.add(target)
    print(f"  [{ref_type}] -> {target}")
    return True

conn.browse("i=85", on_ref)
```

See [`examples/python/example.py`](examples/python/example.py) for a complete example.

### Example Nodeset2 Files

The [`examples/nodesets/`](examples/nodesets/) directory contains OPC UA Nodeset2 XML files for testing:

- **`type_library.xml`** – Defines object types for process equipment:
  `PumpType`, `FlowMeterType`, `TemperatureSensorType`, `ValveType`

- **`instance.xml`** – Creates instances of these types:
  `Pump01`, `Pump02`, `FlowMeter01`, `TempSensor01`, `TempSensor02`, `ControlValve01`

## The DuckDB Extension (`opcua-duckdb`)

A DuckDB loadable extension that exposes the OPC UA read/write operations as table functions.

### Table Functions

| Function | Parameters | Description |
|----------|-----------|-------------|
| `opcua_read(connection, node_ids)` | connection: VARCHAR (JSON), node_ids: VARCHAR (comma-separated) | Read current values |
| `opcua_read_history(connection, node_ids, from, to)` | + optional `resample`, `aggregation` | Read historical values |
| `opcua_write(connection, node_id, value)` | connection: VARCHAR (JSON), node_id: VARCHAR, value: DOUBLE | Write a value |
| `opcua_write_history(connection, node_id, timestamp, value)` | connection: VARCHAR (JSON), node_id, timestamp: VARCHAR, value: DOUBLE | Write a historical value |

### Connection JSON

The first parameter to all table functions is a JSON string describing the connection:

```json
{
  "endpoint_url": "opc.tcp://localhost:4840",
  "security_policy": "Basic256Sha256",
  "security_mode": "SignAndEncrypt",
  "certificate_path": "/path/to/cert.pem",
  "private_key_path": "/path/to/key.pem",
  "username": "admin",
  "password": "secret"
}
```

Only `endpoint_url` is required; all other fields are optional.

### Usage in DuckDB

```sql
-- Read current values
SELECT * FROM opcua_read(
    '{"endpoint_url":"opc.tcp://localhost:4840"}',
    'ns=2;s=Pump01.Speed,ns=2;s=TempSensor01.Temperature'
);

-- Read history (raw)
SELECT * FROM opcua_read_history(
    '{"endpoint_url":"opc.tcp://localhost:4840"}',
    'ns=2;s=TempSensor01.Temperature',
    '2024-01-01T00:00:00Z',
    '2024-01-02T00:00:00Z'
);

-- Read history with aggregation
SELECT * FROM opcua_read_history(
    '{"endpoint_url":"opc.tcp://localhost:4840"}',
    'ns=2;s=TempSensor01.Temperature',
    '2024-01-01T00:00:00Z',
    '2024-01-02T00:00:00Z',
    resample := 60.0,
    aggregation := 'Average'
);

-- Write a value
SELECT * FROM opcua_write(
    '{"endpoint_url":"opc.tcp://localhost:4840"}',
    'ns=2;s=Pump01.Speed',
    1500.0
);

-- Write a historical value
SELECT * FROM opcua_write_history(
    '{"endpoint_url":"opc.tcp://localhost:4840"}',
    'ns=2;s=TempSensor01.Temperature',
    '2024-06-15T12:00:00Z',
    85.5
);

-- Use a connection with authentication
SELECT * FROM opcua_read(
    '{"endpoint_url":"opc.tcp://server:4840","username":"admin","password":"secret"}',
    'ns=2;s=Pump01.Speed'
);
```

### Building the Extension

```bash
cargo build -p opcua-duckdb --release
```

The resulting `.so` / `.dll` / `.dylib` can be loaded in DuckDB with:

```sql
LOAD 'path/to/libopcua_duckdb.so';
```

## Dependencies

- [opcua](https://crates.io/crates/opcua) – OPC UA protocol implementation
- [PyO3](https://pyo3.rs/) – Rust ↔ Python bindings
- [DuckDB](https://duckdb.org/) – In-process analytical database
- [chrono](https://crates.io/crates/chrono) – Date/time handling
- [asyncua](https://pypi.org/project/asyncua/) – Python OPC UA client (used by the crawler)

## OPC UA Namespace Crawler (`opcua-crawler`)

A Python project that crawls an OPC UA server's address space and stores it
as a graph in DuckDB.  Nodes become vertices; references become edges.

### Installation

```bash
cd opcua-crawler
pip install -e ".[dev]"
```

### Usage

```bash
# Crawl a server and write the graph to a DuckDB file
opcua-crawler opc.tcp://localhost:4840 namespace.duckdb

# Skip nodes of specific types
opcua-crawler opc.tcp://localhost:4840 namespace.duckdb \
    --skip-type "i=58" --skip-type "i=62"

# Verbose output
opcua-crawler opc.tcp://localhost:4840 namespace.duckdb -v
```

Or use the crawler programmatically:

```python
import asyncio
from opcua_crawler import OpcUaCrawler

async def main():
    crawler = OpcUaCrawler(
        endpoint_url="opc.tcp://localhost:4840",
        db_path="namespace.duckdb",
        skip_types={"i=58"},  # skip BaseObjectType sub-trees
    )
    conn = await crawler.crawl()

    # Query the resulting graph
    print(conn.execute("SELECT COUNT(*) FROM nodes").fetchone())
    print(conn.execute("SELECT COUNT(*) FROM edges").fetchone())

asyncio.run(main())
```

### Schema

**nodes** – one row per OPC UA node:

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Internal graph id |
| nodeid | VARCHAR | OPC UA NodeId string |
| nodeclass | INTEGER | OPC UA NodeClass |
| browsename | VARCHAR | Browse name |
| displayname | VARCHAR | Display name |
| description | VARCHAR | Node description |
| typeid | INTEGER | FK → nodes.id of the type definition |
| parentid | INTEGER | FK → nodes.id of the parent |
| properties | JSON | Properties as `{"name": {"nodeid": "…", "value": …}}` |
| descendants | INTEGER[] | All transitively reachable descendant ids |
| *(+ more)* | | writemask, isabstract, symmetric, datatype, … |

**edges** – one row per reference:

| Column | Type | Description |
|--------|------|-------------|
| parentid | INTEGER | Source node id |
| childid | INTEGER | Target node id |
| referenceid | INTEGER | Reference-type node id |

### Crawling behaviour

* Browsing starts at **i=86** (Types) then **i=85** (Objects).
* **HasProperty** sub-nodes are stored in the parent's `properties` JSON
  column instead of as separate node rows.
* **HasTypeDefinition** (and sub-types) references are stored in the node's
  `typeid` column instead of in the `edges` table.
* Nodes whose type definition is in the `--skip-type` set are skipped
  together with their sub-trees.
* After traversal, the `descendants` column is filled with a BFS over
  the edge graph.
* A **transitive closure** is computed: for every indirect path A → … → C
  a direct edge is added.  The `referenceid` of closure edges is set to the
  most common reference type in the existing edges.

### Running the tests

```bash
cd opcua-crawler
pytest tests/ -v
```

## License

See the repository for license information.
