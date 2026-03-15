# OpcUaDuckDb

A Rust workspace providing OPC UA client functionality, Python bindings, and a DuckDB extension.

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
| **Constructor** | `OpcUaClient::new(endpoint_url, security_policy) → Result<OpcUaClient>` |
| **read_values** | `read_values(&[&str]) → Result<Vec<Vqt>>` |
| **write_value** | `write_value(&str, &Vqt) → Result<()>` |
| **read_history** | `read_history(&[&str], from, to, resample, aggregation) → Result<Vec<(String, Vec<Vqt>)>>` |
| **write_history** | `write_history(&str, &[Vqt]) → Result<()>` |
| **browse** | `browse(&str, callback) → Result<bool>` — callback returns `false` if target already explored |

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
from opcua_python import OpcUaClient, Vqt

# Connect
client = OpcUaClient("opc.tcp://localhost:4840")

# Read values
values = client.read_values(["ns=2;s=Pump01.Speed", "ns=2;s=TempSensor01.Temperature"])
for v in values:
    print(f"value={v.value}, quality={v.quality}, ts={v.timestamp}")

# Write a value
client.write_value("ns=2;s=Pump01.Speed", Vqt(1500.0))

# Read history
import time
history = client.read_history(
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

client.browse("i=85", on_ref)
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
| `opcua_read(endpoint, node_ids)` | endpoint: VARCHAR, node_ids: VARCHAR (comma-separated) | Read current values |
| `opcua_read_history(endpoint, node_ids, from, to)` | + optional `resample`, `aggregation` | Read historical values |
| `opcua_write(endpoint, node_id, value)` | endpoint, node_id: VARCHAR, value: DOUBLE | Write a value |
| `opcua_write_history(endpoint, node_id, timestamp, value)` | endpoint, node_id, timestamp: VARCHAR, value: DOUBLE | Write a historical value |

### Usage in DuckDB

```sql
-- Read current values
SELECT * FROM opcua_read('opc.tcp://localhost:4840', 'ns=2;s=Pump01.Speed,ns=2;s=TempSensor01.Temperature');

-- Read history (raw)
SELECT * FROM opcua_read_history(
    'opc.tcp://localhost:4840',
    'ns=2;s=TempSensor01.Temperature',
    '2024-01-01T00:00:00Z',
    '2024-01-02T00:00:00Z'
);

-- Read history with aggregation
SELECT * FROM opcua_read_history(
    'opc.tcp://localhost:4840',
    'ns=2;s=TempSensor01.Temperature',
    '2024-01-01T00:00:00Z',
    '2024-01-02T00:00:00Z',
    resample := 60.0,
    aggregation := 'Average'
);

-- Write a value
SELECT * FROM opcua_write('opc.tcp://localhost:4840', 'ns=2;s=Pump01.Speed', 1500.0);

-- Write a historical value
SELECT * FROM opcua_write_history(
    'opc.tcp://localhost:4840',
    'ns=2;s=TempSensor01.Temperature',
    '2024-06-15T12:00:00Z',
    85.5
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

## License

See the repository for license information.
