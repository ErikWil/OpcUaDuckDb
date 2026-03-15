//! DuckDB extension for OPC UA read/write operations.
//!
//! Exposes the following table functions:
//! - `opcua_read(connection, node_id, ...)` – read current values
//! - `opcua_read_history(connection, node_id, from_ts, to_ts, ...)` – read historical values
//! - `opcua_write(connection, node_id, value)` – write a value (returns status)
//! - `opcua_write_history(connection, node_id, timestamp, value)` – write historical values (returns status)
//!
//! The `connection` parameter is a JSON string describing the OPC UA connection.
//! Minimal: `'{"endpoint_url":"opc.tcp://localhost:4840"}'`
//! Full: `'{"endpoint_url":"opc.tcp://server:4840","security_policy":"Basic256Sha256","username":"admin","password":"secret"}'`

use duckdb::{
    Connection, Result,
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use opcua_client::{OpcUaClient, OpcUaConnectionConfig, OpcValue, Vqt};
use std::error::Error;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Parse a JSON connection string into an `OpcUaConnectionConfig`.
fn parse_connection(json: &str) -> Result<OpcUaConnectionConfig, Box<dyn Error>> {
    let config: OpcUaConnectionConfig = serde_json::from_str(json)
        .map_err(|e| format!("Invalid connection JSON: {e}"))?;
    Ok(config)
}

// ---------------------------------------------------------------------------
// opcua_read – read current values
// ---------------------------------------------------------------------------

#[repr(C)]
struct ReadBindData {
    connection: OpcUaConnectionConfig,
    node_ids: Vec<String>,
}

#[repr(C)]
struct ReadInitData {
    done: AtomicBool,
}

struct OpcUaReadVTab;

impl VTab for OpcUaReadVTab {
    type InitData = ReadInitData;
    type BindData = ReadBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("node_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("quality", LogicalTypeHandle::from(LogicalTypeId::UInteger));
        bind.add_result_column("timestamp", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let connection_json = bind.get_parameter(0).to_string();
        let connection = parse_connection(&connection_json)?;
        let node_ids_str = bind.get_parameter(1).to_string();
        let node_ids: Vec<String> = node_ids_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(ReadBindData { connection, node_ids })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ReadInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let client = OpcUaClient::new(&bind_data.connection)?;
        let id_refs: Vec<&str> = bind_data.node_ids.iter().map(|s| s.as_str()).collect();
        let values = client.read_values(&id_refs)?;

        let count = values.len().min(bind_data.node_ids.len());
        for i in 0..count {
            let node_id_vec = output.flat_vector(0);
            let node_id_c = CString::new(bind_data.node_ids[i].clone())?;
            node_id_vec.insert(i, node_id_c);

            let mut value_vec = output.flat_vector(1);
            if let Some(f) = values[i].value.as_f64() {
                value_vec.as_mut_slice::<f64>()[i] = f;
            } else {
                value_vec.set_null(i);
            }

            let mut quality_vec = output.flat_vector(2);
            quality_vec.as_mut_slice::<u32>()[i] = values[i].quality;

            let ts_vec = output.flat_vector(3);
            let ts_str = CString::new(values[i].timestamp.to_rfc3339())?;
            ts_vec.insert(i, ts_str);
        }

        output.set_len(count);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // connection (JSON)
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // comma-separated node_ids
        ])
    }
}

// ---------------------------------------------------------------------------
// opcua_read_history – read historical values
// ---------------------------------------------------------------------------

#[repr(C)]
struct ReadHistoryBindData {
    connection: OpcUaConnectionConfig,
    node_ids: Vec<String>,
    from_ts: String,
    to_ts: String,
    resample: f64,
    aggregation: String,
}

#[repr(C)]
struct ReadHistoryInitData {
    rows: Mutex<Vec<(String, f64, u32, String)>>,
    offset: AtomicUsize,
}

struct OpcUaReadHistoryVTab;

impl VTab for OpcUaReadHistoryVTab {
    type InitData = ReadHistoryInitData;
    type BindData = ReadHistoryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("node_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("quality", LogicalTypeHandle::from(LogicalTypeId::UInteger));
        bind.add_result_column("timestamp", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let connection_json = bind.get_parameter(0).to_string();
        let connection = parse_connection(&connection_json)?;
        let node_ids_str = bind.get_parameter(1).to_string();
        let from_ts = bind.get_parameter(2).to_string();
        let to_ts = bind.get_parameter(3).to_string();

        let node_ids: Vec<String> = node_ids_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let resample = bind
            .get_named_parameter("resample")
            .map(|v| v.to_string().parse::<f64>().unwrap_or(0.0))
            .unwrap_or(0.0);

        let aggregation = bind
            .get_named_parameter("aggregation")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "Average".to_string());

        Ok(ReadHistoryBindData {
            connection,
            node_ids,
            from_ts,
            to_ts,
            resample,
            aggregation,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ReadHistoryInitData {
            rows: Mutex::new(Vec::new()),
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        // Populate rows on first call
        {
            let mut rows = init_data.rows.lock().unwrap();
            if rows.is_empty() && init_data.offset.load(Ordering::Relaxed) == 0 {
                let client = OpcUaClient::new(&bind_data.connection)?;
                let id_refs: Vec<&str> = bind_data.node_ids.iter().map(|s| s.as_str()).collect();

                let from = chrono::DateTime::parse_from_rfc3339(&bind_data.from_ts)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| format!("Invalid from timestamp: {e}"))?;
                let to = chrono::DateTime::parse_from_rfc3339(&bind_data.to_ts)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| format!("Invalid to timestamp: {e}"))?;

                let results = client.read_history(
                    &id_refs,
                    from,
                    to,
                    bind_data.resample,
                    &bind_data.aggregation,
                )?;

                for (node_id, vqts) in results {
                    for vqt in vqts {
                        rows.push((
                            node_id.clone(),
                            vqt.value.as_f64().unwrap_or(f64::NAN),
                            vqt.quality,
                            vqt.timestamp.to_rfc3339(),
                        ));
                    }
                }
            }
        }

        let rows = init_data.rows.lock().unwrap();
        let offset = init_data.offset.load(Ordering::Relaxed);

        if offset >= rows.len() {
            output.set_len(0);
            return Ok(());
        }

        let batch_size = (rows.len() - offset).min(2048);
        for i in 0..batch_size {
            let row = &rows[offset + i];

            let node_id_vec = output.flat_vector(0);
            let node_id_c = CString::new(row.0.clone())?;
            node_id_vec.insert(i, node_id_c);

            let mut value_vec = output.flat_vector(1);
            value_vec.as_mut_slice::<f64>()[i] = row.1;

            let mut quality_vec = output.flat_vector(2);
            quality_vec.as_mut_slice::<u32>()[i] = row.2;

            let ts_vec = output.flat_vector(3);
            let ts_c = CString::new(row.3.clone())?;
            ts_vec.insert(i, ts_c);
        }

        init_data
            .offset
            .store(offset + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // connection (JSON)
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // comma-separated node_ids
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // from (ISO 8601)
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // to (ISO 8601)
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("resample".to_string(), LogicalTypeHandle::from(LogicalTypeId::Double)),
            ("aggregation".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}

// ---------------------------------------------------------------------------
// opcua_write – write a single value (returns status)
// ---------------------------------------------------------------------------

#[repr(C)]
struct WriteBindData {
    connection: OpcUaConnectionConfig,
    node_id: String,
    value: f64,
}

#[repr(C)]
struct WriteInitData {
    done: AtomicBool,
}

struct OpcUaWriteVTab;

impl VTab for OpcUaWriteVTab {
    type InitData = WriteInitData;
    type BindData = WriteBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("node_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let connection_json = bind.get_parameter(0).to_string();
        let connection = parse_connection(&connection_json)?;
        let node_id = bind.get_parameter(1).to_string();
        let value: f64 = bind.get_parameter(2).to_string().parse()?;

        Ok(WriteBindData {
            connection,
            node_id,
            value,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(WriteInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let client = OpcUaClient::new(&bind_data.connection)?;
        let vqt = Vqt::new(OpcValue::Double(bind_data.value));
        let status = match client.write_value(&bind_data.node_id, &vqt) {
            Ok(()) => "OK".to_string(),
            Err(e) => format!("Error: {e}"),
        };

        let node_id_vec = output.flat_vector(0);
        let node_id_c = CString::new(bind_data.node_id.clone())?;
        node_id_vec.insert(0, node_id_c);

        let status_vec = output.flat_vector(1);
        let status_c = CString::new(status)?;
        status_vec.insert(0, status_c);

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // connection (JSON)
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // node_id
            LogicalTypeHandle::from(LogicalTypeId::Double),  // value
        ])
    }
}

// ---------------------------------------------------------------------------
// opcua_write_history – write historical value (returns status)
// ---------------------------------------------------------------------------

#[repr(C)]
struct WriteHistoryBindData {
    connection: OpcUaConnectionConfig,
    node_id: String,
    timestamp: String,
    value: f64,
}

#[repr(C)]
struct WriteHistoryInitData {
    done: AtomicBool,
}

struct OpcUaWriteHistoryVTab;

impl VTab for OpcUaWriteHistoryVTab {
    type InitData = WriteHistoryInitData;
    type BindData = WriteHistoryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("node_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let connection_json = bind.get_parameter(0).to_string();
        let connection = parse_connection(&connection_json)?;
        let node_id = bind.get_parameter(1).to_string();
        let timestamp = bind.get_parameter(2).to_string();
        let value: f64 = bind.get_parameter(3).to_string().parse()?;

        Ok(WriteHistoryBindData {
            connection,
            node_id,
            timestamp,
            value,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(WriteHistoryInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let client = OpcUaClient::new(&bind_data.connection)?;

        let ts = chrono::DateTime::parse_from_rfc3339(&bind_data.timestamp)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| format!("Invalid timestamp: {e}"))?;

        let vqt = Vqt::with_details(OpcValue::Double(bind_data.value), 0, ts);
        let status = match client.write_history(&bind_data.node_id, &[vqt]) {
            Ok(()) => "OK".to_string(),
            Err(e) => format!("Error: {e}"),
        };

        let node_id_vec = output.flat_vector(0);
        let node_id_c = CString::new(bind_data.node_id.clone())?;
        node_id_vec.insert(0, node_id_c);

        let status_vec = output.flat_vector(1);
        let status_c = CString::new(status)?;
        status_vec.insert(0, status_c);

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // connection (JSON)
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // node_id
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // timestamp (ISO 8601)
            LogicalTypeHandle::from(LogicalTypeId::Double),  // value
        ])
    }
}

// ---------------------------------------------------------------------------
// Extension entrypoint
// ---------------------------------------------------------------------------

#[duckdb_entrypoint_c_api(ext_name = "opcua_duckdb", min_duckdb_version = "v0.0.1")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<OpcUaReadVTab>("opcua_read")?;
    con.register_table_function::<OpcUaReadHistoryVTab>("opcua_read_history")?;
    con.register_table_function::<OpcUaWriteVTab>("opcua_write")?;
    con.register_table_function::<OpcUaWriteHistoryVTab>("opcua_write_history")?;
    Ok(())
}
