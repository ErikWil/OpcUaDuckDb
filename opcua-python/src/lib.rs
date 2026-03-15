//! Python bindings for the OPC UA client library.
//!
//! Exposes `Connection` with `read_values`, `write_value`, `read_history`,
//! `write_history`, and `browse` to Python via PyO3.

use chrono::{DateTime, TimeZone, Utc};
use opcua_client::{OpcUaConnectionConfig, OpcUaError, OpcValue, Vqt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

/// Convert an `OpcUaError` into a Python `RuntimeError`.
fn opcua_err(err: OpcUaError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Convert a Python object to an `OpcValue`.
fn py_to_opc_value(obj: &Bound<'_, PyAny>) -> PyResult<OpcValue> {
    if obj.is_none() {
        return Ok(OpcValue::Null);
    }
    if let Ok(v) = obj.extract::<bool>() {
        return Ok(OpcValue::Boolean(v));
    }
    if let Ok(v) = obj.extract::<i64>() {
        if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            return Ok(OpcValue::Int32(v as i32));
        }
        return Ok(OpcValue::Int64(v));
    }
    if let Ok(v) = obj.extract::<f64>() {
        return Ok(OpcValue::Double(v));
    }
    if let Ok(v) = obj.extract::<String>() {
        return Ok(OpcValue::String(v));
    }
    Err(PyRuntimeError::new_err(format!(
        "Unsupported value type: {}",
        obj.get_type().name()?
    )))
}

/// Convert an `OpcValue` to a Python object.
fn opc_value_to_py(py: Python<'_>, val: &OpcValue) -> PyObject {
    match val {
        OpcValue::Boolean(v) => (*v).into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        OpcValue::Int8(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::UInt8(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Int16(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::UInt16(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Int32(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::UInt32(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Int64(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::UInt64(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Float(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Double(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::String(v) => v.into_pyobject(py).unwrap().into_any().unbind(),
        OpcValue::Null => py.None(),
    }
}

/// A VQT (Value-Quality-Timestamp) data point exposed to Python.
#[pyclass(name = "Vqt")]
struct PyVqt {
    #[pyo3(get, set)]
    value: PyObject,
    #[pyo3(get, set)]
    quality: u32,
    #[pyo3(get, set)]
    timestamp: f64,
}

#[pymethods]
impl PyVqt {
    /// Create a new VQT.
    ///
    /// Args:
    ///     value: The data value (bool, int, float, str, or None).
    ///     quality: OPC UA StatusCode (0 = Good). Defaults to 0.
    ///     timestamp: UNIX timestamp (seconds since epoch). Defaults to now.
    #[new]
    #[pyo3(signature = (value, quality=0, timestamp=None))]
    fn new(value: PyObject, quality: u32, timestamp: Option<f64>) -> Self {
        let ts = timestamp.unwrap_or_else(|| Utc::now().timestamp() as f64);
        Self {
            value,
            quality,
            timestamp: ts,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let val_repr = self.value.bind(py).repr().map_or_else(
            |_| "?".to_string(),
            |r| r.to_string(),
        );
        format!(
            "Vqt(value={}, quality={}, timestamp={})",
            val_repr, self.quality, self.timestamp
        )
    }
}

impl PyVqt {
    fn to_vqt(&self, py: Python<'_>) -> PyResult<Vqt> {
        let value = py_to_opc_value(self.value.bind(py))?;
        let ts_secs = self.timestamp as i64;
        let ts_nanos = ((self.timestamp - ts_secs as f64) * 1e9) as u32;
        let timestamp = Utc
            .timestamp_opt(ts_secs, ts_nanos)
            .single()
            .unwrap_or_else(Utc::now);
        Ok(Vqt::with_details(value, self.quality, timestamp))
    }

    fn from_vqt(py: Python<'_>, vqt: &Vqt) -> Self {
        Self {
            value: opc_value_to_py(py, &vqt.value),
            quality: vqt.quality,
            timestamp: vqt.timestamp.timestamp() as f64
                + vqt.timestamp.timestamp_subsec_nanos() as f64 / 1e9,
        }
    }
}

/// OPC UA connection object for Python.
///
/// Contains connection parameters and, once connected, provides methods
/// for reading, writing, browsing and history operations.
///
/// Example:
///     conn = Connection("opc.tcp://localhost:4840")
///     values = conn.read_values(["ns=2;s=Temperature"])
#[pyclass(name = "Connection")]
struct PyOpcUaConnection {
    inner: opcua_client::OpcUaClient,
}

#[pymethods]
impl PyOpcUaConnection {
    /// Create a connection to an OPC UA server.
    ///
    /// Args:
    ///     uri: The server endpoint (e.g. "opc.tcp://localhost:4840").
    ///     security_policy: Optional security policy name (e.g. "Basic256Sha256").
    ///     security_mode: Optional message security mode ("None", "Sign", or "SignAndEncrypt").
    ///     certificate_path: Optional path to client certificate file.
    ///     private_key_path: Optional path to client private key file.
    ///     auth_token: Optional authentication token string.
    ///     username: Optional username for username/password authentication.
    ///     password: Optional password for username/password authentication.
    #[new]
    #[pyo3(signature = (uri, security_policy=None, security_mode=None, certificate_path=None, private_key_path=None, auth_token=None, username=None, password=None))]
    fn new(
        uri: &str,
        security_policy: Option<String>,
        security_mode: Option<String>,
        certificate_path: Option<String>,
        private_key_path: Option<String>,
        auth_token: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> PyResult<Self> {
        let config = OpcUaConnectionConfig {
            endpoint_url: uri.to_string(),
            security_policy,
            security_mode,
            certificate_path,
            private_key_path,
            auth_token,
            username,
            password,
        };
        let inner = opcua_client::OpcUaClient::new(&config).map_err(opcua_err)?;
        Ok(Self { inner })
    }

    /// Read current values for a list of NodeIds.
    ///
    /// Args:
    ///     node_ids: List of NodeId strings (e.g. ["ns=2;s=Temperature"]).
    ///
    /// Returns:
    ///     List of Vqt objects.
    fn read_values(&self, py: Python<'_>, node_ids: Vec<String>) -> PyResult<Vec<PyVqt>> {
        let id_refs: Vec<&str> = node_ids.iter().map(|s| s.as_str()).collect();
        let results = self.inner.read_values(&id_refs).map_err(opcua_err)?;
        Ok(results.iter().map(|v| PyVqt::from_vqt(py, v)).collect())
    }

    /// Write a value to a NodeId.
    ///
    /// Args:
    ///     node_id: The NodeId string.
    ///     vqt: A Vqt object with the value to write.
    fn write_value(&self, node_id: &str, vqt: &PyVqt, py: Python<'_>) -> PyResult<()> {
        let v = vqt.to_vqt(py)?;
        self.inner.write_value(node_id, &v).map_err(opcua_err)?;
        Ok(())
    }

    /// Read historical values.
    ///
    /// Args:
    ///     node_ids: List of NodeId strings.
    ///     from_ts: Start time as UNIX timestamp.
    ///     to_ts: End time as UNIX timestamp.
    ///     resample: Resampling interval in seconds (0.0 for raw).
    ///     aggregation: Aggregation type ("Average", "Minimum", "Maximum", etc.).
    ///
    /// Returns:
    ///     List of (node_id, [Vqt]) tuples.
    #[pyo3(signature = (node_ids, from_ts, to_ts, resample=0.0, aggregation="Average"))]
    fn read_history(
        &self,
        py: Python<'_>,
        node_ids: Vec<String>,
        from_ts: f64,
        to_ts: f64,
        resample: f64,
        aggregation: &str,
    ) -> PyResult<PyObject> {
        let id_refs: Vec<&str> = node_ids.iter().map(|s| s.as_str()).collect();
        let from = timestamp_to_datetime(from_ts);
        let to = timestamp_to_datetime(to_ts);

        let results = self
            .inner
            .read_history(&id_refs, from, to, resample, aggregation)
            .map_err(opcua_err)?;

        let list = PyList::empty(py);
        for (node_id, vqts) in &results {
            let py_vqts: Vec<PyVqt> = vqts.iter().map(|v| PyVqt::from_vqt(py, v)).collect();
            let tuple = (node_id, py_vqts);
            list.append(tuple)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Write historical values to a NodeId.
    ///
    /// Args:
    ///     node_id: The NodeId string.
    ///     values: List of Vqt objects.
    fn write_history(
        &self,
        py: Python<'_>,
        node_id: &str,
        values: Vec<Py<PyVqt>>,
    ) -> PyResult<()> {
        let vqts: Vec<Vqt> = values
            .iter()
            .map(|v| v.borrow(py).to_vqt(py))
            .collect::<PyResult<Vec<_>>>()?;
        self.inner.write_history(node_id, &vqts).map_err(opcua_err)?;
        Ok(())
    }

    /// Browse the address space from a NodeId.
    ///
    /// Args:
    ///     node_id: The starting NodeId string.
    ///     callback: A callable(ref_type: str, target_node: str) -> bool.
    ///               Return False if the target has already been explored.
    ///
    /// Returns:
    ///     True if browsing completed successfully.
    fn browse(
        &self,
        py: Python<'_>,
        node_id: &str,
        callback: PyObject,
    ) -> PyResult<bool> {
        let result = self
            .inner
            .browse(node_id, |ref_type, target| {
                let ret = callback
                    .call1(py, (ref_type, target))
                    .and_then(|r| r.extract::<bool>(py));
                match ret {
                    Ok(v) => v,
                    Err(_) => false,
                }
            })
            .map_err(opcua_err)?;
        Ok(result)
    }
}

fn timestamp_to_datetime(ts: f64) -> DateTime<Utc> {
    let secs = ts as i64;
    let nanos = ((ts - secs as f64) * 1e9) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Python module for OPC UA client operations.
#[pymodule]
fn opcua_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOpcUaConnection>()?;
    m.add_class::<PyVqt>()?;
    Ok(())
}
