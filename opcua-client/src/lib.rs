//! # opcua-client
//!
//! A Rust library for OPC UA client operations, exposing five methods and a constructor:
//!
//! - `read_values`: Read current values for a list of NodeIds
//! - `write_value`: Write a single value to a NodeId
//! - `read_history`: Read historical values with optional resampling/aggregation
//! - `write_history`: Write historical values to a NodeId
//! - `browse`: Browse the OPC UA address space with a visitor callback

mod types;

pub use types::{OpcUaConnectionConfig, OpcUaError, OpcValue, Vqt};

use chrono::{DateTime, Utc};
use opcua::client::prelude::*;
use opcua::sync::RwLock;
use opcua::types::DecodingOptions;
use std::str::FromStr;
use std::sync::Arc;

/// Parse a NodeId string such as `"ns=2;s=MyVariable"` or `"i=85"`.
fn parse_node_id(s: &str) -> Result<NodeId, OpcUaError> {
    NodeId::from_str(s)
        .map_err(|_| OpcUaError::Read(format!("Invalid NodeId string: {s}")))
}

/// OPC UA client providing read, write, history, and browse operations.
pub struct OpcUaClient {
    session: Arc<RwLock<Session>>,
}

impl OpcUaClient {
    /// Create a new client connected to the given OPC UA endpoint.
    ///
    /// # Arguments
    /// * `config` - Connection configuration containing the endpoint URL and optional
    ///   security/authentication parameters.
    pub fn new(config: &OpcUaConnectionConfig) -> Result<Self, OpcUaError> {
        let mut builder = ClientBuilder::new()
            .application_name("opcua-client-rs")
            .application_uri("urn:opcua-client-rs")
            .session_retry_limit(3);

        if let (Some(cert), Some(key)) = (&config.certificate_path, &config.private_key_path) {
            builder = builder
                .certificate_path(cert)
                .private_key_path(key);
        } else {
            builder = builder.create_sample_keypair(true);
        }
        builder = builder.trust_server_certs(true);

        let mut client = builder
            .client()
            .ok_or_else(|| OpcUaError::Connection("Failed to create OPC UA client".into()))?;

        let policy = config
            .security_policy
            .as_deref()
            .unwrap_or("None");
        let mode = match config.security_mode.as_deref() {
            Some("Sign") => MessageSecurityMode::Sign,
            Some("SignAndEncrypt") => MessageSecurityMode::SignAndEncrypt,
            _ => MessageSecurityMode::None,
        };
        let endpoint: EndpointDescription = (
            config.endpoint_url.as_str(),
            policy,
            mode,
        )
            .into();

        let identity = if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            IdentityToken::UserName(user.clone(), pass.clone())
        } else {
            IdentityToken::Anonymous
        };

        let session = client
            .connect_to_endpoint(endpoint, identity)
            .map_err(|e| OpcUaError::Connection(format!("Failed to connect: {e}")))?;

        Ok(Self { session })
    }

    /// Read current values for the given NodeId strings.
    ///
    /// Returns a `Vqt` (Value-Quality-Timestamp) for each requested NodeId.
    pub fn read_values(&self, node_ids: &[&str]) -> Result<Vec<Vqt>, OpcUaError> {
        let session = self.session.read();
        let nodes_to_read: Vec<ReadValueId> = node_ids
            .iter()
            .map(|id| {
                let node = parse_node_id(id).unwrap_or_else(|_| NodeId::null());
                ReadValueId::from(node)
            })
            .collect();

        let results = session
            .read(&nodes_to_read, TimestampsToReturn::Both, 0.0)
            .map_err(|e| OpcUaError::Read(format!("Read failed: {e}")))?;

        results
            .iter()
            .map(|dv| Vqt::from_data_value(dv))
            .collect()
    }

    /// Write a single value to the specified NodeId.
    pub fn write_value(&self, node_id: &str, vqt: &Vqt) -> Result<(), OpcUaError> {
        let session = self.session.read();
        let node = parse_node_id(node_id)?;
        let value = WriteValue {
            node_id: node,
            attribute_id: AttributeId::Value as u32,
            index_range: UAString::null(),
            value: vqt.to_data_value(),
        };

        let results = session
            .write(&[value])
            .map_err(|e| OpcUaError::Write(format!("Write failed: {e}")))?;

        if let Some(status) = results.first() {
            if status.is_good() {
                Ok(())
            } else {
                Err(OpcUaError::Write(format!(
                    "Write returned bad status: {status}"
                )))
            }
        } else {
            Err(OpcUaError::Write("No write result returned".into()))
        }
    }

    /// Read historical values for the given NodeIds within a time range.
    ///
    /// # Arguments
    /// * `node_ids` - NodeId strings to read history for
    /// * `from` - Start of the time range
    /// * `to` - End of the time range
    /// * `resample` - Resampling interval in seconds (0.0 for raw data)
    /// * `aggregation` - Aggregation type (e.g. `"Average"`, `"Interpolative"`, `"Minimum"`, `"Maximum"`)
    pub fn read_history(
        &self,
        node_ids: &[&str],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        resample: f64,
        aggregation: &str,
    ) -> Result<Vec<(String, Vec<Vqt>)>, OpcUaError> {
        let session = self.session.read();
        let start_time = opcua::types::DateTime::from(from);
        let end_time = opcua::types::DateTime::from(to);

        let mut all_results = Vec::new();

        if resample > 0.0 {
            // Aggregated history read
            let aggregate_type = match aggregation.to_lowercase().as_str() {
                "average" => NodeId::new(0, 2341u32),        // AggregateFunction_Average
                "interpolative" => NodeId::new(0, 2344u32),  // AggregateFunction_Interpolative
                "minimum" | "min" => NodeId::new(0, 2346u32), // AggregateFunction_Minimum
                "maximum" | "max" => NodeId::new(0, 2347u32), // AggregateFunction_Maximum
                "count" => NodeId::new(0, 2352u32),          // AggregateFunction_Count
                "total" | "sum" => NodeId::new(0, 2348u32),  // AggregateFunction_Total
                _ => {
                    return Err(OpcUaError::Read(format!(
                        "Unknown aggregation type: {aggregation}"
                    )));
                }
            };

            for node_id_str in node_ids {
                let node_id = parse_node_id(node_id_str)?;
                let details = ReadProcessedDetails {
                    start_time: start_time.clone(),
                    end_time: end_time.clone(),
                    processing_interval: resample * 1000.0, // convert seconds to milliseconds
                    aggregate_type: Some(vec![aggregate_type.clone()]),
                    aggregate_configuration: AggregateConfiguration {
                        use_server_capabilities_defaults: true,
                        treat_uncertain_as_bad: false,
                        percent_data_bad: 0,
                        percent_data_good: 100,
                        use_sloped_extrapolation: false,
                    },
                };

                let action = HistoryReadAction::ReadProcessedDetails(details);
                let nodes = vec![HistoryReadValueId {
                    node_id,
                    index_range: UAString::null(),
                    data_encoding: QualifiedName::null(),
                    continuation_point: ByteString::null(),
                }];

                let result = session
                    .history_read(
                        action,
                        TimestampsToReturn::Both,
                        false,
                        &nodes,
                    )
                    .map_err(|e| {
                        OpcUaError::Read(format!("History read failed: {e}"))
                    })?;

                let vqts = Self::extract_history_data(&result)?;
                all_results.push((node_id_str.to_string(), vqts));
            }
        } else {
            // Raw history read
            for node_id_str in node_ids {
                let node_id = parse_node_id(node_id_str)?;
                let details = ReadRawModifiedDetails {
                    is_read_modified: false,
                    start_time: start_time.clone(),
                    end_time: end_time.clone(),
                    num_values_per_node: 0,
                    return_bounds: false,
                };

                let action = HistoryReadAction::ReadRawModifiedDetails(details);
                let nodes = vec![HistoryReadValueId {
                    node_id,
                    index_range: UAString::null(),
                    data_encoding: QualifiedName::null(),
                    continuation_point: ByteString::null(),
                }];

                let result = session
                    .history_read(
                        action,
                        TimestampsToReturn::Both,
                        false,
                        &nodes,
                    )
                    .map_err(|e| {
                        OpcUaError::Read(format!("History read failed: {e}"))
                    })?;

                let vqts = Self::extract_history_data(&result)?;
                all_results.push((node_id_str.to_string(), vqts));
            }
        }

        Ok(all_results)
    }

    /// Write historical values to the specified NodeId.
    pub fn write_history(
        &self,
        node_id: &str,
        values: &[Vqt],
    ) -> Result<(), OpcUaError> {
        let session = self.session.read();
        let node = parse_node_id(node_id)?;

        let data_values: Vec<DataValue> = values.iter().map(|v| v.to_data_value()).collect();
        let update_details = UpdateDataDetails {
            node_id: node,
            perform_insert_replace: PerformUpdateType::Replace,
            update_values: Some(data_values),
        };

        let action = HistoryUpdateAction::UpdateDataDetails(update_details);
        let results = session
            .history_update(&[action])
            .map_err(|e| OpcUaError::Write(format!("History write failed: {e}")))?;

        if let Some(result) = results.first() {
            if result.status_code.is_good() {
                Ok(())
            } else {
                Err(OpcUaError::Write(format!(
                    "History write returned bad status: {}",
                    result.status_code
                )))
            }
        } else {
            Err(OpcUaError::Write("No history write result returned".into()))
        }
    }

    /// Browse the address space starting from the given NodeId.
    ///
    /// The callback receives `(reference_type, target_node_id)` and should return `false`
    /// if the target node has already been explored (to prevent cycles).
    ///
    /// Returns `true` if browsing completed successfully.
    pub fn browse<F>(
        &self,
        node_id: &str,
        mut callback: F,
    ) -> Result<bool, OpcUaError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        let session = self.session.read();
        let node = parse_node_id(node_id)?;

        let browse_description = BrowseDescription {
            node_id: node,
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
            include_subtypes: true,
            node_class_mask: 0, // all node classes
            result_mask: BrowseDescriptionResultMask::all().bits(),
        };

        let results = session
            .browse(&[browse_description])
            .map_err(|e| OpcUaError::Browse(format!("Browse failed: {e}")))?;

        if let Some(browse_results) = results {
            if let Some(result) = browse_results.first() {
                if let Some(refs) = &result.references {
                    for reference in refs {
                        let ref_type = reference.reference_type_id.to_string();
                        let target = reference.node_id.node_id.to_string();
                        if !callback(&ref_type, &target) {
                            return Ok(true);
                        }
                    }
                }
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Extract VQT values from a history read result.
    fn extract_history_data(
        results: &[HistoryReadResult],
    ) -> Result<Vec<Vqt>, OpcUaError> {
        let mut vqts = Vec::new();
        let decoding_options = DecodingOptions::default();
        for result in results {
            if !result.status_code.is_good() {
                return Err(OpcUaError::Read(format!(
                    "History read result bad status: {}",
                    result.status_code
                )));
            }
            if !result.history_data.is_null() {
                let data: HistoryData = result
                    .history_data
                    .decode_inner::<HistoryData>(&decoding_options)
                    .map_err(|e| {
                        OpcUaError::Read(format!("Failed to decode history data: {e}"))
                    })?;
                if let Some(data_values) = data.data_values {
                    for dv in &data_values {
                        vqts.push(Vqt::from_data_value(dv)?);
                    }
                }
            }
        }
        Ok(vqts)
    }
}
