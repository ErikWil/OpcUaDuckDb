//! Core types for the OPC UA client library.

use chrono::{DateTime, Utc};
use opcua::client::prelude::*;
use thiserror::Error;

/// Errors returned by the OPC UA client.
#[derive(Error, Debug)]
pub enum OpcUaError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Read error: {0}")]
    Read(String),
    #[error("Write error: {0}")]
    Write(String),
    #[error("Browse error: {0}")]
    Browse(String),
}

/// A dynamically typed OPC UA value.
#[derive(Debug, Clone, PartialEq)]
pub enum OpcValue {
    Boolean(bool),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Null,
}

impl OpcValue {
    /// Convert from an OPC UA Variant.
    pub fn from_variant(variant: &Variant) -> Self {
        match variant {
            Variant::Boolean(v) => OpcValue::Boolean(*v),
            Variant::SByte(v) => OpcValue::Int8(*v),
            Variant::Byte(v) => OpcValue::UInt8(*v),
            Variant::Int16(v) => OpcValue::Int16(*v),
            Variant::UInt16(v) => OpcValue::UInt16(*v),
            Variant::Int32(v) => OpcValue::Int32(*v),
            Variant::UInt32(v) => OpcValue::UInt32(*v),
            Variant::Int64(v) => OpcValue::Int64(*v),
            Variant::UInt64(v) => OpcValue::UInt64(*v),
            Variant::Float(v) => OpcValue::Float(*v),
            Variant::Double(v) => OpcValue::Double(*v),
            Variant::String(v) => OpcValue::String(v.as_ref().to_string()),
            Variant::Empty => OpcValue::Null,
            _ => OpcValue::String(format!("{variant:?}")),
        }
    }

    /// Convert to an OPC UA Variant.
    pub fn to_variant(&self) -> Variant {
        match self {
            OpcValue::Boolean(v) => Variant::Boolean(*v),
            OpcValue::Int8(v) => Variant::SByte(*v),
            OpcValue::UInt8(v) => Variant::Byte(*v),
            OpcValue::Int16(v) => Variant::Int16(*v),
            OpcValue::UInt16(v) => Variant::UInt16(*v),
            OpcValue::Int32(v) => Variant::Int32(*v),
            OpcValue::UInt32(v) => Variant::UInt32(*v),
            OpcValue::Int64(v) => Variant::Int64(*v),
            OpcValue::UInt64(v) => Variant::UInt64(*v),
            OpcValue::Float(v) => Variant::Float(*v),
            OpcValue::Double(v) => Variant::Double(*v),
            OpcValue::String(v) => Variant::String(UAString::from(v.as_str())),
            OpcValue::Null => Variant::Empty,
        }
    }

    /// Return the value as an f64 if it can be represented as one.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            OpcValue::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            OpcValue::Int8(v) => Some(*v as f64),
            OpcValue::UInt8(v) => Some(*v as f64),
            OpcValue::Int16(v) => Some(*v as f64),
            OpcValue::UInt16(v) => Some(*v as f64),
            OpcValue::Int32(v) => Some(*v as f64),
            OpcValue::UInt32(v) => Some(*v as f64),
            OpcValue::Int64(v) => Some(*v as f64),
            OpcValue::UInt64(v) => Some(*v as f64),
            OpcValue::Float(v) => Some(*v as f64),
            OpcValue::Double(v) => Some(*v),
            _ => None,
        }
    }
}

/// Value-Quality-Timestamp triple representing an OPC UA data point.
#[derive(Debug, Clone)]
pub struct Vqt {
    /// The data value.
    pub value: OpcValue,
    /// OPC UA StatusCode (0 = Good).
    pub quality: u32,
    /// Source timestamp.
    pub timestamp: DateTime<Utc>,
}

impl Vqt {
    /// Create a new VQT with Good quality and the current timestamp.
    pub fn new(value: OpcValue) -> Self {
        Self {
            value,
            quality: 0,
            timestamp: Utc::now(),
        }
    }

    /// Create a VQT with explicit quality and timestamp.
    pub fn with_details(value: OpcValue, quality: u32, timestamp: DateTime<Utc>) -> Self {
        Self {
            value,
            quality,
            timestamp,
        }
    }

    /// Convert from an OPC UA DataValue.
    pub fn from_data_value(dv: &DataValue) -> Result<Self, OpcUaError> {
        let value = dv
            .value
            .as_ref()
            .map(|v| OpcValue::from_variant(v))
            .unwrap_or(OpcValue::Null);

        let quality = dv
            .status
            .as_ref()
            .map(|s| s.bits())
            .unwrap_or(0);

        let timestamp = dv
            .source_timestamp
            .as_ref()
            .map(|t| t.as_chrono())
            .unwrap_or_else(Utc::now);

        Ok(Self {
            value,
            quality,
            timestamp,
        })
    }

    /// Convert to an OPC UA DataValue.
    pub fn to_data_value(&self) -> DataValue {
        DataValue {
            value: Some(self.value.to_variant()),
            status: Some(StatusCode::from_bits_truncate(self.quality)),
            source_timestamp: Some(opcua::types::DateTime::from(self.timestamp)),
            source_picoseconds: None,
            server_timestamp: None,
            server_picoseconds: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opc_value_round_trip() {
        let cases: Vec<OpcValue> = vec![
            OpcValue::Boolean(true),
            OpcValue::Int32(42),
            OpcValue::Double(3.14),
            OpcValue::String("hello".into()),
            OpcValue::Null,
        ];
        for val in cases {
            let variant = val.to_variant();
            let back = OpcValue::from_variant(&variant);
            assert_eq!(val, back);
        }
    }

    #[test]
    fn test_vqt_data_value_round_trip() {
        let ts = Utc::now();
        let vqt = Vqt::with_details(OpcValue::Double(42.5), 0, ts);
        let dv = vqt.to_data_value();
        let back = Vqt::from_data_value(&dv).unwrap();
        assert_eq!(back.quality, 0);
        if let OpcValue::Double(v) = back.value {
            assert!((v - 42.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_opc_value_as_f64() {
        assert_eq!(OpcValue::Int32(10).as_f64(), Some(10.0));
        assert_eq!(OpcValue::Boolean(true).as_f64(), Some(1.0));
        assert_eq!(OpcValue::String("x".into()).as_f64(), None);
    }

    #[test]
    fn test_vqt_new_defaults() {
        let vqt = Vqt::new(OpcValue::Int32(7));
        assert_eq!(vqt.quality, 0);
        if let OpcValue::Int32(v) = vqt.value {
            assert_eq!(v, 7);
        } else {
            panic!("Expected Int32");
        }
    }
}
