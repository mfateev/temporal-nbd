use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: &str = "v1";
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Operation {
    #[serde(rename = "AddDevice")]
    AddDevice,
    #[serde(rename = "RemoveDevice")]
    RemoveDevice,
    #[serde(rename = "ListDevices")]
    ListDevices,
    #[serde(rename = "Health")]
    Health,
}

impl Operation {
    pub fn is_mutating(self) -> bool {
        matches!(self, Self::AddDevice | Self::RemoveDevice)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorCode {
    #[serde(rename = "InvalidArgument")]
    InvalidArgument,
    #[serde(rename = "AlreadyExists")]
    AlreadyExists,
    #[serde(rename = "NotFound")]
    NotFound,
    #[serde(rename = "Busy")]
    Busy,
    #[serde(rename = "Internal")]
    Internal,
    #[serde(rename = "Timeout")]
    Timeout,
    #[serde(rename = "Unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default = "empty_object")]
    pub details: Value,
}

impl ErrorBody {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: empty_object(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResponseEnvelope {
    pub version: String,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<Value>,
    pub error: Option<ErrorBody>,
}

impl ResponseEnvelope {
    pub fn ok(request_id: impl Into<String>, result: impl Serialize) -> Self {
        let result = serde_json::to_value(result).unwrap_or(Value::Null);
        Self {
            version: PROTOCOL_VERSION.to_string(),
            request_id: request_id.into(),
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(
        request_id: impl Into<String>,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION.to_string(),
            request_id: request_id.into(),
            ok: false,
            result: None,
            error: Some(ErrorBody::new(code, message)),
        }
    }

    pub fn from_protocol_error(error: ProtocolError) -> Self {
        Self {
            version: PROTOCOL_VERSION.to_string(),
            request_id: error.request_id.unwrap_or_default(),
            ok: false,
            result: None,
            error: Some(error.error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddDeviceRequest {
    pub volume_id: String,
    #[serde(default)]
    pub ublk_device_id: Option<u32>,
    #[serde(default)]
    pub overrides: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoveDeviceRequest {
    #[serde(default)]
    pub device_id: Option<u32>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListDevicesRequest {
    #[serde(default)]
    pub device_id: Option<u32>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthRequest {}

#[derive(Clone, Debug, PartialEq)]
pub enum RequestBody {
    AddDevice(AddDeviceRequest),
    RemoveDevice(RemoveDeviceRequest),
    ListDevices(ListDevicesRequest),
    Health(HealthRequest),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ControlRequest {
    pub request_id: String,
    pub idempotency_key: Option<String>,
    pub body: RequestBody,
}

impl ControlRequest {
    pub fn operation(&self) -> Operation {
        match self.body {
            RequestBody::AddDevice(_) => Operation::AddDevice,
            RequestBody::RemoveDevice(_) => Operation::RemoveDevice,
            RequestBody::ListDevices(_) => Operation::ListDevices,
            RequestBody::Health(_) => Operation::Health,
        }
    }

    pub fn body_fingerprint(&self) -> Result<Vec<u8>, serde_json::Error> {
        match &self.body {
            RequestBody::AddDevice(body) => serde_json::to_vec(body),
            RequestBody::RemoveDevice(body) => serde_json::to_vec(body),
            RequestBody::ListDevices(body) => serde_json::to_vec(body),
            RequestBody::Health(body) => serde_json::to_vec(body),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProtocolError {
    pub request_id: Option<String>,
    pub error: ErrorBody,
}

impl ProtocolError {
    pub fn new(request_id: Option<String>, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            request_id,
            error: ErrorBody::new(code, message),
        }
    }

    pub fn invalid(request_id: Option<String>, message: impl Into<String>) -> Self {
        Self::new(request_id, ErrorCode::InvalidArgument, message)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RequestEnvelope {
    version: String,
    request_id: String,
    #[serde(default)]
    idempotency_key: Option<String>,
    op: Operation,
    #[serde(default = "empty_object")]
    body: Value,
}

pub fn decode_request(payload: &[u8]) -> Result<ControlRequest, ProtocolError> {
    let envelope: RequestEnvelope = serde_json::from_slice(payload)
        .map_err(|err| ProtocolError::invalid(None, format!("invalid JSON payload: {err}")))?;

    if envelope.version != PROTOCOL_VERSION {
        return Err(ProtocolError::invalid(
            Some(envelope.request_id),
            format!(
                "unsupported protocol version '{}' (expected '{}')",
                envelope.version, PROTOCOL_VERSION
            ),
        ));
    }

    let request_id = envelope.request_id.trim().to_string();
    if request_id.is_empty() {
        return Err(ProtocolError::invalid(
            Some(envelope.request_id),
            "request_id must be non-empty",
        ));
    }

    let idempotency_key = envelope
        .idempotency_key
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if envelope.op.is_mutating() && idempotency_key.is_none() {
        return Err(ProtocolError::invalid(
            Some(request_id),
            format!(
                "idempotency_key is required and non-empty for {:?}",
                envelope.op
            ),
        ));
    }

    let body = match envelope.op {
        Operation::AddDevice => {
            let body: AddDeviceRequest =
                parse_body(Some(request_id.clone()), envelope.body, "AddDevice body")?;
            if body.volume_id.trim().is_empty() {
                return Err(ProtocolError::invalid(
                    Some(request_id),
                    "AddDevice body.volume_id must be non-empty",
                ));
            }
            RequestBody::AddDevice(body)
        }
        Operation::RemoveDevice => {
            let body: RemoveDeviceRequest =
                parse_body(Some(request_id.clone()), envelope.body, "RemoveDevice body")?;
            let has_device_id = body.device_id.is_some();
            let has_volume_id = body
                .volume_id
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if !has_device_id && !has_volume_id {
                return Err(ProtocolError::invalid(
                    Some(request_id),
                    "RemoveDevice requires device_id or volume_id",
                ));
            }
            RequestBody::RemoveDevice(body)
        }
        Operation::ListDevices => {
            let body: ListDevicesRequest =
                parse_body(Some(request_id.clone()), envelope.body, "ListDevices body")?;
            RequestBody::ListDevices(body)
        }
        Operation::Health => {
            let body: HealthRequest =
                parse_body(Some(request_id.clone()), envelope.body, "Health body")?;
            RequestBody::Health(body)
        }
    };

    Ok(ControlRequest {
        request_id,
        idempotency_key,
        body,
    })
}

fn parse_body<T: DeserializeOwned>(
    request_id: Option<String>,
    value: Value,
    context: &str,
) -> Result<T, ProtocolError> {
    serde_json::from_value(value)
        .map_err(|err| ProtocolError::invalid(request_id, format!("invalid {context}: {err}")))
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut length_prefix = [0_u8; 4];
    match reader.read_exact(&mut length_prefix).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }

    let payload_len_u32 = u32::from_be_bytes(length_prefix);
    let payload_len = usize::try_from(payload_len_u32)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "payload length overflow"))?;
    if payload_len == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "payload length must be > 0",
        ));
    }
    if payload_len > max_payload_bytes {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "payload length {} exceeds max {}",
                payload_len, max_payload_bytes
            ),
        ));
    }

    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "payload exceeds u32::MAX"))?;
    writer.write_all(&payload_len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn decode_request_requires_idempotency_for_mutations() {
        let payload = br#"{
            "version":"v1",
            "request_id":"req-1",
            "op":"AddDevice",
            "body":{"volume_id":"vol-a"}
        }"#;

        let err = decode_request(payload).expect_err("AddDevice without idempotency_key must fail");
        assert_eq!(err.error.code, ErrorCode::InvalidArgument);
        assert!(err.error.message.contains("idempotency_key"));
    }

    #[test]
    fn decode_request_accepts_read_only_without_idempotency_key() {
        let payload = br#"{
            "version":"v1",
            "request_id":"req-2",
            "op":"ListDevices",
            "body":{}
        }"#;

        let req = decode_request(payload).expect("ListDevices should parse");
        assert_eq!(req.operation(), Operation::ListDevices);
        assert!(req.idempotency_key.is_none());
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let (mut writer, mut reader) = duplex(1024);
        let payload = br#"{"hello":"world"}"#.to_vec();

        write_frame(&mut writer, &payload)
            .await
            .expect("frame should write");
        let out = read_frame(&mut reader, DEFAULT_MAX_PAYLOAD_BYTES)
            .await
            .expect("frame should read")
            .expect("frame payload should exist");

        assert_eq!(out, payload);
    }
}
