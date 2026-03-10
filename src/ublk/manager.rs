use crate::control::protocol::{
    AddDeviceRequest, ControlRequest, ErrorBody, ErrorCode, ListDevicesRequest, Operation,
    RemoveDeviceRequest, RequestBody, ResponseEnvelope,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeviceState {
    #[serde(rename = "Allocating")]
    Allocating,
    #[serde(rename = "OpeningVolume")]
    OpeningVolume,
    #[serde(rename = "Serving")]
    Serving,
    #[serde(rename = "Draining")]
    Draining,
    #[serde(rename = "Detached")]
    Detached,
    #[serde(rename = "Failed")]
    Failed,
    #[serde(rename = "ForceDetached")]
    ForceDetached,
}

impl DeviceState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Detached | Self::Failed | Self::ForceDetached)
    }

    fn parse_filter(value: &str) -> Option<Self> {
        match value {
            "Allocating" => Some(Self::Allocating),
            "OpeningVolume" => Some(Self::OpeningVolume),
            "Serving" => Some(Self::Serving),
            "Draining" => Some(Self::Draining),
            "Detached" => Some(Self::Detached),
            "Failed" => Some(Self::Failed),
            "ForceDetached" => Some(Self::ForceDetached),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddDeviceResult {
    pub device_id: u32,
    pub device_path: String,
    pub volume_id: String,
    pub state: DeviceState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoveDeviceResult {
    pub device_id: Option<u32>,
    pub volume_id: Option<String>,
    pub state: DeviceState,
    pub noop: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceStatus {
    pub device_id: u32,
    pub device_path: String,
    pub volume_id: String,
    pub state: DeviceState,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListDevicesResult {
    pub devices: Vec<DeviceStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResult {
    pub status: String,
    pub active_devices: usize,
    pub total_devices: usize,
    pub degraded_devices: Vec<DeviceStatus>,
}

#[derive(Clone, Debug)]
struct DeviceRecord {
    device_id: u32,
    volume_id: String,
    device_path: String,
    state: DeviceState,
    last_error: Option<String>,
    updated_seq: u64,
}

impl DeviceRecord {
    fn status(&self) -> DeviceStatus {
        DeviceStatus {
            device_id: self.device_id,
            device_path: self.device_path.clone(),
            volume_id: self.volume_id.clone(),
            state: self.state,
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct CachedIdempotentOutcome {
    op: Operation,
    fingerprint: Vec<u8>,
    ok: bool,
    result: Option<Value>,
    error: Option<ErrorBody>,
}

impl CachedIdempotentOutcome {
    fn from_response(op: Operation, fingerprint: Vec<u8>, response: &ResponseEnvelope) -> Self {
        Self {
            op,
            fingerprint,
            ok: response.ok,
            result: response.result.clone(),
            error: response.error.clone(),
        }
    }

    fn to_response(&self, request_id: String) -> ResponseEnvelope {
        ResponseEnvelope {
            version: crate::control::protocol::PROTOCOL_VERSION.to_string(),
            request_id,
            ok: self.ok,
            result: self.result.clone(),
            error: self.error.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ControlManager {
    max_devices: usize,
    terminal_history_limit: usize,
    idempotency_cache_limit: usize,
    next_device_id: u32,
    next_sequence: u64,
    devices: BTreeMap<u32, DeviceRecord>,
    active_volume_to_device: HashMap<String, u32>,
    idempotency_cache: HashMap<String, CachedIdempotentOutcome>,
}

impl ControlManager {
    pub fn new(
        max_devices: usize,
        terminal_history_limit: usize,
        idempotency_cache_limit: usize,
    ) -> Self {
        Self {
            max_devices: max_devices.max(1),
            terminal_history_limit: terminal_history_limit.max(1),
            idempotency_cache_limit: idempotency_cache_limit.max(16),
            next_device_id: 0,
            next_sequence: 0,
            devices: BTreeMap::new(),
            active_volume_to_device: HashMap::new(),
            idempotency_cache: HashMap::new(),
        }
    }

    pub fn handle_request(&mut self, request: ControlRequest) -> ResponseEnvelope {
        let request_id = request.request_id.clone();
        match request.body {
            RequestBody::AddDevice(body) => {
                let key = request.idempotency_key.unwrap_or_default();
                let fingerprint = serde_json::to_vec(&body).unwrap_or_default();
                self.handle_add_with_idempotency(request_id, key, fingerprint, body)
            }
            RequestBody::RemoveDevice(body) => {
                let key = request.idempotency_key.unwrap_or_default();
                let fingerprint = serde_json::to_vec(&body).unwrap_or_default();
                self.handle_remove_with_idempotency(request_id, key, fingerprint, body)
            }
            RequestBody::ListDevices(body) => self.handle_list_devices(request_id, body),
            RequestBody::Health(_body) => self.handle_health(request_id),
        }
    }

    pub fn lookup_cached_mutating_outcome(
        &self,
        request_id: String,
        idempotency_key: &str,
        op: Operation,
        fingerprint: &[u8],
    ) -> Option<ResponseEnvelope> {
        if idempotency_key.trim().is_empty() {
            return Some(ResponseEnvelope::error(
                request_id,
                ErrorCode::InvalidArgument,
                "idempotency_key is required for mutating operations",
            ));
        }

        let cached = self.idempotency_cache.get(idempotency_key)?;
        if cached.op != op || cached.fingerprint != fingerprint {
            return Some(ResponseEnvelope::error(
                request_id,
                ErrorCode::InvalidArgument,
                "idempotency_key was reused with a different operation or payload",
            ));
        }
        Some(cached.to_response(request_id))
    }

    pub fn cache_mutating_outcome(
        &mut self,
        idempotency_key: String,
        op: Operation,
        fingerprint: Vec<u8>,
        response: &ResponseEnvelope,
    ) {
        self.insert_idempotency_outcome(idempotency_key, op, fingerprint, response);
    }

    pub fn add_device(&mut self, request_id: String, body: AddDeviceRequest) -> ResponseEnvelope {
        self.handle_add_device(request_id, body)
    }

    pub fn remove_device(
        &mut self,
        request_id: String,
        body: RemoveDeviceRequest,
    ) -> ResponseEnvelope {
        self.handle_remove_device(request_id, body)
    }

    pub fn list_devices(&self, request_id: String, body: ListDevicesRequest) -> ResponseEnvelope {
        self.handle_list_devices(request_id, body)
    }

    pub fn health(&self, request_id: String) -> ResponseEnvelope {
        self.handle_health(request_id)
    }

    fn handle_add_with_idempotency(
        &mut self,
        request_id: String,
        idempotency_key: String,
        fingerprint: Vec<u8>,
        body: AddDeviceRequest,
    ) -> ResponseEnvelope {
        if let Some(cached) = self.lookup_cached_mutating_outcome(
            request_id.clone(),
            &idempotency_key,
            Operation::AddDevice,
            &fingerprint,
        ) {
            return cached;
        }

        let response = self.add_device(request_id, body);
        self.cache_mutating_outcome(
            idempotency_key,
            Operation::AddDevice,
            fingerprint,
            &response,
        );
        response
    }

    fn handle_remove_with_idempotency(
        &mut self,
        request_id: String,
        idempotency_key: String,
        fingerprint: Vec<u8>,
        body: RemoveDeviceRequest,
    ) -> ResponseEnvelope {
        if let Some(cached) = self.lookup_cached_mutating_outcome(
            request_id.clone(),
            &idempotency_key,
            Operation::RemoveDevice,
            &fingerprint,
        ) {
            return cached;
        }

        let response = self.remove_device(request_id, body);
        self.cache_mutating_outcome(
            idempotency_key,
            Operation::RemoveDevice,
            fingerprint,
            &response,
        );
        response
    }

    fn insert_idempotency_outcome(
        &mut self,
        idempotency_key: String,
        op: Operation,
        fingerprint: Vec<u8>,
        response: &ResponseEnvelope,
    ) {
        if self.idempotency_cache.len() >= self.idempotency_cache_limit {
            if let Some(old_key) = self.idempotency_cache.keys().next().cloned() {
                self.idempotency_cache.remove(&old_key);
            }
        }

        self.idempotency_cache.insert(
            idempotency_key,
            CachedIdempotentOutcome::from_response(op, fingerprint, response),
        );
    }

    fn handle_add_device(
        &mut self,
        request_id: String,
        body: AddDeviceRequest,
    ) -> ResponseEnvelope {
        let volume_id = body.volume_id.trim().to_string();
        if volume_id.is_empty() {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::InvalidArgument,
                "volume_id must be non-empty",
            );
        }
        if self.active_volume_to_device.contains_key(&volume_id) {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::AlreadyExists,
                format!("volume_id '{}' is already attached", volume_id),
            );
        }
        if self.active_device_count() >= self.max_devices {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::Unavailable,
                format!("max_devices={} reached", self.max_devices),
            );
        }

        let device_id = match body.ublk_device_id {
            Some(id) => {
                if self.device_active(id) {
                    return ResponseEnvelope::error(
                        request_id,
                        ErrorCode::AlreadyExists,
                        format!("device_id '{}' is already in use", id),
                    );
                }
                id
            }
            None => self.allocate_device_id(),
        };

        let device_path = format!("/dev/ublkb{device_id}");
        let sequence = self.next_sequence();
        let record = DeviceRecord {
            device_id,
            volume_id: volume_id.clone(),
            device_path: device_path.clone(),
            state: DeviceState::Serving,
            last_error: None,
            updated_seq: sequence,
        };
        self.devices.insert(device_id, record);
        self.active_volume_to_device
            .insert(volume_id.clone(), device_id);

        ResponseEnvelope::ok(
            request_id,
            AddDeviceResult {
                device_id,
                device_path,
                volume_id,
                state: DeviceState::Serving,
            },
        )
    }

    fn handle_remove_device(
        &mut self,
        request_id: String,
        body: RemoveDeviceRequest,
    ) -> ResponseEnvelope {
        let volume_id = body
            .volume_id
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let target_by_volume = volume_id
            .as_deref()
            .and_then(|id| self.active_volume_to_device.get(id).copied())
            .or_else(|| {
                volume_id
                    .as_deref()
                    .and_then(|id| self.find_recent_device_by_volume(id))
            });
        let target = match (body.device_id, target_by_volume) {
            (Some(device_id), Some(by_volume)) if device_id != by_volume => {
                return ResponseEnvelope::error(
                    request_id,
                    ErrorCode::InvalidArgument,
                    "device_id and volume_id refer to different devices",
                );
            }
            (Some(device_id), _) => Some(device_id),
            (None, by_volume) => by_volume,
        };

        let final_state = if body.force {
            DeviceState::ForceDetached
        } else {
            DeviceState::Detached
        };

        let Some(device_id) = target else {
            return ResponseEnvelope::ok(
                request_id,
                RemoveDeviceResult {
                    device_id: body.device_id,
                    volume_id,
                    state: final_state,
                    noop: true,
                },
            );
        };

        let draining_seq = self.next_sequence();
        let final_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return ResponseEnvelope::ok(
                request_id,
                RemoveDeviceResult {
                    device_id: Some(device_id),
                    volume_id,
                    state: final_state,
                    noop: true,
                },
            );
        };

        let mut noop = false;
        if record.state.is_terminal() {
            noop = true;
        } else {
            record.state = DeviceState::Draining;
            record.updated_seq = draining_seq;
            record.state = final_state;
            record.updated_seq = final_seq;
            self.active_volume_to_device.remove(&record.volume_id);
        }

        let actual_volume_id = Some(record.volume_id.clone());
        let result_state = record.state;
        let result_noop = noop;
        let _ = record;
        self.prune_terminal_history();
        ResponseEnvelope::ok(
            request_id,
            RemoveDeviceResult {
                device_id: Some(device_id),
                volume_id: actual_volume_id,
                state: result_state,
                noop: result_noop,
            },
        )
    }

    fn handle_list_devices(
        &self,
        request_id: String,
        body: ListDevicesRequest,
    ) -> ResponseEnvelope {
        let state_filter = match body.state.as_deref() {
            Some(value) => match DeviceState::parse_filter(value) {
                Some(state) => Some(state),
                None => {
                    return ResponseEnvelope::error(
                        request_id,
                        ErrorCode::InvalidArgument,
                        format!("unknown state filter '{}'", value),
                    );
                }
            },
            None => None,
        };

        let devices = self
            .devices
            .values()
            .filter(|record| {
                body.device_id
                    .map(|id| record.device_id == id)
                    .unwrap_or(true)
            })
            .filter(|record| {
                body.volume_id
                    .as_ref()
                    .map(|value| record.volume_id == value.as_str())
                    .unwrap_or(true)
            })
            .filter(|record| {
                state_filter
                    .map(|state| record.state == state)
                    .unwrap_or(true)
            })
            .map(DeviceRecord::status)
            .collect();

        ResponseEnvelope::ok(request_id, ListDevicesResult { devices })
    }

    fn handle_health(&self, request_id: String) -> ResponseEnvelope {
        let degraded_devices = self
            .devices
            .values()
            .filter(|record| {
                matches!(
                    record.state,
                    DeviceState::Failed | DeviceState::ForceDetached
                )
            })
            .map(DeviceRecord::status)
            .collect::<Vec<_>>();
        let active_devices = self.active_device_count();

        ResponseEnvelope::ok(
            request_id,
            HealthResult {
                status: if degraded_devices.is_empty() {
                    "ok".to_string()
                } else {
                    "degraded".to_string()
                },
                active_devices,
                total_devices: self.devices.len(),
                degraded_devices,
            },
        )
    }

    fn active_device_count(&self) -> usize {
        self.devices
            .values()
            .filter(|record| !record.state.is_terminal())
            .count()
    }

    fn device_active(&self, device_id: u32) -> bool {
        self.devices
            .get(&device_id)
            .is_some_and(|record| !record.state.is_terminal())
    }

    fn allocate_device_id(&mut self) -> u32 {
        loop {
            let candidate = self.next_device_id;
            self.next_device_id = self.next_device_id.saturating_add(1);
            if !self.device_active(candidate) {
                return candidate;
            }
        }
    }

    fn find_recent_device_by_volume(&self, volume_id: &str) -> Option<u32> {
        self.devices
            .values()
            .filter(|record| record.volume_id == volume_id)
            .max_by_key(|record| record.updated_seq)
            .map(|record| record.device_id)
    }

    fn prune_terminal_history(&mut self) {
        let terminal_count = self
            .devices
            .values()
            .filter(|record| record.state.is_terminal())
            .count();
        if terminal_count <= self.terminal_history_limit {
            return;
        }

        let mut terminal_entries = self
            .devices
            .iter()
            .filter(|(_, record)| record.state.is_terminal())
            .map(|(device_id, record)| (*device_id, record.updated_seq))
            .collect::<Vec<_>>();
        terminal_entries.sort_by_key(|(_, seq)| *seq);

        let remove_count = terminal_count - self.terminal_history_limit;
        for (device_id, _) in terminal_entries.into_iter().take(remove_count) {
            self.devices.remove(&device_id);
        }
    }

    fn next_sequence(&mut self) -> u64 {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.next_sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::protocol::{decode_request, ErrorCode};

    fn parse_request(json: &str) -> ControlRequest {
        decode_request(json.as_bytes()).expect("request should parse")
    }

    #[test]
    fn add_device_replay_returns_same_result() {
        let mut manager = ControlManager::new(8, 128, 128);
        let request = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }"#,
        );
        let replay = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-2",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }"#,
        );

        let first = manager.handle_request(request);
        let second = manager.handle_request(replay);

        assert!(first.ok);
        assert!(second.ok);
        assert_eq!(first.result, second.result);
        assert_eq!(second.request_id, "req-2");
    }

    #[test]
    fn add_device_reuse_key_with_different_body_is_rejected() {
        let mut manager = ControlManager::new(8, 128, 128);
        let first = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }"#,
        );
        let second = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-2",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-b"}
            }"#,
        );

        assert!(manager.handle_request(first).ok);
        let response = manager.handle_request(second);
        assert!(!response.ok);
        let error = response.error.expect("error body should exist");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn add_device_rejects_duplicate_active_volume() {
        let mut manager = ControlManager::new(8, 128, 128);
        let first = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }"#,
        );
        let second = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-2",
                "idempotency_key":"key-2",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }"#,
        );

        assert!(manager.handle_request(first).ok);
        let response = manager.handle_request(second);
        assert!(!response.ok);
        let error = response.error.expect("error body should exist");
        assert_eq!(error.code, ErrorCode::AlreadyExists);
    }

    #[test]
    fn remove_device_missing_target_is_success_noop() {
        let mut manager = ControlManager::new(8, 128, 128);
        let request = parse_request(
            r#"{
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"remove-1",
                "op":"RemoveDevice",
                "body":{"volume_id":"missing"}
            }"#,
        );

        let response = manager.handle_request(request);
        assert!(response.ok);

        let result: RemoveDeviceResult =
            serde_json::from_value(response.result.expect("remove result should exist"))
                .expect("result should deserialize");
        assert!(result.noop);
        assert_eq!(result.state, DeviceState::Detached);
    }
}
