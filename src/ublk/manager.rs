use crate::control::protocol::{
    AddDeviceRequest, ErrorBody, ErrorCode, ListDevicesRequest, Operation, RemoveDeviceRequest,
    ResponseEnvelope,
};
#[cfg(test)]
use crate::control::protocol::{ControlRequest, RequestBody};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};

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
pub struct AddDeviceReservation {
    pub device_id: u32,
    pub device_path: String,
    pub volume_id: String,
}

#[derive(Clone, Debug)]
pub enum RemovePreparation {
    Immediate(ResponseEnvelope),
    Draining {
        device_id: u32,
        volume_id: String,
        force: bool,
    },
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
    idempotency_order: VecDeque<String>,
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
            idempotency_order: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub fn handle_request(&mut self, request: ControlRequest) -> ResponseEnvelope {
        let request_id = request.request_id;
        match request.body {
            RequestBody::AddDevice(body) => {
                let key = request.idempotency_key.unwrap_or_default();
                let fingerprint = serde_json::to_vec(&body).unwrap_or_default();
                if let Some(cached) = self.lookup_cached_mutating_outcome(
                    request_id.clone(),
                    &key,
                    Operation::AddDevice,
                    &fingerprint,
                ) {
                    return cached;
                }

                let response = match self.reserve_add_device(request_id.clone(), &body) {
                    Ok(reservation) => {
                        let _ = self.mark_device_opening(reservation.device_id);
                        self.finalize_add_success(request_id, reservation.device_id)
                    }
                    Err(response) => response,
                };
                self.cache_mutating_outcome(key, Operation::AddDevice, fingerprint, &response);
                response
            }
            RequestBody::RemoveDevice(body) => {
                let key = request.idempotency_key.unwrap_or_default();
                let fingerprint = serde_json::to_vec(&body).unwrap_or_default();
                if let Some(cached) = self.lookup_cached_mutating_outcome(
                    request_id.clone(),
                    &key,
                    Operation::RemoveDevice,
                    &fingerprint,
                ) {
                    return cached;
                }

                let response = match self.prepare_remove_device(request_id.clone(), &body) {
                    RemovePreparation::Immediate(response) => response,
                    RemovePreparation::Draining {
                        device_id, force, ..
                    } => self.complete_remove_device(request_id, device_id, force),
                };
                self.cache_mutating_outcome(key, Operation::RemoveDevice, fingerprint, &response);
                response
            }
            RequestBody::ListDevices(body) => self.list_devices(request_id, body),
            RequestBody::Health(_) => self.health(request_id),
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

    pub fn reserve_add_device(
        &mut self,
        request_id: String,
        body: &AddDeviceRequest,
    ) -> Result<AddDeviceReservation, ResponseEnvelope> {
        let volume_id = body.volume_id.trim().to_string();
        if volume_id.is_empty() {
            return Err(ResponseEnvelope::error(
                request_id,
                ErrorCode::InvalidArgument,
                "volume_id must be non-empty",
            ));
        }
        if self.active_volume_to_device.contains_key(&volume_id) {
            return Err(ResponseEnvelope::error(
                request_id,
                ErrorCode::AlreadyExists,
                format!("volume_id '{}' is already attached", volume_id),
            ));
        }
        if self.active_device_count() >= self.max_devices {
            return Err(ResponseEnvelope::error(
                request_id,
                ErrorCode::Unavailable,
                format!("max_devices={} reached", self.max_devices),
            ));
        }

        let device_id = match body.ublk_device_id {
            Some(id) => {
                if self.device_active(id) {
                    return Err(ResponseEnvelope::error(
                        request_id,
                        ErrorCode::AlreadyExists,
                        format!("device_id '{}' is already in use", id),
                    ));
                }
                id
            }
            None => self.allocate_device_id(request_id.clone())?,
        };

        let device_path = format!("/dev/ublkb{device_id}");
        let record = DeviceRecord {
            device_id,
            volume_id: volume_id.clone(),
            device_path: device_path.clone(),
            state: DeviceState::Allocating,
            last_error: None,
            updated_seq: self.next_sequence(),
        };
        self.devices.insert(device_id, record);
        self.active_volume_to_device
            .insert(volume_id.clone(), device_id);

        Ok(AddDeviceReservation {
            device_id,
            device_path,
            volume_id,
        })
    }

    pub fn mark_device_opening(&mut self, device_id: u32) -> Result<(), ResponseEnvelope> {
        let Some(state) = self.devices.get(&device_id).map(|record| record.state) else {
            return Err(ResponseEnvelope::error(
                format!("add-device:{device_id}"),
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            ));
        };

        if state == DeviceState::OpeningVolume {
            return Ok(());
        }
        if state.is_terminal() {
            return Err(ResponseEnvelope::error(
                format!("add-device:{device_id}"),
                ErrorCode::Busy,
                format!(
                    "device_id '{}' is in terminal state '{}'",
                    device_id,
                    state_name(state)
                ),
            ));
        }

        let updated_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return Err(ResponseEnvelope::error(
                format!("add-device:{device_id}"),
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            ));
        };
        record.state = DeviceState::OpeningVolume;
        record.updated_seq = updated_seq;
        Ok(())
    }

    pub fn finalize_add_success(&mut self, request_id: String, device_id: u32) -> ResponseEnvelope {
        let updated_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::Internal,
                format!(
                    "device_id '{}' missing before final add transition",
                    device_id
                ),
            );
        };

        record.state = DeviceState::Serving;
        record.last_error = None;
        record.updated_seq = updated_seq;
        ResponseEnvelope::ok(
            request_id,
            AddDeviceResult {
                device_id: record.device_id,
                device_path: record.device_path.clone(),
                volume_id: record.volume_id.clone(),
                state: record.state,
            },
        )
    }

    pub fn fail_add_device(
        &mut self,
        request_id: String,
        device_id: u32,
        code: ErrorCode,
        message: String,
    ) -> ResponseEnvelope {
        let updated_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::Internal,
                format!(
                    "device_id '{}' missing during add failure cleanup",
                    device_id
                ),
            );
        };

        let volume_id = record.volume_id.clone();
        record.state = DeviceState::Failed;
        record.last_error = Some(message.clone());
        record.updated_seq = updated_seq;
        let _ = record;
        self.active_volume_to_device.remove(&volume_id);
        self.prune_terminal_history();
        ResponseEnvelope::error(request_id, code, message)
    }

    pub fn prepare_remove_device(
        &mut self,
        request_id: String,
        body: &RemoveDeviceRequest,
    ) -> RemovePreparation {
        let volume_id = body
            .volume_id
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let target_by_active_volume = volume_id
            .as_deref()
            .and_then(|id| self.active_volume_to_device.get(id).copied());
        let target_by_recent_volume = volume_id
            .as_deref()
            .and_then(|id| self.find_recent_device_by_volume(id));

        let target = match (body.device_id, target_by_active_volume) {
            (Some(device_id), Some(active_id)) if device_id != active_id => {
                return RemovePreparation::Immediate(ResponseEnvelope::error(
                    request_id,
                    ErrorCode::InvalidArgument,
                    "device_id and volume_id refer to different active devices",
                ));
            }
            (Some(device_id), _) => Some(device_id),
            (None, Some(active_id)) => Some(active_id),
            (None, None) => target_by_recent_volume,
        };

        let Some(device_id) = target else {
            return RemovePreparation::Immediate(ResponseEnvelope::error(
                request_id,
                ErrorCode::NotFound,
                "requested device was not found",
            ));
        };

        let Some(state) = self.devices.get(&device_id).map(|record| record.state) else {
            return RemovePreparation::Immediate(ResponseEnvelope::error(
                request_id,
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            ));
        };

        if state == DeviceState::Draining {
            return RemovePreparation::Immediate(ResponseEnvelope::error(
                request_id,
                ErrorCode::Busy,
                format!("device_id '{}' is already draining", device_id),
            ));
        }

        if state.is_terminal() {
            let volume_id = self
                .devices
                .get(&device_id)
                .map(|record| record.volume_id.clone())
                .unwrap_or_default();
            return RemovePreparation::Immediate(ResponseEnvelope::ok(
                request_id,
                RemoveDeviceResult {
                    device_id: Some(device_id),
                    volume_id: Some(volume_id),
                    state,
                    noop: true,
                },
            ));
        }

        let updated_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return RemovePreparation::Immediate(ResponseEnvelope::error(
                request_id,
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            ));
        };
        let volume_id = record.volume_id.clone();
        record.state = DeviceState::Draining;
        record.updated_seq = updated_seq;
        let _ = record;
        self.active_volume_to_device.remove(&volume_id);

        RemovePreparation::Draining {
            device_id,
            volume_id,
            force: body.force,
        }
    }

    pub fn complete_remove_device(
        &mut self,
        request_id: String,
        device_id: u32,
        force_detach: bool,
    ) -> ResponseEnvelope {
        let Some(state) = self.devices.get(&device_id).map(|record| record.state) else {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            );
        };

        if state.is_terminal() {
            let volume_id = self
                .devices
                .get(&device_id)
                .map(|record| record.volume_id.clone())
                .unwrap_or_default();
            return ResponseEnvelope::ok(
                request_id,
                RemoveDeviceResult {
                    device_id: Some(device_id),
                    volume_id: Some(volume_id),
                    state,
                    noop: true,
                },
            );
        }
        if state != DeviceState::Draining {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::Busy,
                format!(
                    "device_id '{}' cannot complete remove from state '{}'",
                    device_id,
                    state_name(state)
                ),
            );
        }

        let updated_seq = self.next_sequence();
        let Some(record) = self.devices.get_mut(&device_id) else {
            return ResponseEnvelope::error(
                request_id,
                ErrorCode::NotFound,
                format!("device_id '{}' not found", device_id),
            );
        };
        record.state = if force_detach {
            DeviceState::ForceDetached
        } else {
            DeviceState::Detached
        };
        record.updated_seq = updated_seq;
        let response = ResponseEnvelope::ok(
            request_id,
            RemoveDeviceResult {
                device_id: Some(device_id),
                volume_id: Some(record.volume_id.clone()),
                state: record.state,
                noop: false,
            },
        );
        self.prune_terminal_history();
        response
    }

    pub fn list_devices(&self, request_id: String, body: ListDevicesRequest) -> ResponseEnvelope {
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

    pub fn health(&self, request_id: String) -> ResponseEnvelope {
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

    fn insert_idempotency_outcome(
        &mut self,
        idempotency_key: String,
        op: Operation,
        fingerprint: Vec<u8>,
        response: &ResponseEnvelope,
    ) {
        if self.idempotency_cache.contains_key(&idempotency_key) {
            self.idempotency_order.retain(|key| key != &idempotency_key);
        }
        self.idempotency_cache.insert(
            idempotency_key.clone(),
            CachedIdempotentOutcome::from_response(op, fingerprint, response),
        );
        self.idempotency_order.push_back(idempotency_key);

        while self.idempotency_cache.len() > self.idempotency_cache_limit {
            let Some(oldest_key) = self.idempotency_order.pop_front() else {
                break;
            };
            self.idempotency_cache.remove(&oldest_key);
        }
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

    fn allocate_device_id(&mut self, request_id: String) -> Result<u32, ResponseEnvelope> {
        let start = self.next_device_id;
        loop {
            let candidate = self.next_device_id;
            self.next_device_id = self.next_device_id.wrapping_add(1);
            if !self.device_active(candidate) {
                return Ok(candidate);
            }
            if self.next_device_id == start {
                return Err(ResponseEnvelope::error(
                    request_id,
                    ErrorCode::Unavailable,
                    "no free device_id available",
                ));
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

fn state_name(state: DeviceState) -> &'static str {
    match state {
        DeviceState::Allocating => "Allocating",
        DeviceState::OpeningVolume => "OpeningVolume",
        DeviceState::Serving => "Serving",
        DeviceState::Draining => "Draining",
        DeviceState::Detached => "Detached",
        DeviceState::Failed => "Failed",
        DeviceState::ForceDetached => "ForceDetached",
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
    fn remove_device_missing_target_returns_not_found() {
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
        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|value| value.code.clone()),
            Some(ErrorCode::NotFound)
        );
    }
}
