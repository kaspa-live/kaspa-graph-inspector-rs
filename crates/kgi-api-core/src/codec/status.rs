use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kgi_api_model::protocol::SystemStatus;
use kgi_model::lifecycle::{NodeServiceStatus, ProcessingState, ProcessingStatus, StorageServiceStatus, SupervisorStatus};
use serde::Serialize;

use super::{
    lifecycle::{
        NodeServiceStatusStateDto, ProcessingStateNameDto, RecoveryModeDto, StorageServiceStatusStateDto, SupervisorLifecycleDto,
    },
    to_json_bytes,
};

/// Complete wire representation of the public system status.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SystemStatusDto {
    kgi_version: String,
    supervisor: SupervisorStatusDto,
    node: NodeServiceStatusDto,
    storage: StorageServiceStatusDto,
    processing: ProcessingStatusDto,
}

impl From<&SystemStatus> for SystemStatusDto {
    fn from(value: &SystemStatus) -> Self {
        Self {
            kgi_version: value.kgi_version.clone(),
            supervisor: value.supervisor.into(),
            node: (&value.node).into(),
            storage: value.storage.into(),
            processing: value.processing.into(),
        }
    }
}

impl SystemStatusDto {
    /// Serializes a semantic system-status value as compact JSON.
    pub(crate) fn encode(value: &SystemStatus) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(&Self::from(value))
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct SupervisorStatusDto {
    lifecycle: SupervisorLifecycleDto,
    desired_recovery: Option<RecoveryModeDto>,
    active_recovery: Option<RecoveryModeDto>,
}

impl From<SupervisorStatus> for SupervisorStatusDto {
    fn from(value: SupervisorStatus) -> Self {
        Self {
            lifecycle: value.lifecycle.into(),
            desired_recovery: value.desired_recovery.map(Into::into),
            active_recovery: value.active_recovery.map(Into::into),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct NodeServiceStatusDto {
    state: NodeServiceStatusStateDto,
    last_validated: Option<ValidatedNodeStatusDto>,
}

impl From<&NodeServiceStatus> for NodeServiceStatusDto {
    fn from(value: &NodeServiceStatus) -> Self {
        Self { state: value.state.into(), last_validated: value.last_validated.as_ref().map(Into::into) }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ValidatedNodeStatusDto {
    network_id: NetworkIdDto,
    server_version: String,
    rpc_api_version: Option<u16>,
    rpc_api_revision: Option<u16>,
}

impl From<&kgi_model::lifecycle::ValidatedNodeStatus> for ValidatedNodeStatusDto {
    fn from(value: &kgi_model::lifecycle::ValidatedNodeStatus) -> Self {
        Self {
            network_id: value.network_id.into(),
            server_version: value.server_version.clone(),
            rpc_api_version: value.rpc_api_version,
            rpc_api_revision: value.rpc_api_revision,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct NetworkIdDto {
    network_type: NetworkTypeDto,
    suffix: Option<u32>,
}

impl From<NetworkId> for NetworkIdDto {
    fn from(value: NetworkId) -> Self {
        Self { network_type: value.network_type().into(), suffix: value.suffix() }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NetworkTypeDto {
    Mainnet,
    Testnet,
    Devnet,
    Simnet,
}

impl From<NetworkType> for NetworkTypeDto {
    fn from(value: NetworkType) -> Self {
        match value {
            NetworkType::Mainnet => Self::Mainnet,
            NetworkType::Testnet => Self::Testnet,
            NetworkType::Devnet => Self::Devnet,
            NetworkType::Simnet => Self::Simnet,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct StorageServiceStatusDto {
    state: StorageServiceStatusStateDto,
}

impl From<StorageServiceStatus> for StorageServiceStatusDto {
    fn from(value: StorageServiceStatus) -> Self {
        Self { state: value.state.into() }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct ProcessingStatusDto {
    state: ProcessingStateNameDto,
    mode: Option<RecoveryModeDto>,
}

impl From<ProcessingStatus> for ProcessingStatusDto {
    fn from(value: ProcessingStatus) -> Self {
        let mode = match value.state {
            ProcessingState::Reconciling { mode } => Some(mode.into()),
            _ => None,
        };
        Self { state: value.state.into(), mode }
    }
}

#[cfg(test)]
mod tests {
    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use kgi_api_model::protocol::SystemStatus;
    use kgi_model::lifecycle::{
        NodeServiceStatus, NodeServiceStatusState, ProcessingState, ProcessingStatus, RecoveryMode, StorageServiceStatus,
        StorageServiceStatusState, SupervisorLifecycle, SupervisorStatus, ValidatedNodeStatus,
    };
    use serde_json::json;

    use super::{NetworkTypeDto, SystemStatusDto};

    #[test]
    fn network_types_use_exact_public_spellings() {
        let cases = [
            (NetworkType::Mainnet, "\"mainnet\""),
            (NetworkType::Testnet, "\"testnet\""),
            (NetworkType::Devnet, "\"devnet\""),
            (NetworkType::Simnet, "\"simnet\""),
        ];
        for (network_type, expected) in cases {
            assert_eq!(serde_json::to_string(&NetworkTypeDto::from(network_type)).expect("network JSON"), expected);
        }
    }

    #[test]
    fn system_status_maps_component_values_and_explicit_nulls() {
        let status = SystemStatus {
            kgi_version: "0.1.0".to_owned(),
            supervisor: SupervisorStatus {
                lifecycle: SupervisorLifecycle::ShuttingDown,
                desired_recovery: Some(RecoveryMode::Rebuild),
                active_recovery: None,
            },
            node: NodeServiceStatus {
                state: NodeServiceStatusState::Ready,
                last_validated: Some(ValidatedNodeStatus {
                    network_id: NetworkId::with_suffix(NetworkType::Testnet, 12),
                    server_version: "2.1.0".to_owned(),
                    rpc_api_version: Some(1),
                    rpc_api_revision: None,
                }),
            },
            storage: StorageServiceStatus { state: StorageServiceStatusState::AwaitingInitialization },
            processing: ProcessingStatus { state: ProcessingState::Reconciling { mode: RecoveryMode::Resync } },
        };

        let encoded = SystemStatusDto::encode(&status).expect("status JSON");
        assert!(!encoded.contains(&b' '));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).expect("status value"),
            json!({
                "kgi_version": "0.1.0",
                "supervisor": {
                    "lifecycle": "shutting-down",
                    "desired_recovery": "rebuild",
                    "active_recovery": null
                },
                "node": {
                    "state": "ready",
                    "last_validated": {
                        "network_id": { "network_type": "testnet", "suffix": 12 },
                        "server_version": "2.1.0",
                        "rpc_api_version": 1,
                        "rpc_api_revision": null
                    }
                },
                "storage": { "state": "awaiting-initialization" },
                "processing": { "state": "reconciling", "mode": "resync" }
            })
        );
    }

    #[test]
    fn non_reconciling_processing_state_has_no_recovery_mode() {
        let status = SystemStatus {
            kgi_version: "0.1.0".to_owned(),
            supervisor: SupervisorStatus { lifecycle: SupervisorLifecycle::Running, desired_recovery: None, active_recovery: None },
            node: NodeServiceStatus { state: NodeServiceStatusState::Connecting, last_validated: None },
            storage: StorageServiceStatus { state: StorageServiceStatusState::Ready },
            processing: ProcessingStatus { state: ProcessingState::Deactivating },
        };

        let encoded = SystemStatusDto::encode(&status).expect("status JSON");
        let value = serde_json::from_slice::<serde_json::Value>(&encoded).expect("status value");
        assert_eq!(value["node"]["last_validated"], serde_json::Value::Null);
        assert_eq!(value["processing"], json!({ "state": "stopping", "mode": null }));
    }
}
