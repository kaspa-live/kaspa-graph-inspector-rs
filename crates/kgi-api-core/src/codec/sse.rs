use serde::Serialize;

use crate::publication::GraphPublicationState;

use super::{lifecycle::GraphPublicationStateDto, scalar::U64Dto, to_json_bytes};

/// SSE event name carrying a fresh client registration.
pub(crate) const CLIENT_REGISTRATION_EVENT: &str = "client-registration";
/// SSE event name carrying a graph revision wakeup.
pub(crate) const PUBLICATION_WAKEUP_EVENT: &str = "publication-wakeup";
/// SSE event name carrying a publication lifecycle observation.
pub(crate) const PUBLICATION_STATE_EVENT: &str = "publication-state";

/// Payload assigning one opaque SSE client identifier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ClientRegistrationDto {
    client_id: String,
}

impl ClientRegistrationDto {
    /// Constructs one client-registration payload.
    pub(crate) fn new(client_id: String) -> Self {
        Self { client_id }
    }

    /// Serializes this payload as compact JSON.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(self)
    }
}

/// Payload indicating that a registered client can attempt graph catch-up.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct PublicationWakeupDto {
    publication_id: U64Dto,
    revision: U64Dto,
}

impl PublicationWakeupDto {
    /// Constructs one publication-wakeup payload.
    pub(crate) fn new(publication_id: u64, revision: u64) -> Self {
        Self { publication_id: publication_id.into(), revision: revision.into() }
    }

    /// Serializes this payload as compact JSON.
    pub(crate) fn encode(self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(&self)
    }
}

/// Payload establishing or changing an observed publication lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct PublicationStateDto {
    publication_id: U64Dto,
    revision: U64Dto,
    state: GraphPublicationStateDto,
}

impl PublicationStateDto {
    /// Constructs one publication-state payload.
    pub(crate) fn new(publication_id: u64, revision: u64, state: GraphPublicationState) -> Self {
        Self { publication_id: publication_id.into(), revision: revision.into(), state: state.into() }
    }

    /// Serializes this payload as compact JSON.
    pub(crate) fn encode(self) -> Result<Vec<u8>, serde_json::Error> {
        to_json_bytes(&self)
    }
}

#[cfg(test)]
mod tests {
    use crate::publication::GraphPublicationState;

    use super::{
        CLIENT_REGISTRATION_EVENT, ClientRegistrationDto, PUBLICATION_STATE_EVENT, PUBLICATION_WAKEUP_EVENT, PublicationStateDto,
        PublicationWakeupDto,
    };

    #[test]
    fn sse_control_payloads_and_event_names_match_the_protocol() {
        assert_eq!(CLIENT_REGISTRATION_EVENT, "client-registration");
        assert_eq!(PUBLICATION_WAKEUP_EVENT, "publication-wakeup");
        assert_eq!(PUBLICATION_STATE_EVENT, "publication-state");

        assert_eq!(ClientRegistrationDto::new("K7m...".to_owned()).encode().expect("registration JSON"), br#"{"client_id":"K7m..."}"#);
        assert_eq!(
            PublicationWakeupDto::new(42, 123_456).encode().expect("wakeup JSON"),
            br#"{"publication_id":"42","revision":"123456"}"#
        );
        assert_eq!(
            PublicationStateDto::new(42, 123_456, GraphPublicationState::Live).encode().expect("state JSON"),
            br#"{"publication_id":"42","revision":"123456","state":"live"}"#
        );
    }
}
