#![allow(dead_code, reason = "the client scheduling foundation is completed by later increment points")]

use std::{
    collections::VecDeque,
    sync::{Mutex, Weak},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::codec::sse::{ClientRegistrationDto, PublicationStateDto, PublicationWakeupDto};

/// Maximum number of SSE clients registered with one publication.
pub(crate) const MAX_SSE_CLIENTS: usize = 1_024;
/// Maximum number of semantic messages retained by one SSE mailbox.
pub(crate) const SSE_CLIENT_BUFFER_CAPACITY: usize = 8;

/// One opaque publication-local client identifier before wire encoding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClientIdentifier(u64);

impl ClientIdentifier {
    /// Samples one uniformly random identifier from the supplied source.
    pub(crate) fn generate(random: &mut impl RngCore) -> Self {
        Self(random.next_u64())
    }

    /// Returns the raw key used by the publication-local registry.
    pub(crate) const fn key(self) -> u64 {
        self.0
    }

    /// Encodes the identifier as canonical unpadded base64url.
    pub(crate) fn encode(self) -> String {
        URL_SAFE_NO_PAD.encode(self.0.to_be_bytes())
    }

    /// Constructs the protocol payload assigning this identifier.
    pub(crate) fn registration(self) -> ClientRegistrationDto {
        ClientRegistrationDto::new(self.encode())
    }
}

/// One semantic message delivered by an SSE connection task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SseMessage {
    /// Assigns a fresh opaque client identifier.
    ClientRegistration(ClientRegistrationDto),
    /// Announces an available graph revision.
    PublicationWakeup(PublicationWakeupDto),
    /// Establishes or changes the publication lifecycle state.
    PublicationState(PublicationStateDto),
}

/// One ordered item retained by an SSE mailbox.
#[derive(Clone, Debug, Eq, PartialEq)]
enum SseMailboxItem {
    /// A message whose admission and position are never coalesced.
    Required(SseMessage),
    /// The sole coalescible ordinary graph notification.
    GraphWakeup(PublicationWakeupDto),
}

/// Synchronous state of one bounded SSE mailbox.
struct SseMailboxState {
    closed: bool,
    queue: VecDeque<SseMailboxItem>,
}

/// Reason one synchronous mailbox admission was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SseMailboxAdmissionError {
    /// The connection has closed this mailbox permanently.
    Closed,
    /// The complete item or batch does not fit in the bounded queue.
    Full,
}

/// One connection-owned ordered SSE delivery mailbox.
pub(crate) struct SseMailbox {
    state: Mutex<SseMailboxState>,
    ready: Notify,
}

impl SseMailbox {
    /// Creates one open, empty connection mailbox.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SseMailboxState { closed: false, queue: VecDeque::with_capacity(SSE_CLIENT_BUFFER_CAPACITY) }),
            ready: Notify::new(),
        }
    }

    /// Atomically appends one complete ordered batch of required messages.
    pub(crate) fn admit_required<const N: usize>(&self, messages: [SseMessage; N]) -> Result<(), SseMailboxAdmissionError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }
        if N > SSE_CLIENT_BUFFER_CAPACITY - state.queue.len() {
            return Err(SseMailboxAdmissionError::Full);
        }
        state.queue.extend(messages.into_iter().map(SseMailboxItem::Required));
        drop(state);
        if N != 0 {
            self.ready.notify_one();
        }
        Ok(())
    }

    /// Inserts or coalesces the ordinary wakeup for one publication.
    pub(crate) fn admit_graph_wakeup(&self, wakeup: PublicationWakeupDto) -> Result<(), SseMailboxAdmissionError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }

        let publication_id = wakeup.publication_id();
        if let Some(index) = state
            .queue
            .iter()
            .position(|item| matches!(item, SseMailboxItem::GraphWakeup(queued) if queued.publication_id() == publication_id))
        {
            state.queue.remove(index);
            state.queue.push_back(SseMailboxItem::GraphWakeup(wakeup));
        } else {
            if state.queue.len() == SSE_CLIENT_BUFFER_CAPACITY {
                return Err(SseMailboxAdmissionError::Full);
            }
            state.queue.push_back(SseMailboxItem::GraphWakeup(wakeup));
        }

        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Removes a still-queued ordinary wakeup for one publication.
    pub(crate) fn remove_graph_wakeup(&self, publication_id: u64) -> bool {
        let mut state = self.lock_state();
        let Some(index) = state
            .queue
            .iter()
            .position(|item| matches!(item, SseMailboxItem::GraphWakeup(queued) if queued.publication_id() == publication_id))
        else {
            return false;
        };
        state.queue.remove(index);
        true
    }

    /// Closes admission and wakes the receiver so it can drain and finish.
    pub(crate) fn close(&self) {
        let mut state = self.lock_state();
        if state.closed {
            return;
        }
        state.closed = true;
        drop(state);
        self.ready.notify_one();
    }

    /// Receives the next semantic message, or finishes after a closed queue drains.
    pub(crate) async fn recv(&self) -> Option<SseMessage> {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = self.lock_state();
                if let Some(item) = state.queue.pop_front() {
                    return Some(match item {
                        SseMailboxItem::Required(message) => message,
                        SseMailboxItem::GraphWakeup(wakeup) => SseMessage::PublicationWakeup(wakeup),
                    });
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SseMailboxState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Publication-local wake scheduling state for one live registration.
struct DeltaClientRegistration {
    wake_at_revision_id: u64,
    wake_sent: bool,
    mailbox: Weak<SseMailbox>,
    disconnect: CancellationToken,
}

/// Scheduling update selected by a completed logical delta operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeltaScheduleAction {
    /// Replaces the boundary and its already-sent state.
    Replace { wake_at_revision_id: u64, wake_sent: bool },
    /// Validates the registration without installing another schedule.
    ValidateOnly,
}

/// Result of applying a delta completion to one supplied registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeltaCompletionResult {
    /// The registration existed and the selected action was applied.
    Accepted,
    /// The supplied registration no longer exists in this publication.
    RegistrationMissing,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::rngs::mock::StepRng;

    use crate::{
        codec::sse::{ClientRegistrationDto, PublicationStateDto, PublicationWakeupDto},
        publication::GraphPublicationState,
    };

    use super::{ClientIdentifier, MAX_SSE_CLIENTS, SSE_CLIENT_BUFFER_CAPACITY, SseMailbox, SseMailboxAdmissionError, SseMessage};

    fn required_registration(value: usize) -> SseMessage {
        SseMessage::ClientRegistration(ClientRegistrationDto::new(format!("client-{value}")))
    }

    fn required_state(revision: u64) -> SseMessage {
        SseMessage::PublicationState(PublicationStateDto::new(7, revision, GraphPublicationState::Live))
    }

    fn wakeup(publication_id: u64, revision: u64) -> PublicationWakeupDto {
        PublicationWakeupDto::new(publication_id, revision)
    }

    #[test]
    fn client_identifier_uses_canonical_eleven_character_base64url() {
        let zero = ClientIdentifier(0);
        let maximum = ClientIdentifier(u64::MAX);

        assert_eq!(zero.encode(), "AAAAAAAAAAA");
        assert_eq!(maximum.encode(), "__________8");
        assert_eq!(zero.encode().len(), 11);
        assert_eq!(maximum.encode().len(), 11);
        assert!(!zero.encode().contains('='));
        assert!(!maximum.encode().contains('='));
    }

    #[test]
    fn client_identifier_generation_accepts_a_deterministic_random_source() {
        let mut random = StepRng::new(0x0123_4567_89ab_cdef, 1);

        let first = ClientIdentifier::generate(&mut random);
        let second = ClientIdentifier::generate(&mut random);

        assert_eq!(first.key(), 0x0123_4567_89ab_cdef);
        assert_eq!(second.key(), 0x0123_4567_89ab_cdf0);
        assert_eq!(first.encode().len(), 11);
        assert_eq!(first.registration(), crate::codec::sse::ClientRegistrationDto::new(first.encode()));
    }

    #[test]
    fn client_scheduling_capacities_match_the_service_contract() {
        assert_eq!(MAX_SSE_CLIENTS, 1_024);
        assert_eq!(SSE_CLIENT_BUFFER_CAPACITY, 8);
    }

    #[tokio::test]
    async fn required_batch_admission_is_atomic() {
        let mailbox = SseMailbox::new();
        for value in 0..6 {
            mailbox.admit_required([required_registration(value)]).expect("single required item fits");
        }

        let batch = [required_registration(6), required_state(7), SseMessage::PublicationWakeup(wakeup(7, 7))];
        assert_eq!(mailbox.admit_required(batch), Err(SseMailboxAdmissionError::Full));

        for value in 0..6 {
            assert_eq!(mailbox.recv().await, Some(required_registration(value)));
        }
        mailbox.close();
        assert_eq!(mailbox.recv().await, None);
    }

    #[tokio::test]
    async fn graph_wakeup_coalesces_and_moves_after_new_required_messages() {
        let mailbox = SseMailbox::new();
        mailbox.admit_required([required_registration(0)]).expect("registration fits");
        mailbox.admit_graph_wakeup(wakeup(7, 1)).expect("initial wakeup fits");
        mailbox.admit_required([required_state(1)]).expect("state message fits");
        mailbox.admit_graph_wakeup(wakeup(7, 2)).expect("wakeup coalesces");

        assert_eq!(mailbox.recv().await, Some(required_registration(0)));
        assert_eq!(mailbox.recv().await, Some(required_state(1)));
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 2))));
    }

    #[tokio::test]
    async fn required_admission_never_evicts_an_ordinary_wakeup() {
        let mailbox = SseMailbox::new();
        mailbox.admit_graph_wakeup(wakeup(7, 1)).expect("initial wakeup fits");
        for value in 0..7 {
            mailbox.admit_required([required_registration(value)]).expect("required item fits");
        }

        assert_eq!(mailbox.admit_required([required_state(2)]), Err(SseMailboxAdmissionError::Full));
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 1))));
    }

    #[test]
    fn first_graph_wakeup_rejects_a_queue_full_of_required_messages() {
        let mailbox = SseMailbox::new();
        for value in 0..SSE_CLIENT_BUFFER_CAPACITY {
            mailbox.admit_required([required_registration(value)]).expect("required item fits");
        }

        assert_eq!(mailbox.admit_graph_wakeup(wakeup(7, 1)), Err(SseMailboxAdmissionError::Full));
    }

    #[tokio::test]
    async fn graph_wakeup_removal_is_publication_specific() {
        let mailbox = SseMailbox::new();
        mailbox.admit_graph_wakeup(wakeup(7, 1)).expect("first wakeup fits");
        mailbox.admit_graph_wakeup(wakeup(8, 2)).expect("second publication wakeup fits");

        assert!(!mailbox.remove_graph_wakeup(6));
        assert!(mailbox.remove_graph_wakeup(7));
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(8, 2))));
    }

    #[tokio::test]
    async fn close_rejects_admission_and_drains_before_finishing() {
        let mailbox = SseMailbox::new();
        mailbox.admit_required([required_registration(0)]).expect("registration fits");
        mailbox.close();

        assert_eq!(mailbox.admit_required([required_state(1)]), Err(SseMailboxAdmissionError::Closed));
        assert_eq!(mailbox.admit_graph_wakeup(wakeup(7, 1)), Err(SseMailboxAdmissionError::Closed));
        assert_eq!(mailbox.recv().await, Some(required_registration(0)));
        assert_eq!(mailbox.recv().await, None);
    }

    #[tokio::test]
    async fn close_notifies_an_empty_waiting_receiver() {
        let mailbox = Arc::new(SseMailbox::new());
        let receiver = {
            let mailbox = mailbox.clone();
            tokio::spawn(async move { mailbox.recv().await })
        };
        tokio::task::yield_now().await;

        mailbox.close();

        assert_eq!(receiver.await.expect("receiver task joins"), None);
    }

    #[tokio::test]
    async fn admission_notifies_an_empty_waiting_receiver() {
        let mailbox = Arc::new(SseMailbox::new());
        let receiver = {
            let mailbox = mailbox.clone();
            tokio::spawn(async move { mailbox.recv().await })
        };
        tokio::task::yield_now().await;

        mailbox.admit_required([required_state(1)]).expect("state message fits");

        assert_eq!(receiver.await.expect("receiver task joins"), Some(required_state(1)));
    }
}
