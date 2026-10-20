#![allow(dead_code, reason = "the client scheduling foundation is completed by later increment points")]

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    codec::sse::{ClientRegistrationDto, PublicationStateDto, PublicationWakeupDto},
    publication::GraphPublicationState,
};

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

impl SseMailboxState {
    fn coalesce_graph_wakeup(&mut self, wakeup: PublicationWakeupDto) -> bool {
        let publication_id = wakeup.publication_id();
        let Some(index) = self
            .queue
            .iter()
            .position(|item| matches!(item, SseMailboxItem::GraphWakeup(queued) if queued.publication_id() == publication_id))
        else {
            return false;
        };
        self.queue.remove(index);
        self.queue.push_back(SseMailboxItem::GraphWakeup(wakeup));
        true
    }

    fn remove_graph_wakeup(&mut self, publication_id: u64) -> bool {
        let Some(index) = self
            .queue
            .iter()
            .position(|item| matches!(item, SseMailboxItem::GraphWakeup(queued) if queued.publication_id() == publication_id))
        else {
            return false;
        };
        self.queue.remove(index);
        true
    }
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

        if !state.coalesce_graph_wakeup(wakeup) {
            if state.queue.len() == SSE_CLIENT_BUFFER_CAPACITY {
                return Err(SseMailboxAdmissionError::Full);
            }
            state.queue.push_back(SseMailboxItem::GraphWakeup(wakeup));
        }

        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Updates an ordinary wakeup only while it remains queued.
    pub(crate) fn coalesce_graph_wakeup(&self, wakeup: PublicationWakeupDto) -> Result<bool, SseMailboxAdmissionError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }
        let coalesced = state.coalesce_graph_wakeup(wakeup);
        drop(state);
        if coalesced {
            self.ready.notify_one();
        }
        Ok(coalesced)
    }

    /// Removes a still-queued ordinary wakeup for one publication.
    pub(crate) fn remove_graph_wakeup(&self, publication_id: u64) -> bool {
        self.lock_state().remove_graph_wakeup(publication_id)
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

/// Publication-local delta-wakeup schedule for one SSE client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeltaWakeSchedule {
    wake_at_revision_id: u64,
    wake_sent: bool,
}

/// Publication-local state retained for one live SSE client.
struct SseClientRegistration {
    schedule: DeltaWakeSchedule,
    mailbox: Weak<SseMailbox>,
    disconnect: CancellationToken,
}

struct SseClientRegistryState {
    publication_state: GraphPublicationState,
    head_revision_id: u64,
    clients: HashMap<u64, SseClientRegistration>,
}

/// Publication-local registry of live SSE client wake schedules.
pub(crate) struct SseClientRegistry {
    state: Mutex<SseClientRegistryState>,
}

/// Failure to establish one publication-local SSE registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SseClientRegistrationError {
    /// The publication has reached its settled live-client bound.
    Capacity,
    /// The complete initial message batch could not enter the mailbox.
    Mailbox(SseMailboxAdmissionError),
}

impl SseClientRegistry {
    /// Creates an empty registry coherent with one publication core.
    pub(crate) fn new(publication_state: GraphPublicationState, head_revision_id: u64) -> Self {
        Self {
            state: Mutex::new(SseClientRegistryState {
                publication_state,
                head_revision_id,
                clients: HashMap::with_capacity(MAX_SSE_CLIENTS),
            }),
        }
    }

    /// Registers one connection and atomically admits its initial message batch.
    pub(crate) fn register(
        &self,
        publication_id: u64,
        client_revision_id: u64,
        mailbox: &Arc<SseMailbox>,
        disconnect: CancellationToken,
    ) -> Result<ClientIdentifier, SseClientRegistrationError> {
        self.register_with(publication_id, client_revision_id, mailbox, disconnect, &mut OsRng)
    }

    /// Removes one connection's exact registration.
    ///
    /// Returns whether that registration still existed and was removed.
    pub(crate) fn remove(&self, identifier: ClientIdentifier) -> bool {
        self.lock_state().clients.remove(&identifier.key()).is_some()
    }

    /// Publishes one required lifecycle observation to every live registration.
    pub(crate) fn publish_state(&self, publication_id: u64, publication_state: GraphPublicationState, revision: u64) {
        let mut cancellations = Vec::new();
        {
            let mut state = self.lock_state();
            assert_eq!(state.head_revision_id, revision, "publication state observes the latest published Head");
            state.publication_state = publication_state;
            let message = SseMessage::PublicationState(PublicationStateDto::new(publication_id, revision, publication_state));
            state.clients.retain(|_, registration| {
                let Some(mailbox) = registration.mailbox.upgrade() else {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                };
                if mailbox.admit_required([message.clone()]).is_err() {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                }
                true
            });
        }
        Self::cancel_connections(cancellations);
    }

    /// Records a published Head revision and schedules every eligible client.
    pub(crate) fn advance_head(&self, publication_id: u64, revision: u64) {
        let mut cancellations = Vec::new();
        {
            let mut state = self.lock_state();
            assert!(revision >= state.head_revision_id, "publication Head revision does not regress");
            state.head_revision_id = revision;
            state.clients.retain(|_, registration| {
                let Some(mailbox) = registration.mailbox.upgrade() else {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                };

                let wakeup = PublicationWakeupDto::new(publication_id, revision);
                let admitted = if registration.schedule.wake_sent {
                    mailbox.coalesce_graph_wakeup(wakeup).map(|_| ())
                } else if revision >= registration.schedule.wake_at_revision_id {
                    mailbox.admit_graph_wakeup(wakeup)
                } else {
                    Ok(())
                };
                if admitted.is_err() {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                }
                if !registration.schedule.wake_sent && revision >= registration.schedule.wake_at_revision_id {
                    registration.schedule.wake_sent = true;
                }
                true
            });
        }
        Self::cancel_connections(cancellations);
    }

    fn register_with(
        &self,
        publication_id: u64,
        client_revision_id: u64,
        mailbox: &Arc<SseMailbox>,
        disconnect: CancellationToken,
        random: &mut impl RngCore,
    ) -> Result<ClientIdentifier, SseClientRegistrationError> {
        let mut cancellations = Vec::new();
        let result = {
            let mut state = self.lock_state();
            state.clients.retain(|_, registration| {
                if registration.mailbox.upgrade().is_none() {
                    cancellations.push(registration.disconnect.clone());
                    false
                } else {
                    true
                }
            });

            if state.clients.len() == MAX_SSE_CLIENTS {
                Err(SseClientRegistrationError::Capacity)
            } else {
                let identifier = loop {
                    let candidate = ClientIdentifier::generate(random);
                    if !state.clients.contains_key(&candidate.key()) {
                        break candidate;
                    }
                };
                let batch = [
                    SseMessage::ClientRegistration(identifier.registration()),
                    SseMessage::PublicationState(PublicationStateDto::new(
                        publication_id,
                        state.head_revision_id,
                        state.publication_state,
                    )),
                    SseMessage::PublicationWakeup(PublicationWakeupDto::new(publication_id, state.head_revision_id)),
                ];
                match mailbox.admit_required(batch) {
                    Ok(()) => {
                        state.clients.insert(
                            identifier.key(),
                            SseClientRegistration {
                                schedule: DeltaWakeSchedule { wake_at_revision_id: client_revision_id, wake_sent: true },
                                mailbox: Arc::downgrade(mailbox),
                                disconnect,
                            },
                        );
                        Ok(identifier)
                    }
                    Err(error) => {
                        cancellations.push(disconnect);
                        Err(SseClientRegistrationError::Mailbox(error))
                    }
                }
            }
        };
        Self::cancel_connections(cancellations);
        result
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SseClientRegistryState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cancel_connections(cancellations: Vec<CancellationToken>) {
        for cancellation in cancellations {
            cancellation.cancel();
        }
    }
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

    use super::{
        ClientIdentifier, MAX_SSE_CLIENTS, SSE_CLIENT_BUFFER_CAPACITY, SseClientRegistrationError, SseClientRegistry, SseMailbox,
        SseMailboxAdmissionError, SseMessage,
    };

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

    #[tokio::test]
    async fn registration_installs_one_coherent_initial_batch_and_schedule() {
        let registry = SseClientRegistry::new(GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);

        let identifier = registry.register_with(7, 3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");

        assert_eq!(identifier.key(), 41);
        assert_eq!(mailbox.recv().await, Some(SseMessage::ClientRegistration(identifier.registration())));
        assert_eq!(
            mailbox.recv().await,
            Some(SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Synchronizing)))
        );
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 9))));
        let state = registry.lock_state();
        let registration = state.clients.get(&identifier.key()).expect("registration was installed");
        assert_eq!(registration.schedule.wake_at_revision_id, 3);
        assert!(registration.schedule.wake_sent);
        assert!(registration.mailbox.upgrade().is_some());
        assert!(!disconnect.is_cancelled());
    }

    #[test]
    fn registration_regenerates_a_colliding_identifier() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let first_mailbox = Arc::new(SseMailbox::new());
        let second_mailbox = Arc::new(SseMailbox::new());
        let mut first_random = StepRng::new(41, 1);
        let mut colliding_random = StepRng::new(41, 1);

        let first = registry
            .register_with(7, 3, &first_mailbox, tokio_util::sync::CancellationToken::new(), &mut first_random)
            .expect("first registration succeeds");
        let second = registry
            .register_with(7, 3, &second_mailbox, tokio_util::sync::CancellationToken::new(), &mut colliding_random)
            .expect("collision is regenerated");

        assert_eq!(first.key(), 41);
        assert_eq!(second.key(), 42);
    }

    #[test]
    fn registration_accepts_the_final_client_and_rejects_the_next() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let mut random = StepRng::new(0, 1);
        let mut mailboxes = Vec::with_capacity(MAX_SSE_CLIENTS);

        for _ in 0..MAX_SSE_CLIENTS {
            let mailbox = Arc::new(SseMailbox::new());
            registry
                .register_with(7, 3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
                .expect("client within the bound is admitted");
            mailboxes.push(mailbox);
        }

        let saturated = Arc::new(SseMailbox::new());
        assert_eq!(
            registry.register_with(7, 3, &saturated, tokio_util::sync::CancellationToken::new(), &mut random),
            Err(SseClientRegistrationError::Capacity)
        );
        assert_eq!(registry.lock_state().clients.len(), MAX_SSE_CLIENTS);
    }

    #[test]
    fn failed_initial_batch_installs_no_registration() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        for value in 0..6 {
            mailbox.admit_required([required_registration(value)]).expect("fixture item fits");
        }
        let mut random = StepRng::new(41, 1);
        let disconnect = tokio_util::sync::CancellationToken::new();

        assert_eq!(
            registry.register_with(7, 3, &mailbox, disconnect.clone(), &mut random),
            Err(SseClientRegistrationError::Mailbox(SseMailboxAdmissionError::Full))
        );
        assert!(registry.lock_state().clients.is_empty());
        assert_eq!(mailbox.lock_state().queue.len(), 6);
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn stale_weak_registration_is_removed_and_cancelled_on_the_next_operation() {
        let registry = SseClientRegistry::new(GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        registry.register_with(7, 3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        drop(mailbox);

        registry.publish_state(7, GraphPublicationState::Live, 9);

        assert!(registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[tokio::test]
    async fn lifecycle_message_failure_removes_and_cancels_the_registration() {
        let registry = SseClientRegistry::new(GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        registry.register_with(7, 3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        for value in 0..5 {
            mailbox.admit_required([required_registration(value)]).expect("fixture item fits");
        }

        registry.publish_state(7, GraphPublicationState::Live, 9);

        {
            let state = registry.lock_state();
            assert_eq!(state.publication_state, GraphPublicationState::Live);
            assert!(state.clients.is_empty());
        }
        assert!(disconnect.is_cancelled());
        for expected in [
            SseMessage::ClientRegistration(ClientIdentifier(41).registration()),
            SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Synchronizing)),
            SseMessage::PublicationWakeup(wakeup(7, 9)),
        ] {
            assert_eq!(mailbox.recv().await, Some(expected));
        }
    }

    #[tokio::test]
    async fn head_advancement_schedules_once_then_coalesces_only_while_queued() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(7, 3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        for _ in 0..3 {
            mailbox.recv().await.expect("initial batch item");
        }
        {
            let mut state = registry.lock_state();
            let registration = state.clients.get_mut(&identifier.key()).expect("registration exists");
            registration.schedule.wake_at_revision_id = 12;
            registration.schedule.wake_sent = false;
        }

        registry.advance_head(7, 11);
        assert!(mailbox.lock_state().queue.is_empty());
        registry.advance_head(7, 12);
        registry.publish_state(7, GraphPublicationState::Live, 12);
        registry.advance_head(7, 13);

        assert_eq!(mailbox.recv().await, Some(required_state(12)));
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 13))));
        assert!(registry.lock_state().clients[&identifier.key()].schedule.wake_sent);

        registry.advance_head(7, 14);
        assert!(mailbox.lock_state().queue.is_empty());
    }

    #[test]
    fn failed_wakeup_admission_removes_and_cancels_the_registration() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        let identifier = registry.register_with(7, 3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        {
            let mut mailbox_state = mailbox.lock_state();
            mailbox_state.queue.clear();
        }
        for value in 0..SSE_CLIENT_BUFFER_CAPACITY {
            mailbox.admit_required([required_registration(value)]).expect("fixture item fits");
        }
        {
            let mut state = registry.lock_state();
            let registration = state.clients.get_mut(&identifier.key()).expect("registration exists");
            registration.schedule.wake_at_revision_id = 10;
            registration.schedule.wake_sent = false;
        }

        registry.advance_head(7, 10);

        assert!(registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn disconnect_removes_only_the_exact_registration() {
        let registry = SseClientRegistry::new(GraphPublicationState::Live, 9);
        let first_mailbox = Arc::new(SseMailbox::new());
        let second_mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let first = registry
            .register_with(7, 3, &first_mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("first registration succeeds");
        let second = registry
            .register_with(7, 3, &second_mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("second registration succeeds");

        assert!(registry.remove(first));
        assert!(!registry.remove(first));
        let state = registry.lock_state();
        assert!(!state.clients.contains_key(&first.key()));
        assert!(state.clients.contains_key(&second.key()));
    }
}
