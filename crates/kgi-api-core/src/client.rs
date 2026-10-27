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
    fn admit_required<const N: usize>(&mut self, messages: [SseMessage; N]) -> Result<(), SseMailboxAdmissionError> {
        if self.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }
        if N > SSE_CLIENT_BUFFER_CAPACITY - self.queue.len() {
            return Err(SseMailboxAdmissionError::Full);
        }
        self.queue.extend(messages.into_iter().map(SseMailboxItem::Required));
        Ok(())
    }

    fn admit_graph_wakeup(&mut self, wakeup: PublicationWakeupDto) -> Result<(), SseMailboxAdmissionError> {
        if self.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }
        if !self.coalesce_queued_graph_wakeup(wakeup) {
            if self.queue.len() == SSE_CLIENT_BUFFER_CAPACITY {
                return Err(SseMailboxAdmissionError::Full);
            }
            self.queue.push_back(SseMailboxItem::GraphWakeup(wakeup));
        }
        Ok(())
    }

    fn coalesce_graph_wakeup(&mut self, wakeup: PublicationWakeupDto) -> Result<bool, SseMailboxAdmissionError> {
        if self.closed {
            return Err(SseMailboxAdmissionError::Closed);
        }
        Ok(self.coalesce_queued_graph_wakeup(wakeup))
    }

    fn coalesce_queued_graph_wakeup(&mut self, wakeup: PublicationWakeupDto) -> bool {
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
        state.admit_required(messages)?;
        drop(state);
        if N != 0 {
            self.ready.notify_one();
        }
        Ok(())
    }

    /// Inserts or coalesces the ordinary wakeup for one publication.
    pub(crate) fn admit_graph_wakeup(&self, wakeup: PublicationWakeupDto) -> Result<(), SseMailboxAdmissionError> {
        let mut state = self.lock_state();
        state.admit_graph_wakeup(wakeup)?;
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Updates an ordinary wakeup only while it remains queued.
    pub(crate) fn coalesce_graph_wakeup(&self, wakeup: PublicationWakeupDto) -> Result<bool, SseMailboxAdmissionError> {
        let mut state = self.lock_state();
        let coalesced = state.coalesce_graph_wakeup(wakeup)?;
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

    fn notify_receiver(&self) {
        self.ready.notify_one();
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
    publication_id: u64,
    publication_state: GraphPublicationState,
    head_revision_id: u64,
    clients: HashMap<u64, SseClientRegistration>,
}

impl SseClientRegistryState {
    fn generate_identifier(&self, random: &mut impl RngCore) -> ClientIdentifier {
        loop {
            let candidate = ClientIdentifier::generate(random);
            if !self.clients.contains_key(&candidate.key()) {
                return candidate;
            }
        }
    }

    fn replace_registration_into(
        &mut self,
        old_identifier: ClientIdentifier,
        replacement: &mut Self,
        random: &mut impl RngCore,
        cancellations: &mut Vec<CancellationToken>,
    ) -> Result<(ClientIdentifier, Arc<SseMailbox>), SseClientReplacementError> {
        let Some(registration) = self.clients.remove(&old_identifier.key()) else {
            return Err(SseClientReplacementError::RegistrationMissing);
        };
        let disconnect = registration.disconnect;
        let Some(mailbox) = registration.mailbox.upgrade() else {
            cancellations.push(disconnect);
            return Err(SseClientReplacementError::RegistrationMissing);
        };
        replacement.clients.retain(|_, registration| {
            if registration.mailbox.upgrade().is_none() {
                cancellations.push(registration.disconnect.clone());
                false
            } else {
                true
            }
        });

        let mut mailbox_state = mailbox.lock_state();
        mailbox_state.remove_graph_wakeup(self.publication_id);
        if replacement.clients.len() == MAX_SSE_CLIENTS {
            cancellations.push(disconnect);
            return Err(SseClientReplacementError::Capacity);
        }

        let identifier = replacement.generate_identifier(random);
        let batch = [
            SseMessage::ClientRegistration(identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(
                replacement.publication_id,
                replacement.head_revision_id,
                replacement.publication_state,
            )),
            SseMessage::PublicationWakeup(PublicationWakeupDto::new(replacement.publication_id, replacement.head_revision_id)),
        ];
        if let Err(error) = mailbox_state.admit_required(batch) {
            cancellations.push(disconnect);
            return Err(SseClientReplacementError::Mailbox(error));
        }
        replacement.clients.insert(
            identifier.key(),
            SseClientRegistration {
                schedule: DeltaWakeSchedule { wake_at_revision_id: replacement.head_revision_id, wake_sent: true },
                mailbox: Arc::downgrade(&mailbox),
                disconnect,
            },
        );
        drop(mailbox_state);
        Ok((identifier, mailbox))
    }
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

/// Failure to move one live SSE registration to a replacement publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SseClientReplacementError {
    /// The old publication no longer owns the supplied live registration.
    RegistrationMissing,
    /// The replacement publication has reached its settled live-client bound.
    Capacity,
    /// The complete replacement message batch could not enter the connection mailbox.
    Mailbox(SseMailboxAdmissionError),
}

impl SseClientRegistry {
    /// Creates an empty registry coherent with one publication core.
    pub(crate) fn new(publication_id: u64, publication_state: GraphPublicationState, head_revision_id: u64) -> Self {
        Self {
            state: Mutex::new(SseClientRegistryState {
                publication_id,
                publication_state,
                head_revision_id,
                clients: HashMap::with_capacity(MAX_SSE_CLIENTS),
            }),
        }
    }

    /// Registers one connection and atomically admits its initial message batch.
    pub(crate) fn register(
        &self,
        client_revision_id: u64,
        mailbox: &Arc<SseMailbox>,
        disconnect: CancellationToken,
    ) -> Result<ClientIdentifier, SseClientRegistrationError> {
        self.register_with(client_revision_id, mailbox, disconnect, &mut OsRng)
    }

    /// Removes one connection's exact registration.
    ///
    /// Returns whether that registration still existed and was removed.
    pub(crate) fn remove(&self, identifier: ClientIdentifier) -> bool {
        self.lock_state().clients.remove(&identifier.key()).is_some()
    }

    /// Validates that one publication-local registration remains live.
    pub(crate) fn validate(&self, identifier: ClientIdentifier) -> bool {
        self.complete_delta(identifier, DeltaScheduleAction::ValidateOnly) == DeltaCompletionResult::Accepted
    }

    /// Publishes one required lifecycle observation to every live registration.
    pub(crate) fn publish_state(&self, publication_state: GraphPublicationState, revision: u64) {
        let mut cancellations = Vec::new();
        let mut readiness = Vec::new();
        {
            let mut state = self.lock_state();
            assert_eq!(state.head_revision_id, revision, "publication state observes the latest published Head");
            state.publication_state = publication_state;
            let message = SseMessage::PublicationState(PublicationStateDto::new(state.publication_id, revision, publication_state));
            state.clients.retain(|_, registration| {
                let Some(mailbox) = registration.mailbox.upgrade() else {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                };
                if mailbox.lock_state().admit_required([message.clone()]).is_err() {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                }
                readiness.push(mailbox);
                true
            });
        }
        Self::notify_mailboxes(readiness);
        Self::cancel_connections(cancellations);
    }

    /// Records a published Head revision and schedules every eligible client.
    pub(crate) fn advance_head(&self, revision: u64) {
        let mut cancellations = Vec::new();
        let mut readiness = Vec::new();
        {
            let mut state = self.lock_state();
            assert!(revision >= state.head_revision_id, "publication Head revision does not regress");
            state.head_revision_id = revision;
            let publication_id = state.publication_id;
            state.clients.retain(|_, registration| {
                let Some(mailbox) = registration.mailbox.upgrade() else {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                };

                let wakeup = PublicationWakeupDto::new(publication_id, revision);
                let mut mailbox_state = mailbox.lock_state();
                let queued = if registration.schedule.wake_sent {
                    mailbox_state.coalesce_graph_wakeup(wakeup)
                } else if revision >= registration.schedule.wake_at_revision_id {
                    mailbox_state.admit_graph_wakeup(wakeup).map(|()| true)
                } else {
                    Ok(false)
                };
                let Ok(queued) = queued else {
                    cancellations.push(registration.disconnect.clone());
                    return false;
                };
                if !registration.schedule.wake_sent && revision >= registration.schedule.wake_at_revision_id {
                    registration.schedule.wake_sent = true;
                }
                drop(mailbox_state);
                if queued {
                    readiness.push(mailbox);
                }
                true
            });
        }
        Self::notify_mailboxes(readiness);
        Self::cancel_connections(cancellations);
    }

    /// Applies one completed coordinated-delta schedule action atomically.
    pub(crate) fn complete_delta(&self, identifier: ClientIdentifier, action: DeltaScheduleAction) -> DeltaCompletionResult {
        let (result, readiness, cancellation) = {
            let mut state = self.lock_state();
            let Some(registration) = state.clients.get(&identifier.key()) else {
                return DeltaCompletionResult::RegistrationMissing;
            };
            let disconnect = registration.disconnect.clone();
            match registration.mailbox.upgrade() {
                None => {
                    state.clients.remove(&identifier.key());
                    (DeltaCompletionResult::RegistrationMissing, None, Some(disconnect))
                }
                Some(mailbox) => {
                    let mut mailbox_state = mailbox.lock_state();
                    if mailbox_state.closed {
                        drop(mailbox_state);
                        state.clients.remove(&identifier.key());
                        (DeltaCompletionResult::RegistrationMissing, None, Some(disconnect))
                    } else {
                        match action {
                            DeltaScheduleAction::ValidateOnly => (DeltaCompletionResult::Accepted, None, None),
                            DeltaScheduleAction::Replace { wake_at_revision_id, wake_sent } => {
                                mailbox_state.remove_graph_wakeup(state.publication_id);
                                let head_revision_id = state.head_revision_id;
                                let mut replacement = DeltaWakeSchedule { wake_at_revision_id, wake_sent };
                                if !replacement.wake_sent && head_revision_id >= replacement.wake_at_revision_id {
                                    let wakeup = PublicationWakeupDto::new(state.publication_id, head_revision_id);
                                    if mailbox_state.admit_graph_wakeup(wakeup).is_err() {
                                        drop(mailbox_state);
                                        state.clients.remove(&identifier.key());
                                        (DeltaCompletionResult::RegistrationMissing, None, Some(disconnect))
                                    } else {
                                        replacement.wake_sent = true;
                                        state
                                            .clients
                                            .get_mut(&identifier.key())
                                            .expect("the completed registration remains installed")
                                            .schedule = replacement;
                                        drop(mailbox_state);
                                        (DeltaCompletionResult::Accepted, Some(mailbox), None)
                                    }
                                } else {
                                    state
                                        .clients
                                        .get_mut(&identifier.key())
                                        .expect("the completed registration remains installed")
                                        .schedule = replacement;
                                    (DeltaCompletionResult::Accepted, None, None)
                                }
                            }
                        }
                    }
                }
            }
        };
        if let Some(mailbox) = readiness {
            mailbox.notify_receiver();
        }
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        result
    }

    /// Replaces one publication-local registration while preserving its SSE transport.
    pub(crate) fn replace_registration_into(
        &self,
        old_identifier: ClientIdentifier,
        replacement_registry: &Self,
    ) -> Result<ClientIdentifier, SseClientReplacementError> {
        self.replace_registration_into_with(old_identifier, replacement_registry, &mut OsRng)
    }

    fn register_with(
        &self,
        client_revision_id: u64,
        mailbox: &Arc<SseMailbox>,
        disconnect: CancellationToken,
        random: &mut impl RngCore,
    ) -> Result<ClientIdentifier, SseClientRegistrationError> {
        let mut cancellations = Vec::new();
        let mut readiness = None;
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
                let identifier = state.generate_identifier(random);
                let batch = [
                    SseMessage::ClientRegistration(identifier.registration()),
                    SseMessage::PublicationState(PublicationStateDto::new(
                        state.publication_id,
                        state.head_revision_id,
                        state.publication_state,
                    )),
                    SseMessage::PublicationWakeup(PublicationWakeupDto::new(state.publication_id, state.head_revision_id)),
                ];
                let mut mailbox_state = mailbox.lock_state();
                match mailbox_state.admit_required(batch) {
                    Ok(()) => {
                        state.clients.insert(
                            identifier.key(),
                            SseClientRegistration {
                                schedule: DeltaWakeSchedule { wake_at_revision_id: client_revision_id, wake_sent: true },
                                mailbox: Arc::downgrade(mailbox),
                                disconnect,
                            },
                        );
                        readiness = Some(mailbox.clone());
                        Ok(identifier)
                    }
                    Err(error) => {
                        cancellations.push(disconnect);
                        Err(SseClientRegistrationError::Mailbox(error))
                    }
                }
            }
        };
        if let Some(mailbox) = readiness {
            mailbox.notify_receiver();
        }
        Self::cancel_connections(cancellations);
        result
    }

    fn replace_registration_into_with(
        &self,
        old_identifier: ClientIdentifier,
        replacement_registry: &Self,
        random: &mut impl RngCore,
    ) -> Result<ClientIdentifier, SseClientReplacementError> {
        let old_address = std::ptr::from_ref(self).addr();
        let replacement_address = std::ptr::from_ref(replacement_registry).addr();
        assert_ne!(old_address, replacement_address, "publication replacement requires distinct registries");

        let mut cancellations = Vec::new();
        let old_first = old_address < replacement_address;
        let (mut first_state, mut second_state) = if old_first {
            (self.lock_state(), replacement_registry.lock_state())
        } else {
            (replacement_registry.lock_state(), self.lock_state())
        };
        let (old_state, replacement_state) =
            if old_first { (&mut first_state, &mut second_state) } else { (&mut second_state, &mut first_state) };
        assert_ne!(
            old_state.publication_id, replacement_state.publication_id,
            "publication replacement requires distinct publication identities"
        );
        let result = old_state.replace_registration_into(old_identifier, replacement_state, random, &mut cancellations);
        drop(first_state);
        drop(second_state);
        let result = result.map(|(identifier, mailbox)| {
            mailbox.notify_receiver();
            identifier
        });
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

    fn notify_mailboxes(mailboxes: Vec<Arc<SseMailbox>>) {
        for mailbox in mailboxes {
            mailbox.notify_receiver();
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
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use rand::rngs::mock::StepRng;

    use crate::{
        codec::sse::{ClientRegistrationDto, PublicationStateDto, PublicationWakeupDto},
        publication::GraphPublicationState,
    };

    use super::{
        ClientIdentifier, DeltaCompletionResult, DeltaScheduleAction, MAX_SSE_CLIENTS, SSE_CLIENT_BUFFER_CAPACITY,
        SseClientRegistrationError, SseClientRegistry, SseClientReplacementError, SseMailbox, SseMailboxAdmissionError, SseMessage,
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
        let registry = SseClientRegistry::new(7, GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);

        let identifier = registry.register_with(3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");

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

    #[tokio::test]
    async fn registration_linearized_before_lifecycle_change_receives_both_states_in_order() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut StepRng::new(41, 1))
            .expect("registration succeeds");

        registry.publish_state(GraphPublicationState::Live, 9);

        for expected in [
            SseMessage::ClientRegistration(identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Synchronizing)),
            SseMessage::PublicationWakeup(wakeup(7, 9)),
            SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Live)),
        ] {
            assert_eq!(mailbox.recv().await, Some(expected));
        }
    }

    #[tokio::test]
    async fn lifecycle_change_linearized_before_registration_is_observed_in_the_initial_batch() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Synchronizing, 9);
        registry.publish_state(GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut StepRng::new(41, 1))
            .expect("registration succeeds");

        for expected in [
            SseMessage::ClientRegistration(identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Live)),
            SseMessage::PublicationWakeup(wakeup(7, 9)),
        ] {
            assert_eq!(mailbox.recv().await, Some(expected));
        }
        assert!(mailbox.lock_state().queue.is_empty());
    }

    #[test]
    fn registration_regenerates_a_colliding_identifier() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let first_mailbox = Arc::new(SseMailbox::new());
        let second_mailbox = Arc::new(SseMailbox::new());
        let mut first_random = StepRng::new(41, 1);
        let mut colliding_random = StepRng::new(41, 1);

        let first = registry
            .register_with(3, &first_mailbox, tokio_util::sync::CancellationToken::new(), &mut first_random)
            .expect("first registration succeeds");
        let second = registry
            .register_with(3, &second_mailbox, tokio_util::sync::CancellationToken::new(), &mut colliding_random)
            .expect("collision is regenerated");

        assert_eq!(first.key(), 41);
        assert_eq!(second.key(), 42);
    }

    #[test]
    fn registration_accepts_the_final_client_and_rejects_the_next() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let mut random = StepRng::new(0, 1);
        let mut mailboxes = Vec::with_capacity(MAX_SSE_CLIENTS);

        for _ in 0..MAX_SSE_CLIENTS {
            let mailbox = Arc::new(SseMailbox::new());
            registry
                .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
                .expect("client within the bound is admitted");
            mailboxes.push(mailbox);
        }

        let saturated = Arc::new(SseMailbox::new());
        assert_eq!(
            registry.register_with(3, &saturated, tokio_util::sync::CancellationToken::new(), &mut random),
            Err(SseClientRegistrationError::Capacity)
        );
        assert_eq!(registry.lock_state().clients.len(), MAX_SSE_CLIENTS);
    }

    #[test]
    fn failed_initial_batch_installs_no_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        for value in 0..6 {
            mailbox.admit_required([required_registration(value)]).expect("fixture item fits");
        }
        let mut random = StepRng::new(41, 1);
        let disconnect = tokio_util::sync::CancellationToken::new();

        assert_eq!(
            registry.register_with(3, &mailbox, disconnect.clone(), &mut random),
            Err(SseClientRegistrationError::Mailbox(SseMailboxAdmissionError::Full))
        );
        assert!(registry.lock_state().clients.is_empty());
        assert_eq!(mailbox.lock_state().queue.len(), 6);
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn stale_weak_registration_is_removed_and_cancelled_on_the_next_operation() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        registry.register_with(3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        drop(mailbox);

        registry.publish_state(GraphPublicationState::Live, 9);

        assert!(registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[tokio::test]
    async fn lifecycle_message_failure_removes_and_cancels_the_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Synchronizing, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        registry.register_with(3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        for value in 0..5 {
            mailbox.admit_required([required_registration(value)]).expect("fixture item fits");
        }

        registry.publish_state(GraphPublicationState::Live, 9);

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
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
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

        registry.advance_head(11);
        assert!(mailbox.lock_state().queue.is_empty());
        registry.advance_head(12);
        registry.publish_state(GraphPublicationState::Live, 12);
        registry.advance_head(13);

        assert_eq!(mailbox.recv().await, Some(required_state(12)));
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 13))));
        assert!(registry.lock_state().clients[&identifier.key()].schedule.wake_sent);

        registry.advance_head(14);
        assert!(mailbox.lock_state().queue.is_empty());
    }

    #[test]
    fn failed_wakeup_admission_removes_and_cancels_the_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        let identifier = registry.register_with(3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
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

        registry.advance_head(10);

        assert!(registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn disconnect_removes_only_the_exact_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let first_mailbox = Arc::new(SseMailbox::new());
        let second_mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let first = registry
            .register_with(3, &first_mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("first registration succeeds");
        let second = registry
            .register_with(3, &second_mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("second registration succeeds");

        assert!(registry.remove(first));
        assert!(!registry.remove(first));
        let state = registry.lock_state();
        assert!(!state.clients.contains_key(&first.key()));
        assert!(state.clients.contains_key(&second.key()));
    }

    #[tokio::test]
    async fn delta_rearm_removes_the_previous_wakeup_and_waits_for_its_boundary() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        mailbox.lock_state().queue.clear();
        mailbox.admit_graph_wakeup(wakeup(7, 9)).expect("previous wakeup fits");

        assert_eq!(
            registry.complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 12, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );
        assert!(mailbox.lock_state().queue.is_empty());
        assert_eq!(
            registry.lock_state().clients[&identifier.key()].schedule,
            super::DeltaWakeSchedule { wake_at_revision_id: 12, wake_sent: false }
        );

        registry.advance_head(11);
        assert!(mailbox.lock_state().queue.is_empty());
        registry.advance_head(12);
        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 12))));
        assert!(registry.lock_state().clients[&identifier.key()].schedule.wake_sent);
    }

    #[tokio::test]
    async fn delta_rearm_immediately_wakes_when_head_already_reached_the_boundary() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 12);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        mailbox.lock_state().queue.clear();
        mailbox.admit_graph_wakeup(wakeup(7, 10)).expect("previous wakeup fits");

        assert_eq!(
            registry.complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 12, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );

        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, 12))));
        assert_eq!(
            registry.lock_state().clients[&identifier.key()].schedule,
            super::DeltaWakeSchedule { wake_at_revision_id: 12, wake_sent: true }
        );
    }

    #[test]
    fn immediate_http_continuation_marks_the_replacement_schedule_sent() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 12);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        mailbox.lock_state().queue.clear();
        mailbox.admit_graph_wakeup(wakeup(7, 10)).expect("previous wakeup fits");

        assert_eq!(
            registry.complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 11, wake_sent: true }),
            DeltaCompletionResult::Accepted
        );

        assert!(mailbox.lock_state().queue.is_empty());
        assert_eq!(
            registry.lock_state().clients[&identifier.key()].schedule,
            super::DeltaWakeSchedule { wake_at_revision_id: 11, wake_sent: true }
        );
    }

    #[test]
    fn final_stale_completion_only_validates_the_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Stale, 12);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        mailbox.lock_state().queue.clear();
        mailbox.admit_graph_wakeup(wakeup(7, 12)).expect("existing wakeup fits");
        let schedule = registry.lock_state().clients[&identifier.key()].schedule;

        assert_eq!(registry.complete_delta(identifier, DeltaScheduleAction::ValidateOnly), DeltaCompletionResult::Accepted);
        assert_eq!(registry.lock_state().clients[&identifier.key()].schedule, schedule);
        assert_eq!(mailbox.lock_state().queue.len(), 1);
        assert_eq!(
            registry.complete_delta(ClientIdentifier(99), DeltaScheduleAction::ValidateOnly),
            DeltaCompletionResult::RegistrationMissing
        );
        assert_eq!(registry.lock_state().clients.len(), 1);
    }

    #[test]
    fn completion_removes_and_cancels_a_closed_mailbox_registration() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, 12);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut random = StepRng::new(41, 1);
        let identifier = registry.register_with(3, &mailbox, disconnect.clone(), &mut random).expect("registration succeeds");
        mailbox.close();

        assert_eq!(
            registry.complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: 20, wake_sent: false }),
            DeltaCompletionResult::RegistrationMissing
        );
        assert!(registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[tokio::test]
    async fn maximum_revision_boundary_saturates_without_missing_its_wakeup() {
        let registry = SseClientRegistry::new(7, GraphPublicationState::Live, u64::MAX - 1);
        let mailbox = Arc::new(SseMailbox::new());
        let mut random = StepRng::new(41, 1);
        let identifier = registry
            .register_with(3, &mailbox, tokio_util::sync::CancellationToken::new(), &mut random)
            .expect("registration succeeds");
        mailbox.lock_state().queue.clear();

        assert_eq!(
            registry.complete_delta(identifier, DeltaScheduleAction::Replace { wake_at_revision_id: u64::MAX, wake_sent: false }),
            DeltaCompletionResult::Accepted
        );
        registry.advance_head(u64::MAX);

        assert_eq!(mailbox.recv().await, Some(SseMessage::PublicationWakeup(wakeup(7, u64::MAX))));
    }

    #[tokio::test]
    async fn publication_replacement_preserves_required_messages_and_installs_one_new_batch() {
        let old_registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let replacement_registry = SseClientRegistry::new(8, GraphPublicationState::Synchronizing, 20);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut old_random = StepRng::new(41, 1);
        let old_identifier =
            old_registry.register_with(3, &mailbox, disconnect.clone(), &mut old_random).expect("old registration succeeds");
        mailbox.admit_required([required_state(9)]).expect("required old message fits");
        mailbox.admit_graph_wakeup(wakeup(7, 10)).expect("ordinary old wakeup fits");
        let mut replacement_random = StepRng::new(51, 1);

        let replacement_identifier = old_registry
            .replace_registration_into_with(old_identifier, &replacement_registry, &mut replacement_random)
            .expect("replacement succeeds");

        assert_eq!(replacement_identifier.key(), 51);
        assert!(old_registry.lock_state().clients.is_empty());
        {
            let replacement_state = replacement_registry.lock_state();
            assert_eq!(
                replacement_state.clients[&replacement_identifier.key()].schedule,
                super::DeltaWakeSchedule { wake_at_revision_id: 20, wake_sent: true }
            );
        }
        assert!(!disconnect.is_cancelled());
        for expected in [
            SseMessage::ClientRegistration(old_identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(7, 9, GraphPublicationState::Live)),
            SseMessage::PublicationWakeup(wakeup(7, 9)),
            required_state(9),
            SseMessage::ClientRegistration(replacement_identifier.registration()),
            SseMessage::PublicationState(PublicationStateDto::new(8, 20, GraphPublicationState::Synchronizing)),
            SseMessage::PublicationWakeup(wakeup(8, 20)),
        ] {
            assert_eq!(mailbox.recv().await, Some(expected));
        }
        assert!(mailbox.lock_state().queue.is_empty());
    }

    #[test]
    fn publication_replacement_regenerates_a_collision_in_the_replacement_registry() {
        let old_registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let replacement_registry = SseClientRegistry::new(8, GraphPublicationState::Live, 20);
        let mailbox = Arc::new(SseMailbox::new());
        let occupied_mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut old_random = StepRng::new(1, 1);
        let old_identifier =
            old_registry.register_with(3, &mailbox, disconnect.clone(), &mut old_random).expect("old registration succeeds");
        let mut occupied_random = StepRng::new(41, 1);
        replacement_registry
            .register_with(20, &occupied_mailbox, tokio_util::sync::CancellationToken::new(), &mut occupied_random)
            .expect("collision fixture succeeds");
        let mut replacement_random = StepRng::new(41, 1);

        let replacement_identifier = old_registry
            .replace_registration_into_with(old_identifier, &replacement_registry, &mut replacement_random)
            .expect("collision is regenerated");

        assert_eq!(replacement_identifier.key(), 42);
    }

    #[test]
    fn failed_replacement_batch_leaves_no_partial_registration_and_cancels_transport() {
        let old_registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let replacement_registry = SseClientRegistry::new(8, GraphPublicationState::Synchronizing, 20);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let mut old_random = StepRng::new(41, 1);
        let old_identifier =
            old_registry.register_with(3, &mailbox, disconnect.clone(), &mut old_random).expect("old registration succeeds");
        mailbox.lock_state().queue.clear();
        for value in 0..6 {
            mailbox.admit_required([required_registration(value)]).expect("fixture required message fits");
        }
        mailbox.admit_graph_wakeup(wakeup(7, 10)).expect("old ordinary wakeup fits");
        let mut replacement_random = StepRng::new(51, 1);

        assert_eq!(
            old_registry.replace_registration_into_with(old_identifier, &replacement_registry, &mut replacement_random),
            Err(SseClientReplacementError::Mailbox(SseMailboxAdmissionError::Full))
        );

        assert!(old_registry.lock_state().clients.is_empty());
        assert!(replacement_registry.lock_state().clients.is_empty());
        assert_eq!(mailbox.lock_state().queue.len(), 6);
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn missing_old_registration_cannot_be_resurrected_in_the_replacement_registry() {
        let old_registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let replacement_registry = SseClientRegistry::new(8, GraphPublicationState::Synchronizing, 20);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let old_identifier =
            old_registry.register_with(3, &mailbox, disconnect.clone(), &mut StepRng::new(41, 1)).expect("old registration succeeds");
        assert!(old_registry.remove(old_identifier));

        assert_eq!(
            old_registry.replace_registration_into_with(old_identifier, &replacement_registry, &mut StepRng::new(51, 1)),
            Err(SseClientReplacementError::RegistrationMissing)
        );
        assert!(replacement_registry.lock_state().clients.is_empty());
        assert!(!disconnect.is_cancelled());
    }

    #[test]
    fn dead_old_mailbox_cannot_be_replaced_and_its_connection_is_cancelled() {
        let old_registry = SseClientRegistry::new(7, GraphPublicationState::Live, 9);
        let replacement_registry = SseClientRegistry::new(8, GraphPublicationState::Synchronizing, 20);
        let mailbox = Arc::new(SseMailbox::new());
        let disconnect = tokio_util::sync::CancellationToken::new();
        let old_identifier =
            old_registry.register_with(3, &mailbox, disconnect.clone(), &mut StepRng::new(41, 1)).expect("old registration succeeds");
        drop(mailbox);

        assert_eq!(
            old_registry.replace_registration_into_with(old_identifier, &replacement_registry, &mut StepRng::new(51, 1)),
            Err(SseClientReplacementError::RegistrationMissing)
        );
        assert!(old_registry.lock_state().clients.is_empty());
        assert!(replacement_registry.lock_state().clients.is_empty());
        assert!(disconnect.is_cancelled());
    }

    #[test]
    fn opposite_replacements_share_one_stable_registry_lock_order() {
        let first_registry = Arc::new(SseClientRegistry::new(7, GraphPublicationState::Live, 9));
        let second_registry = Arc::new(SseClientRegistry::new(8, GraphPublicationState::Synchronizing, 20));
        let first_mailbox = Arc::new(SseMailbox::new());
        let second_mailbox = Arc::new(SseMailbox::new());
        let first_disconnect = tokio_util::sync::CancellationToken::new();
        let second_disconnect = tokio_util::sync::CancellationToken::new();
        let first_identifier = first_registry
            .register_with(3, &first_mailbox, first_disconnect.clone(), &mut StepRng::new(1, 1))
            .expect("first registration succeeds");
        let second_identifier = second_registry
            .register_with(4, &second_mailbox, second_disconnect.clone(), &mut StepRng::new(2, 1))
            .expect("second registration succeeds");
        let start = Arc::new(Barrier::new(3));

        let first_replacement = {
            let first_registry = first_registry.clone();
            let second_registry = second_registry.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                first_registry.replace_registration_into_with(first_identifier, &second_registry, &mut StepRng::new(101, 1))
            })
        };
        let second_replacement = {
            let first_registry = first_registry.clone();
            let second_registry = second_registry.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                second_registry.replace_registration_into_with(second_identifier, &first_registry, &mut StepRng::new(201, 1))
            })
        };

        start.wait();
        assert!(first_replacement.join().expect("first replacement thread joins").is_ok());
        assert!(second_replacement.join().expect("second replacement thread joins").is_ok());
        assert_eq!(first_registry.lock_state().clients.len(), 1);
        assert_eq!(second_registry.lock_state().clients.len(), 1);
        assert!(!first_disconnect.is_cancelled());
        assert!(!second_disconnect.is_cancelled());
    }
}
