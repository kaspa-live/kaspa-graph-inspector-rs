use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use thiserror::Error;

use crate::delta::GraphDelta;

/// Failure to append a delta to retained graph history.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphHistoryAppendError {
    /// The delta does not begin at the history's current revision.
    #[error("graph history revision {history_revision} does not match delta source {delta_from}")]
    RevisionMismatch { history_revision: u64, delta_from: u64 },

    /// The delta spans more than one original graph revision.
    #[error("graph history delta {from_revision_id} -> {to_revision_id} is not atomic")]
    NonAtomicDelta { from_revision_id: u64, to_revision_id: u64 },
}

/// Whether one accepted original revision remains publicly replayable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphHistoryReplayability {
    /// Retain the atomic delta as the next replay unit.
    Replayable,
    /// Advance history without retaining the uncrossable delta.
    ReplayBarrier,
}

/// Failure to select a retained graph-history range.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphHistoryRangeError {
    /// The requested target precedes the starting revision.
    #[error("graph history target revision {target_revision_id} precedes source revision {from_revision_id}")]
    BackwardTarget { from_revision_id: u64, target_revision_id: u64 },

    /// The requested starting boundary precedes an uncrossable replay barrier.
    #[error("graph history start {requested} precedes replay floor {replay_floor_revision}")]
    StartBeforeReplayFloor { requested: u64, replay_floor_revision: u64 },

    /// The requested starting boundary has already been pruned.
    #[error("graph history start {requested} precedes oldest available revision {oldest_available}")]
    StartPruned { requested: u64, oldest_available: u64 },

    /// The requested starting revision is not a retained entry boundary.
    #[error("graph history start {requested} is unavailable at current revision {current}")]
    StartUnavailable { requested: u64, current: u64 },
}

/// One immutable retained delta and its absolute planning-cost boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphHistoryEntry {
    delta: GraphDelta,
    estimated_raw_bytes: u64,
    cumulative_estimated_bytes: u64,
}

impl GraphHistoryEntry {
    /// Returns the retained canonical delta.
    #[must_use]
    pub const fn delta(&self) -> &GraphDelta {
        &self.delta
    }

    /// Returns the raw-byte estimate assigned to this replay unit.
    #[must_use]
    pub const fn estimated_raw_bytes(&self) -> u64 {
        self.estimated_raw_bytes
    }

    /// Returns the absolute cumulative estimate at this entry's target.
    #[must_use]
    pub const fn cumulative_estimated_bytes(&self) -> u64 {
        self.cumulative_estimated_bytes
    }
}

/// Immutable shared list of retained history entries.
pub type GraphHistoryEntryList = Arc<[Arc<GraphHistoryEntry>]>;

/// One retained history selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphHistoryRange {
    /// No retained transition is needed from this revision.
    UpToDate {
        /// Revision already reached by the requester.
        revision: u64,
    },

    /// Gapless retained replay units beginning at `from`.
    Deltas {
        /// Exact retained left boundary selected by the request.
        from: u64,
        /// Target originally requested by the caller.
        requested_target: u64,
        /// Right boundary of the final selected atomic entry.
        actual_target: u64,
        /// Detached immutable entries selected for replay.
        entries: GraphHistoryEntryList,
    },
}

/// Level-scoped retained history for one canonical graph lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphHistory {
    current_revision_id: u64,
    current_cumulative_estimated_bytes: u64,
    replay_floor_revision: u64,
    entries: VecDeque<Arc<GraphHistoryEntry>>,
}

impl Default for GraphHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphHistory {
    /// Constructs empty revision-zero graph history.
    #[must_use]
    pub const fn new() -> Self {
        Self { current_revision_id: 0, current_cumulative_estimated_bytes: 0, replay_floor_revision: 0, entries: VecDeque::new() }
    }

    /// Returns the right boundary of the complete retained lineage.
    #[must_use]
    pub const fn current_revision_id(&self) -> u64 {
        self.current_revision_id
    }

    /// Returns the absolute cumulative raw-byte estimate at the current revision.
    #[must_use]
    pub const fn current_cumulative_estimated_bytes(&self) -> u64 {
        self.current_cumulative_estimated_bytes
    }

    /// Returns the inclusive first revision from which replay may begin.
    #[must_use]
    pub const fn replay_floor_revision(&self) -> u64 {
        self.replay_floor_revision
    }

    /// Returns the number of retained replay units.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no replay units remain retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over retained entries from oldest to newest.
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &Arc<GraphHistoryEntry>> + DoubleEndedIterator {
        self.entries.iter()
    }

    /// Appends one gapless original-revision delta.
    ///
    /// # Panics
    ///
    /// Panics before modifying history if the cumulative estimated-byte cost
    /// would overflow `u64`.
    pub fn append(
        &mut self,
        delta: GraphDelta,
        estimated_raw_bytes: u64,
        replayability: GraphHistoryReplayability,
    ) -> Result<(), GraphHistoryAppendError> {
        if self.current_revision_id != delta.from_revision_id() {
            return Err(GraphHistoryAppendError::RevisionMismatch {
                history_revision: self.current_revision_id,
                delta_from: delta.from_revision_id(),
            });
        }

        let target_revision = delta.to_revision_id();
        if target_revision != delta.from_revision_id().checked_add(1).expect("graph revision successor fits u64") {
            return Err(GraphHistoryAppendError::NonAtomicDelta {
                from_revision_id: delta.from_revision_id(),
                to_revision_id: target_revision,
            });
        }

        let cumulative_estimated_bytes = self
            .current_cumulative_estimated_bytes
            .checked_add(estimated_raw_bytes)
            .expect("graph history cumulative estimate fits u64");
        match replayability {
            GraphHistoryReplayability::Replayable => {
                self.entries.push_back(Arc::new(GraphHistoryEntry { delta, estimated_raw_bytes, cumulative_estimated_bytes }));
            }
            GraphHistoryReplayability::ReplayBarrier => {
                self.entries.clear();
                self.replay_floor_revision = target_revision;
            }
        }
        self.current_revision_id = target_revision;
        self.current_cumulative_estimated_bytes = cumulative_estimated_bytes;
        Ok(())
    }

    /// Removes the longest prefix whose target Head level precedes `head_low_level`.
    pub fn prune(&mut self, head_low_level: u32) {
        while self.entries.front().is_some_and(|entry| entry.delta.head_level() < head_low_level) {
            self.entries.pop_front();
        }
    }

    /// Returns the oldest retained left boundary, or the current revision when empty.
    #[must_use]
    pub fn oldest_available_revision(&self) -> u64 {
        self.entries.front().map_or(self.current_revision_id, |entry| entry.delta.from_revision_id())
    }

    /// Captures the gapless retained entries needed to reach a target revision.
    pub fn range(&self, from_revision_id: u64, target_revision_id: u64) -> Result<GraphHistoryRange, GraphHistoryRangeError> {
        if target_revision_id < from_revision_id {
            return Err(GraphHistoryRangeError::BackwardTarget { from_revision_id, target_revision_id });
        }
        if target_revision_id == from_revision_id {
            return Ok(GraphHistoryRange::UpToDate { revision: from_revision_id });
        }

        if from_revision_id < self.replay_floor_revision {
            return Err(GraphHistoryRangeError::StartBeforeReplayFloor {
                requested: from_revision_id,
                replay_floor_revision: self.replay_floor_revision,
            });
        }

        let oldest_available = self.oldest_available_revision();
        if from_revision_id < oldest_available {
            return Err(GraphHistoryRangeError::StartPruned { requested: from_revision_id, oldest_available });
        }
        if from_revision_id == self.current_revision_id {
            return Ok(GraphHistoryRange::UpToDate { revision: from_revision_id });
        }

        let Some(start_index) = find_boundary_index(&self.entries, from_revision_id, || {}) else {
            return Err(GraphHistoryRangeError::StartUnavailable { requested: from_revision_id, current: self.current_revision_id });
        };

        let mut selected = Vec::new();
        let mut actual_target = from_revision_id;
        for entry in self.entries.range(start_index..) {
            actual_target = entry.delta.to_revision_id();
            selected.push(Arc::clone(entry));
            if actual_target == target_revision_id {
                break;
            }
        }

        Ok(GraphHistoryRange::Deltas {
            from: from_revision_id,
            requested_target: target_revision_id,
            actual_target,
            entries: Arc::from(selected),
        })
    }
}

/// Locates an exact retained left boundary with bounded inspection.
///
/// Gapless append preserves strictly increasing stored left boundaries, so
/// classification does not need to visit newer history entries.
fn find_boundary_index(entries: &VecDeque<Arc<GraphHistoryEntry>>, revision_id: u64, mut inspect: impl FnMut()) -> Option<usize> {
    let mut low = 0;
    let mut high = entries.len();
    while low < high {
        let middle = low + (high - low) / 2;
        inspect();
        match entries[middle].delta.from_revision_id().cmp(&revision_id) {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Some(middle),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    use super::*;

    fn delta(from: u64, to: u64, head_level: u32) -> GraphDelta {
        GraphDelta::new(from, to, to * 10, head_level, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid fixture delta")
    }

    fn append_replayable(
        history: &mut GraphHistory,
        delta: GraphDelta,
        estimated_raw_bytes: u64,
    ) -> Result<(), GraphHistoryAppendError> {
        history.append(delta, estimated_raw_bytes, GraphHistoryReplayability::Replayable)
    }

    fn history() -> GraphHistory {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 2), 10).expect("first entry");
        append_replayable(&mut history, delta(1, 2, 5), 30).expect("second entry");
        append_replayable(&mut history, delta(2, 3, 5), 20).expect("final entry");
        history
    }

    fn estimated_cost(entries: &GraphHistoryEntryList) -> u64 {
        let first = entries.first().expect("nonempty retained range");
        let last = entries.last().expect("nonempty retained range");
        let left_boundary = first.cumulative_estimated_bytes() - first.estimated_raw_bytes();
        last.cumulative_estimated_bytes() - left_boundary
    }

    #[test]
    fn new_history_starts_at_revision_zero_without_cost_or_entries() {
        let history = GraphHistory::new();

        assert_eq!(history.current_revision_id(), 0);
        assert_eq!(history.current_cumulative_estimated_bytes(), 0);
        assert_eq!(history.replay_floor_revision(), 0);
        assert_eq!(history.oldest_available_revision(), 0);
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        assert_eq!(history, GraphHistory::default());
    }

    #[test]
    fn append_accepts_atomic_boundaries_and_accumulates_absolute_cost() {
        let history = history();
        let entries: Vec<_> = history.entries().collect();

        assert_eq!(history.current_revision_id(), 3);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].delta().from_revision_id(), 0);
        assert_eq!(entries[0].delta().to_revision_id(), 1);
        assert_eq!(entries[0].estimated_raw_bytes(), 10);
        assert_eq!(entries[0].cumulative_estimated_bytes(), 10);
        assert_eq!(entries[1].delta().from_revision_id(), 1);
        assert_eq!(entries[1].delta().to_revision_id(), 2);
        assert_eq!(entries[1].estimated_raw_bytes(), 30);
        assert_eq!(entries[1].cumulative_estimated_bytes(), 40);
        assert_eq!(entries[2].cumulative_estimated_bytes(), 60);
    }

    #[test]
    fn append_accepts_the_maximum_cumulative_cost_boundary() {
        let mut history = GraphHistory::new();

        append_replayable(&mut history, delta(0, 1, 1), u64::MAX).expect("maximum cumulative cost is valid");

        assert_eq!(history.current_revision_id(), 1);
        assert_eq!(history.current_cumulative_estimated_bytes(), u64::MAX);
        assert_eq!(history.entries().next().expect("entry").cumulative_estimated_bytes(), u64::MAX);
    }

    #[test]
    fn cumulative_cost_overflow_panics_before_mutating_history() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), u64::MAX).expect("maximum cumulative cost is valid");
        let before = history.clone();

        let result = catch_unwind(AssertUnwindSafe(|| append_replayable(&mut history, delta(1, 2, 2), 1)));

        assert!(result.is_err());
        assert_eq!(history, before);
    }

    #[test]
    fn revision_mismatch_precedes_cumulative_cost_overflow() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), u64::MAX).expect("maximum cumulative cost is valid");
        let before = history.clone();

        assert_eq!(
            append_replayable(&mut history, delta(0, 2, 2), 1),
            Err(GraphHistoryAppendError::RevisionMismatch { history_revision: 1, delta_from: 0 })
        );
        assert_eq!(history, before);
    }

    #[test]
    fn non_atomic_delta_precedes_cumulative_cost_overflow() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), u64::MAX).expect("maximum cumulative cost is valid");
        let before = history.clone();

        assert_eq!(
            append_replayable(&mut history, delta(1, 3, 3), 1),
            Err(GraphHistoryAppendError::NonAtomicDelta { from_revision_id: 1, to_revision_id: 3 })
        );
        assert_eq!(history, before);
    }

    #[test]
    fn replay_barrier_clears_entries_and_advances_the_inclusive_floor() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), 10).expect("first entry");

        history.append(delta(1, 2, 2), 20, GraphHistoryReplayability::ReplayBarrier).expect("gapless replay barrier");

        assert_eq!(history.current_revision_id(), 2);
        assert_eq!(history.current_cumulative_estimated_bytes(), 30);
        assert_eq!(history.replay_floor_revision(), 2);
        assert_eq!(history.oldest_available_revision(), 2);
        assert!(history.is_empty());
        assert_eq!(
            history.range(1, 2),
            Err(GraphHistoryRangeError::StartBeforeReplayFloor { requested: 1, replay_floor_revision: 2 })
        );
        assert_eq!(history.range(2, 2), Ok(GraphHistoryRange::UpToDate { revision: 2 }));
    }

    #[test]
    fn replay_barriers_preserve_absolute_cost_and_move_the_floor_only_forward() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), 10).expect("first entry");
        append_replayable(&mut history, delta(1, 2, 2), 20).expect("second entry");
        history.append(delta(2, 3, 3), 30, GraphHistoryReplayability::ReplayBarrier).expect("first barrier");
        append_replayable(&mut history, delta(3, 4, 4), 40).expect("post-barrier entry");

        let range = history.range(3, 4).expect("source at the floor remains replayable");
        let GraphHistoryRange::Deltas { entries, actual_target, .. } = range else {
            panic!("post-barrier range contains one entry");
        };
        assert_eq!(actual_target, 4);
        assert_eq!(entries[0].cumulative_estimated_bytes(), 100);
        assert_eq!(entries[0].cumulative_estimated_bytes() - entries[0].estimated_raw_bytes(), 60);
        assert_eq!(estimated_cost(&entries), 40);

        history.append(delta(4, 5, 5), 50, GraphHistoryReplayability::ReplayBarrier).expect("second barrier");

        assert_eq!(history.current_revision_id(), 5);
        assert_eq!(history.current_cumulative_estimated_bytes(), 150);
        assert_eq!(history.replay_floor_revision(), 5);
        assert_eq!(history.oldest_available_revision(), 5);
        assert!(history.is_empty());
    }

    #[test]
    fn replay_barrier_overflow_panics_before_clearing_or_advancing() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), u64::MAX).expect("maximum cumulative cost is valid");
        let before = history.clone();

        let result = catch_unwind(AssertUnwindSafe(|| history.append(delta(1, 2, 2), 1, GraphHistoryReplayability::ReplayBarrier)));

        assert!(result.is_err());
        assert_eq!(history, before);
    }

    #[test]
    fn nongapless_append_leaves_history_unchanged() {
        let mut history = history();
        let before = history.clone();

        assert_eq!(
            append_replayable(&mut history, delta(2, 4, 4), 100),
            Err(GraphHistoryAppendError::RevisionMismatch { history_revision: 3, delta_from: 2 })
        );
        assert_eq!(history, before);
    }

    #[test]
    fn pruning_removes_only_the_longest_eligible_prefix() {
        let mut history = GraphHistory::new();
        append_replayable(&mut history, delta(0, 1, 1), 10).expect("first entry");
        append_replayable(&mut history, delta(1, 2, 5), 20).expect("retained entry");
        append_replayable(&mut history, delta(2, 3, 2), 30).expect("eligible non-prefix entry");

        history.prune(4);

        let entries: Vec<_> = history.entries().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].delta().from_revision_id(), 1);
        assert_eq!(entries[1].delta().from_revision_id(), 2);
        assert_eq!(history.oldest_available_revision(), 1);
        assert_eq!(history.current_revision_id(), 3);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(entries[0].cumulative_estimated_bytes() - entries[0].estimated_raw_bytes(), 10);
    }

    #[test]
    fn pruning_preserves_detached_atomic_entries() {
        let mut history = history();
        let captured = match history.range(1, 2).expect("retained atomic target") {
            GraphHistoryRange::Deltas { actual_target, entries, .. } => {
                assert_eq!(actual_target, 2);
                entries
            }
            GraphHistoryRange::UpToDate { .. } => panic!("range contains an entry"),
        };
        let retained_entry = Arc::clone(&captured[0]);

        history.prune(6);

        assert!(history.is_empty());
        assert_eq!(history.oldest_available_revision(), 3);
        assert_eq!(history.current_revision_id(), 3);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(retained_entry.delta().from_revision_id(), 1);
        assert_eq!(retained_entry.delta().to_revision_id(), 2);
        assert_eq!(retained_entry.cumulative_estimated_bytes(), 40);
        assert!(Arc::ptr_eq(&retained_entry, &captured[0]));
    }

    #[test]
    fn ordinary_pruning_preserves_absolute_range_cost_boundaries() {
        let mut history = history();
        let complete = match history.range(0, 3).expect("complete retained range") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains entries"),
        };
        assert_eq!(estimated_cost(&complete), 60);

        history.prune(5);

        let retained = match history.range(1, 3).expect("retained suffix") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains entries"),
        };
        assert_eq!(retained[0].cumulative_estimated_bytes(), 40);
        assert_eq!(retained[0].cumulative_estimated_bytes() - retained[0].estimated_raw_bytes(), 10);
        assert_eq!(estimated_cost(&retained), 50);
        assert_eq!(history.replay_floor_revision(), 0, "ordinary pruning does not create a replay barrier");
    }

    #[test]
    fn replay_floor_advancement_preserves_detached_entry_handles() {
        let mut history = history();
        let captured = match history.range(0, 2).expect("retained prefix") {
            GraphHistoryRange::Deltas { entries, .. } => entries,
            GraphHistoryRange::UpToDate { .. } => panic!("range contains entries"),
        };
        let first = Arc::clone(&captured[0]);

        history.append(delta(3, 4, 6), 70, GraphHistoryReplayability::ReplayBarrier).expect("replay barrier");

        assert!(history.is_empty());
        assert_eq!(history.replay_floor_revision(), 4);
        assert_eq!(first.delta().from_revision_id(), 0);
        assert_eq!(first.delta().to_revision_id(), 1);
        assert_eq!(first.cumulative_estimated_bytes(), 10);
        assert!(Arc::ptr_eq(&first, &captured[0]));
    }

    #[test]
    fn range_checks_target_order_before_retention() {
        let mut history = history();
        history.prune(5);

        assert_eq!(
            history.range(0, 0),
            Ok(GraphHistoryRange::UpToDate { revision: 0 }),
            "equality is decided before the pruned-boundary check"
        );
        assert_eq!(history.range(3, 2), Err(GraphHistoryRangeError::BackwardTarget { from_revision_id: 3, target_revision_id: 2 }));
    }

    #[test]
    fn target_order_and_equality_precede_the_replay_floor() {
        let mut history = GraphHistory::new();
        history.append(delta(0, 1, 1), 10, GraphHistoryReplayability::ReplayBarrier).expect("replay barrier");

        assert_eq!(history.range(0, 0), Ok(GraphHistoryRange::UpToDate { revision: 0 }));
        assert_eq!(history.range(1, 0), Err(GraphHistoryRangeError::BackwardTarget { from_revision_id: 1, target_revision_id: 0 }));
        assert_eq!(
            history.range(0, 1),
            Err(GraphHistoryRangeError::StartBeforeReplayFloor { requested: 0, replay_floor_revision: 1 })
        );
    }

    #[test]
    fn range_classifies_pruned_and_unavailable_starts() {
        let mut history = history();
        history.prune(5);

        assert_eq!(history.range(0, 3), Err(GraphHistoryRangeError::StartPruned { requested: 0, oldest_available: 1 }));
        assert_eq!(history.range(4, 5), Err(GraphHistoryRangeError::StartUnavailable { requested: 4, current: 3 }));
    }

    #[test]
    fn unavailable_future_boundary_uses_bounded_search() {
        let mut history = GraphHistory::new();
        for revision in 0..4_096 {
            history
                .append(
                    delta(revision, revision + 1, u32::try_from(revision + 1).expect("test level fits u32")),
                    1,
                    GraphHistoryReplayability::Replayable,
                )
                .expect("later entry");
        }

        let mut inspected = 0;
        assert_eq!(find_boundary_index(&history.entries, 5_000, || inspected += 1), None);
        assert!(inspected <= 13, "binary search inspected {inspected} of {} entries", history.len());
        assert_eq!(history.range(5_000, 5_001), Err(GraphHistoryRangeError::StartUnavailable { requested: 5_000, current: 4_096 }));
    }

    #[test]
    fn range_selects_complete_entries_at_exact_inside_and_future_targets() {
        let history = history();

        let assert_range = |target, expected_actual, expected_targets: &[u64]| {
            let range = history.range(0, target).expect("available range");
            let GraphHistoryRange::Deltas { from, requested_target, actual_target, entries } = range else {
                panic!("a forward retained range has entries");
            };
            assert_eq!(from, 0);
            assert_eq!(requested_target, target);
            assert_eq!(actual_target, expected_actual);
            assert_eq!(entries.iter().map(|entry| entry.delta().to_revision_id()).collect::<Vec<_>>(), expected_targets);
        };

        assert_range(1, 1, &[1]);
        assert_range(2, 2, &[1, 2]);
        assert_range(3, 3, &[1, 2, 3]);
        assert_range(99, 3, &[1, 2, 3]);
        assert_eq!(history.range(3, 99), Ok(GraphHistoryRange::UpToDate { revision: 3 }));

        let GraphHistoryRange::Deltas { from, requested_target, actual_target, entries } =
            history.range(1, 2).expect("exact interior range")
        else {
            panic!("interior range contains one entry");
        };
        assert_eq!(from, 1);
        assert_eq!(requested_target, 2);
        assert_eq!(actual_target, 2);
        assert_eq!(entries.iter().map(|entry| entry.delta().to_revision_id()).collect::<Vec<_>>(), [2]);
    }

    #[test]
    fn empty_history_returns_current_up_to_date_or_typed_start_errors() {
        let history = GraphHistory::new();

        assert_eq!(history.range(0, 1), Ok(GraphHistoryRange::UpToDate { revision: 0 }));
        assert_eq!(history.range(1, 2), Err(GraphHistoryRangeError::StartUnavailable { requested: 1, current: 0 }));
    }
}
