use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use thiserror::Error;

use crate::delta::GraphDelta;

/// Failure to append a delta to retained graph history.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphHistoryAppendError {
    /// The delta does not begin at the history's current revision.
    #[error("graph history revision {history_revision} does not match delta source {delta_from}")]
    RevisionMismatch { history_revision: u64, delta_from: u64 },
}

/// Failure to select a retained graph-history range.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GraphHistoryRangeError {
    /// The requested target precedes the starting revision.
    #[error("graph history target revision {target_revision_id} precedes source revision {from_revision_id}")]
    BackwardTarget { from_revision_id: u64, target_revision_id: u64 },

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
        /// Right boundary of the final indivisible retained entry.
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
        Self { current_revision_id: 0, current_cumulative_estimated_bytes: 0, entries: VecDeque::new() }
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

    /// Appends one gapless direct or aggregated delta.
    pub fn append(&mut self, delta: GraphDelta, estimated_raw_bytes: u64) -> Result<(), GraphHistoryAppendError> {
        if self.current_revision_id != delta.from_revision_id() {
            return Err(GraphHistoryAppendError::RevisionMismatch {
                history_revision: self.current_revision_id,
                delta_from: delta.from_revision_id(),
            });
        }

        let cumulative_estimated_bytes = self
            .current_cumulative_estimated_bytes
            .checked_add(estimated_raw_bytes)
            .expect("graph history cumulative estimate fits u64");
        let target_revision = delta.to_revision_id();
        self.entries.push_back(Arc::new(GraphHistoryEntry { delta, estimated_raw_bytes, cumulative_estimated_bytes }));
        self.current_revision_id = target_revision;
        self.current_cumulative_estimated_bytes = cumulative_estimated_bytes;
        Ok(())
    }

    /// Removes the longest prefix whose target level precedes `head_low_level`.
    pub fn prune(&mut self, head_low_level: u64) {
        while self.entries.front().is_some_and(|entry| entry.delta.high_level() < head_low_level) {
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
            if actual_target >= target_revision_id {
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

// Gapless append preserves strictly increasing stored left boundaries, so an
// exact boundary can be classified without visiting newer history entries.
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
    use std::collections::HashMap;

    use super::*;

    fn delta(from: u64, to: u64, high_level: u64) -> GraphDelta {
        GraphDelta::new(from, to, to * 10, high_level, HashMap::new(), HashMap::new(), Vec::new(), HashMap::new(), HashMap::new())
            .expect("valid fixture delta")
    }

    fn history() -> GraphHistory {
        let mut history = GraphHistory::new();
        history.append(delta(0, 1, 2), 10).expect("first entry");
        history.append(delta(1, 4, 5), 30).expect("aggregated entry");
        history.append(delta(4, 5, 5), 20).expect("final entry");
        history
    }

    #[test]
    fn new_history_starts_at_revision_zero_without_cost_or_entries() {
        let history = GraphHistory::new();

        assert_eq!(history.current_revision_id(), 0);
        assert_eq!(history.current_cumulative_estimated_bytes(), 0);
        assert_eq!(history.oldest_available_revision(), 0);
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        assert_eq!(history, GraphHistory::default());
    }

    #[test]
    fn append_accepts_outer_aggregate_boundaries_and_accumulates_absolute_cost() {
        let history = history();
        let entries: Vec<_> = history.entries().collect();

        assert_eq!(history.current_revision_id(), 5);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].delta().from_revision_id(), 0);
        assert_eq!(entries[0].delta().to_revision_id(), 1);
        assert_eq!(entries[0].estimated_raw_bytes(), 10);
        assert_eq!(entries[0].cumulative_estimated_bytes(), 10);
        assert_eq!(entries[1].delta().from_revision_id(), 1);
        assert_eq!(entries[1].delta().to_revision_id(), 4);
        assert_eq!(entries[1].estimated_raw_bytes(), 30);
        assert_eq!(entries[1].cumulative_estimated_bytes(), 40);
        assert_eq!(entries[2].cumulative_estimated_bytes(), 60);
    }

    #[test]
    fn nongapless_append_leaves_history_unchanged() {
        let mut history = history();
        let before = history.clone();

        assert_eq!(
            history.append(delta(3, 6, 6), 100),
            Err(GraphHistoryAppendError::RevisionMismatch { history_revision: 5, delta_from: 3 })
        );
        assert_eq!(history, before);
    }

    #[test]
    fn pruning_removes_only_the_longest_eligible_prefix() {
        let mut history = GraphHistory::new();
        history.append(delta(0, 1, 1), 10).expect("first entry");
        history.append(delta(1, 2, 5), 20).expect("retained entry");
        history.append(delta(2, 4, 2), 30).expect("eligible non-prefix aggregate");

        history.prune(4);

        let entries: Vec<_> = history.entries().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].delta().from_revision_id(), 1);
        assert_eq!(entries[1].delta().from_revision_id(), 2);
        assert_eq!(history.oldest_available_revision(), 1);
        assert_eq!(history.current_revision_id(), 4);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(entries[0].cumulative_estimated_bytes() - entries[0].estimated_raw_bytes(), 10);
    }

    #[test]
    fn pruning_treats_aggregates_as_indivisible_and_preserves_detached_entries() {
        let mut history = history();
        let captured = match history.range(1, 2).expect("target inside aggregate") {
            GraphHistoryRange::Deltas { actual_target, entries, .. } => {
                assert_eq!(actual_target, 4);
                entries
            }
            GraphHistoryRange::UpToDate { .. } => panic!("range contains an aggregate"),
        };
        let retained_entry = Arc::clone(&captured[0]);

        history.prune(6);

        assert!(history.is_empty());
        assert_eq!(history.oldest_available_revision(), 5);
        assert_eq!(history.current_revision_id(), 5);
        assert_eq!(history.current_cumulative_estimated_bytes(), 60);
        assert_eq!(retained_entry.delta().from_revision_id(), 1);
        assert_eq!(retained_entry.delta().to_revision_id(), 4);
        assert_eq!(retained_entry.cumulative_estimated_bytes(), 40);
        assert!(Arc::ptr_eq(&retained_entry, &captured[0]));
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
        assert_eq!(history.range(4, 3), Err(GraphHistoryRangeError::BackwardTarget { from_revision_id: 4, target_revision_id: 3 }));
    }

    #[test]
    fn range_classifies_pruned_and_unavailable_starts() {
        let mut history = history();
        history.prune(5);

        assert_eq!(history.range(0, 5), Err(GraphHistoryRangeError::StartPruned { requested: 0, oldest_available: 1 }));
        assert_eq!(
            history.range(2, 5),
            Err(GraphHistoryRangeError::StartUnavailable { requested: 2, current: 5 }),
            "an internal aggregate revision is not reconstructed"
        );
        assert_eq!(history.range(6, 7), Err(GraphHistoryRangeError::StartUnavailable { requested: 6, current: 5 }));
    }

    #[test]
    fn unavailable_internal_aggregate_boundary_uses_bounded_search() {
        let mut history = GraphHistory::new();
        history.append(delta(0, 100, 100), 10).expect("aggregate entry");
        for revision in 100..4_196 {
            history.append(delta(revision, revision + 1, revision + 1), 1).expect("later entry");
        }

        let mut inspected = 0;
        assert_eq!(find_boundary_index(&history.entries, 50, || inspected += 1), None);
        assert!(inspected <= 13, "binary search inspected {inspected} of {} entries", history.len());
        assert_eq!(
            history.range(50, history.current_revision_id()),
            Err(GraphHistoryRangeError::StartUnavailable { requested: 50, current: 4_196 })
        );
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
        assert_range(2, 4, &[1, 4]);
        assert_range(4, 4, &[1, 4]);
        assert_range(99, 5, &[1, 4, 5]);
        assert_eq!(history.range(5, 99), Ok(GraphHistoryRange::UpToDate { revision: 5 }));
    }

    #[test]
    fn empty_history_returns_current_up_to_date_or_typed_start_errors() {
        let history = GraphHistory::new();

        assert_eq!(history.range(0, 1), Ok(GraphHistoryRange::UpToDate { revision: 0 }));
        assert_eq!(history.range(1, 2), Err(GraphHistoryRangeError::StartUnavailable { requested: 1, current: 0 }));
    }
}
