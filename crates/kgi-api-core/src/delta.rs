#![allow(dead_code, reason = "the canonical-delta operation is completed in the next increment-18 point")]

use std::sync::atomic::{AtomicU64, Ordering};

use kgi_api_model::protocol::{
    ADAPTIVE_DELTA_INTERVALS, DELTA_BASE_REVISION_SPAN, INITIAL_ADAPTIVE_DELTA_INTERVAL, MAX_DELTA_REVISION_SPAN,
};
use smallvec::SmallVec;

use crate::publication::GraphPublicationState;

/// Process-wide canonical-delta interval shared across publication replacement.
pub(crate) struct ActiveDeltaInterval {
    value: AtomicU64,
}

impl ActiveDeltaInterval {
    /// Constructs the interval at its settled production initial value.
    pub(crate) const fn new() -> Self {
        Self { value: AtomicU64::new(INITIAL_ADAPTIVE_DELTA_INTERVAL) }
    }

    /// Returns the interval used by a new logical delta decision.
    pub(crate) fn current(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Replaces the interval when the candidate is one of the permitted values.
    ///
    /// Returns whether the candidate was accepted. Rejection leaves the current
    /// value unchanged.
    pub(crate) fn try_update(&self, candidate: u64) -> bool {
        if !ADAPTIVE_DELTA_INTERVALS.contains(&candidate) {
            return false;
        }
        self.value.store(candidate, Ordering::Release);
        true
    }
}

impl Default for ActiveDeltaInterval {
    fn default() -> Self {
        Self::new()
    }
}

/// One source-anchored range selected from the canonical dyadic ladder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeltaRevisionRange {
    source_revision: u64,
    target_revision: u64,
}

impl DeltaRevisionRange {
    const fn selected(source_revision: u64, target_revision: u64) -> Self {
        Self { source_revision, target_revision }
    }

    /// Returns the request's fixed source revision.
    pub(crate) const fn source_revision(self) -> u64 {
        self.source_revision
    }

    /// Returns the selected atomic target boundary.
    pub(crate) const fn target_revision(self) -> u64 {
        self.target_revision
    }
}

/// Initial target decision for one forward canonical-delta request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeltaTargetSelection {
    /// A retained range may be selected and sized through this boundary.
    Range(DeltaRevisionRange),
    /// An advancing Head has not reached the nearest eligible boundary.
    WaitForWakeup {
        /// First future revision eligible under the current active interval.
        wake_at_revision_id: u64,
    },
}

/// Pure dyadic target ladder for one source and one coherently captured Head.
pub(crate) struct DeltaTargetLadder {
    source_revision: u64,
    head_revision: u64,
    current_target: Option<u64>,
    active_interval: u64,
    targets: SmallVec<[u64; 8]>,
}

impl DeltaTargetLadder {
    /// Constructs the ladder for a forward request and an optional existing
    /// non-size-limited cache target.
    ///
    /// # Panics
    ///
    /// Panics when the request is not forward, the existing target is outside
    /// the source/Head range or ladder, or successor arithmetic overflows.
    pub(crate) fn new(source_revision: u64, head_revision: u64, current_target: Option<u64>, active_interval: u64) -> Self {
        assert!(source_revision < head_revision, "a delta target ladder requires a forward request");
        assert!(ADAPTIVE_DELTA_INTERVALS.contains(&active_interval), "the active delta interval is validated before selection");
        let targets = Self::target_boundaries(source_revision);
        if let Some(current_target) = current_target {
            assert!(
                source_revision < current_target && current_target <= head_revision,
                "an existing delta target lies inside the captured forward range"
            );
            assert!(targets.contains(&current_target), "an existing delta target is a boundary on its source ladder");
        }
        Self { source_revision, head_revision, current_target, active_interval, targets }
    }

    /// Selects the request target or the next Live wakeup boundary.
    pub(crate) fn select(&self, state: GraphPublicationState) -> DeltaTargetSelection {
        let successor = self.targets[0];
        if successor <= self.head_revision {
            return DeltaTargetSelection::Range(DeltaRevisionRange::selected(self.source_revision, successor));
        }

        let eligible = |target: u64| {
            state == GraphPublicationState::Stale || target == successor || target - self.source_revision >= self.active_interval
        };
        if let Some(target) = self.targets.iter().copied().find(|&target| target <= self.head_revision && eligible(target)) {
            return DeltaTargetSelection::Range(DeltaRevisionRange::selected(self.source_revision, target));
        }

        let wake_at_revision_id = self
            .targets
            .iter()
            .copied()
            .filter(|&target| target > self.head_revision && eligible(target))
            .min()
            .expect("the ordinary successor is always a future eligible boundary");
        DeltaTargetSelection::WaitForWakeup { wake_at_revision_id }
    }

    /// Returns the nearest future boundary for a cursor and active interval.
    pub(crate) fn next_eligible_boundary_from(source_revision: u64, active_interval: u64) -> u64 {
        assert!(ADAPTIVE_DELTA_INTERVALS.contains(&active_interval), "the active delta interval is validated before selection");
        let targets = Self::target_boundaries(source_revision);
        let successor = targets[0];
        targets
            .iter()
            .copied()
            .filter(|&target| target == successor || target - source_revision >= active_interval)
            .min()
            .expect("the ordinary successor is always an eligible boundary")
    }

    /// Iterates the selected target and its permitted size-descent boundaries.
    ///
    /// An existing non-size-limited cache target is already encoded reusable
    /// coverage, so it is the lower bound: selecting it permits byte-sharing
    /// reclassification, while selecting a shorter target would discard
    /// available progress. Targets are yielded farthest to nearest so the
    /// history can accept the first one whose additive estimate fits.
    pub(crate) fn fitting_targets(&self, selected: DeltaRevisionRange) -> impl Iterator<Item = u64> + '_ {
        assert_eq!(selected.source_revision, self.source_revision, "a fitting target belongs to this source ladder");
        let selected_index = self
            .targets
            .iter()
            .position(|&target| target == selected.target_revision)
            .expect("the selected target is a boundary on its source ladder");
        assert!(selected.target_revision <= self.head_revision, "the selected target does not exceed the captured Head");
        if let Some(current_target) = self.current_target {
            assert!(current_target <= selected.target_revision, "an inadequate cached target precedes the selected target");
        }

        self.targets[selected_index..]
            .iter()
            .copied()
            .take_while(|target| self.current_target.is_none_or(|current| *target >= current))
    }

    /// Returns this source's complete descending target ladder.
    ///
    /// The ordinary successor comes first. Each following entry is the nearest
    /// strictly later boundary aligned to a successively smaller power of two.
    /// Duplicate alignments are removed, leaving targets ordered from farthest
    /// to nearest and retaining the final atomic boundary when distinct.
    fn target_boundaries(source_revision: u64) -> SmallVec<[u64; 8]> {
        let successor = Self::ordinary_successor(source_revision);
        let successor_span = successor - source_revision;
        let mut targets = SmallVec::new();
        targets.push(successor);

        let mut span = successor_span.next_power_of_two() / 2;
        while span > 0 {
            let quotient = source_revision / span;
            let target =
                quotient.checked_add(1).and_then(|quotient| quotient.checked_mul(span)).expect("delta target boundary fits u64");
            if target < successor && targets.last() != Some(&target) {
                targets.push(target);
            }
            span /= 2;
        }
        targets
    }

    fn ordinary_successor(source_revision: u64) -> u64 {
        if source_revision == 0 || source_revision.isolate_lowest_one() < DELTA_BASE_REVISION_SPAN {
            let minimum = source_revision.checked_add(DELTA_BASE_REVISION_SPAN).expect("delta normalization minimum fits u64");
            let remainder = minimum % DELTA_BASE_REVISION_SPAN;
            return if remainder == 0 {
                minimum
            } else {
                minimum.checked_add(DELTA_BASE_REVISION_SPAN - remainder).expect("delta normalization boundary fits u64")
            };
        }

        let span = source_revision.isolate_lowest_one().min(MAX_DELTA_REVISION_SPAN);
        source_revision.checked_add(span).expect("delta ordinary successor fits u64")
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveDeltaInterval, DeltaRevisionRange, DeltaTargetLadder, DeltaTargetSelection};
    use crate::publication::GraphPublicationState;
    use kgi_api_model::protocol::{ADAPTIVE_DELTA_INTERVALS, INITIAL_ADAPTIVE_DELTA_INTERVAL};

    fn range(selection: DeltaTargetSelection) -> DeltaRevisionRange {
        match selection {
            DeltaTargetSelection::Range(range) => range,
            DeltaTargetSelection::WaitForWakeup { wake_at_revision_id } => {
                panic!("expected range, got wake boundary {wake_at_revision_id}")
            }
        }
    }

    #[test]
    fn ordinary_successor_normalizes_then_climbs_the_capped_dyadic_ladder() {
        for (source, expected) in [
            (0, 4),
            (1, 8),
            (2, 8),
            (3, 8),
            (4, 8),
            (5, 12),
            (17, 24),
            (24, 32),
            (32, 64),
            (64, 128),
            (128, 256),
            (256, 384),
            (512, 640),
        ] {
            assert_eq!(DeltaTargetLadder::ordinary_successor(source), expected, "source {source}");
        }
    }

    #[test]
    fn target_boundaries_descend_without_duplicates() {
        let aligned = DeltaTargetLadder::new(32, 64, None, 4);
        assert_eq!(aligned.targets.as_slice(), [64, 48, 40, 36, 34, 33]);

        let unaligned = DeltaTargetLadder::new(17, 24, None, 4);
        assert_eq!(unaligned.targets.as_slice(), [24, 20, 18]);
    }

    #[test]
    fn publication_state_and_active_interval_select_targets_and_wakeups() {
        for state in [GraphPublicationState::Synchronizing, GraphPublicationState::Live] {
            let advancing = DeltaTargetLadder::new(32, 40, None, 8);
            assert_eq!(range(advancing.select(state)).target_revision(), 40);

            let waiting = DeltaTargetLadder::new(32, 40, None, 16);
            assert_eq!(waiting.select(state), DeltaTargetSelection::WaitForWakeup { wake_at_revision_id: 48 });
        }

        let stale = DeltaTargetLadder::new(32, 40, None, 128);
        assert_eq!(range(stale.select(GraphPublicationState::Stale)).target_revision(), 40);

        let normalization = DeltaTargetLadder::new(17, 24, None, 8);
        for state in [GraphPublicationState::Synchronizing, GraphPublicationState::Live] {
            assert_eq!(range(normalization.select(state)).target_revision(), 24);
        }
    }

    #[test]
    fn every_active_interval_is_applied_by_both_advancing_states() {
        for (active_interval, expected) in [
            (1, DeltaTargetSelection::Range(DeltaRevisionRange::selected(32, 40))),
            (2, DeltaTargetSelection::Range(DeltaRevisionRange::selected(32, 40))),
            (4, DeltaTargetSelection::Range(DeltaRevisionRange::selected(32, 40))),
            (8, DeltaTargetSelection::Range(DeltaRevisionRange::selected(32, 40))),
            (16, DeltaTargetSelection::WaitForWakeup { wake_at_revision_id: 48 }),
            (32, DeltaTargetSelection::WaitForWakeup { wake_at_revision_id: 64 }),
            (64, DeltaTargetSelection::WaitForWakeup { wake_at_revision_id: 64 }),
            (128, DeltaTargetSelection::WaitForWakeup { wake_at_revision_id: 64 }),
        ] {
            assert!(ADAPTIVE_DELTA_INTERVALS.contains(&active_interval));
            for state in [GraphPublicationState::Synchronizing, GraphPublicationState::Live] {
                let ladder = DeltaTargetLadder::new(32, 40, None, active_interval);
                assert_eq!(ladder.select(state), expected, "state {state:?}, interval {active_interval}");
            }
        }
    }

    #[test]
    fn size_descent_yields_farthest_to_nearest_boundaries() {
        let ladder = DeltaTargetLadder::new(32, 64, None, 4);
        let selected = range(ladder.select(GraphPublicationState::Live));
        assert_eq!(ladder.fitting_targets(selected).collect::<Vec<_>>(), [64, 48, 40, 36, 34, 33]);
    }

    #[test]
    fn existing_target_bounds_descent_and_can_be_reclassified() {
        let ladder = DeltaTargetLadder::new(32, 64, Some(48), 4);
        let selected = range(ladder.select(GraphPublicationState::Live));
        assert_eq!(ladder.fitting_targets(selected).collect::<Vec<_>>(), [64, 48]);
    }

    #[test]
    fn active_interval_accepts_only_settled_values_without_implicit_reset() {
        let interval = ActiveDeltaInterval::new();
        assert_eq!(interval.current(), INITIAL_ADAPTIVE_DELTA_INTERVAL);
        for candidate in ADAPTIVE_DELTA_INTERVALS {
            assert!(interval.try_update(candidate));
            assert_eq!(interval.current(), candidate);
        }
        assert!(!interval.try_update(3));
        assert_eq!(interval.current(), 128);
    }

    #[test]
    #[should_panic(expected = "delta ordinary successor fits u64")]
    fn successor_overflow_is_an_internal_invariant_failure() {
        let _ = DeltaTargetLadder::ordinary_successor(u64::MAX - 127);
    }
}
