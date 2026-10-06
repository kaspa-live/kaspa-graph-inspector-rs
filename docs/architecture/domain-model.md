# Shared domain model

## Scope and ownership

This document owns shared identities and graph vocabulary used by more than
one component. It defines what the values mean. Storage owns their persistent
representation and database-relative validation; NodeService owns raw-node
normalization; processing documents own how workers use them.

Rust declarations follow the semantic-shape convention in the
[architecture index](README.md#contract-conventions-and-scope).

## Shared value types — settled

```rust
type Timestamp = u64;

enum BlockColor {
    Gray,
    Blue,
    Red,
}

const MAX_DAA_SCORE: u64 = i64::MAX as u64 - 1;
const MAX_BLUE_SCORE: u64 = i64::MAX as u64;

struct ValidatedNodeBlock {
    hash: BlockHash,
    selected_parent: BlockHash,
    direct_parents: Vec<BlockHash>,
    blue_merge_set: Vec<BlockHash>,
    red_merge_set: Vec<BlockHash>,
    timestamp: Timestamp,
    daa_score: u64,
    blue_score: u64,
    blue_work: BlueWork,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ConsensusOrder {
    blue_work: BlueWork,
    hash: BlockHash,
}

struct VspcPoint {
    consensus_order: ConsensusOrder,
    id: CompactId,
}

struct BlockCoordinate {
    level: u64,
    slot: u64,
}

struct MaterializedSyncAnchor {
    point: VspcPoint,
    selected_parent: BlockHash,
    blue_score: u64,
}
```

`Timestamp` is the rusty-kaspa header timestamp: whole milliseconds since the
Unix epoch. KGI accepts the complete upstream `u64` domain. It is informational
block metadata; KGI does not use it for consensus, ordering, recovery, or
arithmetic.

`BlockColor` is the shared color vocabulary. Storage owns persisted color
transitions, VspcProcessor owns when to request them, and API projections
consume the committed value.

Every KGI DAA score is in `0..=MAX_DAA_SCORE`, and every KGI blue score is in
`0..=MAX_BLUE_SCORE`. The fields remain ordinary `u64`; these constants define
their semantic ranges without introducing wrapper types. The DAA range leaves
`i64::MAX` available for storage's no-VSPC sentinel. The blue-score range
matches the signed PostgreSQL representation of the retained pruning-point
score. These are KGI representability limits, not claims that rusty-kaspa's
upstream `u64` values can never exceed them.

`ValidatedNodeBlock` is the sole full-block value allowed to cross from
NodeService into processing or storage. It is a flattened normalized value,
not a wrapper around the raw RPC block. Its `hash` is the authoritative block
identity carried downstream. Only level-zero parents become `direct_parents`.

For an ordinary non-Genesis block, `selected_parent` must occur in
`direct_parents`; absence cannot produce KGI's later selected-parent index.
Parent and merge-set vectors otherwise preserve node order without uniqueness,
self-reference, or graph-validity checks.

The sole membership exception is the exact Genesis hash discovered for the
validated RPC generation. KGI gives Genesis its domain-owned canonical
representation: synthetic ORIGIN as selected parent, empty direct-parent and
merge-set vectors, and blue score zero. Genesis has no special DAA-score
constraint beyond the common representable range; a configured Genesis DAA
score may be nonzero.

The type proves that every field KGI consumes is present and representable and
that an ordinary selected parent can be encoded by index. Under the
[node trust boundary](overview.md#node-trust-boundary--settled), it does not
prove consensus correctness, uniqueness or acyclicity, and it makes no claim
that any referenced hash is materialized in the current database.

`BlockHash` is the immutable block identity. Every accepted representation of
one hash denotes the same block and the same deterministic consensus metadata,
including its `ConsensusOrder` and selected parent. A cryptographic hash
collision is outside KGI's runtime fault model. This is a Kaspa block identity
property, not a revision-specific upstream assumption.

`ConsensusOrder` sorts lexicographically by `(blue_work, hash)`. `VspcPoint`
contains that order and must not duplicate the block hash. Explicit `hash()`,
`order()`, and `id()` accessors are allowed; `Deref` must not model the
relationship.

`MaterializedSyncAnchor` is the complete committed starting point shared by
the recovery pump and both processors. Its `point` is Materialized under the
invariant defined below. Its non-null `selected_parent` is the persisted
selected-parent hash. For Genesis it is synthetic ORIGIN, which is not an
actual direct parent.

`CompactId` is an incremental positive signed 64-bit internal identifier.
Storage assigns values strictly increasingly and never reuses a value within
one database generation. Transaction rollback may leave gaps. The identifier
may cross internal component boundaries where a database-generation cut must
be represented.

`BlockCoordinate` is a database-local display coordinate. Slot allocation
order is not canonical across instances: the same block hash may have
different coordinates in independently allocated databases. A rebuilt
pruning point occupies `(level=1, slot=0)`.

## Coordinate terminology — settled

KGI v2 uses these names:

```text
height              -> level
height_group_index  -> slot
height_groups       -> levels
levels.size remains size
```

`levels.size` denotes the number of allocated slots in a level, not a
horizontal width.

## Identity and materiality vocabulary — settled

Block identity and block materiality are distinct concepts:

```text
Absent
    no persistent identity exists for the hash

BoundaryIdentity
    a persistent identity exists for a reference classified outside the
    retained pruning-point boundary, but no materialized block exists

Materialized
    complete persistent block data and a BlockCoordinate exist for the
    identity, and every retained block in its DAG past is Materialized up to
    permanent BoundaryIdentity leaves outside the pruning-point boundary
```

The shared lookup result is conceptually:

```rust
enum BlockPresence {
    Absent,
    BoundaryIdentity { id: CompactId },
    Materialized {
        id: CompactId,
        coordinate: BlockCoordinate,
    },
}
```

An identifier row, `CompactId`, or raw `blocks` row alone does not provide
processing evidence of the semantic Materialized state. For ordinary block
admission, processing may rely on a definite successful `materialize_block`
outcome or on `BlockPresence::Materialized` returned by the run's
processing-valid `ValidatedDbClient`. Both results carry the retained-past
invariant as part of Materialized; there is no separate materiality state or
certificate. Storage establishes the invariant at bootstrap and every block
commit and preserves it for the database generation.

Ordinary unresolved orphan hashes are not boundary identities. They remain
transient processing state. A permanent `BoundaryIdentity` is never promoted
to `Materialized`; attempting to materialize it is an invariant violation.

## Pruning-point boundary and ORIGIN — settled

The retained pruning-point boundary separates materialized retained graph
state from referenced history that KGI deliberately does not retain. A
reference outside that boundary can have a permanent identity without a
materialized block or drawable graph edge.

ORIGIN is a synthetic outside-boundary identity used when the pruning point's
selected parent is ORIGIN. This preserves a non-null selected-parent identity,
including for Genesis. ORIGIN is not an actual direct parent. A materialized
Genesis has zero actual direct parents; a pruned non-Genesis pruning point can
have actual direct parents even when none has a visible retained edge.

Selected-parent identity, actual direct-parent membership, retained edge
visibility, and materiality are therefore separate properties and must not be
inferred from one another.

## VSPC value types — settled

`VspcChange` is the normalized hash-level transition shared by NodeService,
the recovery pump, and VspcProcessor:

```rust
struct VspcChange {
    removed: Arc<[BlockHash]>,
    added: Arc<[BlockHash]>,
}
```

Both vectors preserve node order, including repeated members. `VspcChange`
does not assert uniqueness or disjointness.

After endpoint and member resolution, VspcProcessor uses:

```rust
struct ReadyVspcChange {
    source: VspcPoint,
    destination: VspcPoint,
    removed: Arc<[CompactId]>,
    added: Arc<[CompactId]>,
}
```

`destination` carries mandatory consensus order through `VspcPoint`.
`ReadyVspcChange` contains the IDs and endpoint order required for readiness
and sequencing. Its ID vectors preserve the positions of the corresponding
hash vectors, including repeats. Storage loads added-block merge sets inside
its transaction.
