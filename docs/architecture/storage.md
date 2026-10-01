# Storage architecture

## Scope and ownership

This document owns database connectivity, bootstrap and migration, persistent
representation, transaction semantics, cache publication, and database read
contracts. Shared identity and materiality meanings belong to the
[domain model](domain-model.md). Processing documents own when storage
operations are requested and how their results affect worker state.
The PostgreSQL client, migration framework, and concrete SQL types remain
deferred in the [decision register](../decisions/deferred.md).

## StorageService lifecycle — settled

`StorageService` is a permanent autonomous worker:

```text
StorageService
    -> connect
    -> acquire the database ownership lock
    -> initialize or migrate when authorized
    -> validate schema, network binding, and processing state
    -> publish Arc<ValidatedDbClient>
    -> publish Arc<ValidatedApiDbClient>
```

Conceptual state, lifecycle events, and Supervisor-facing operations are:

```rust
enum StorageServiceState {
    Connecting,
    AwaitingInitialization,
    Ready(Arc<ValidatedDbClient>),
    Unavailable(StorageUnavailableReason),
    Rejected(StorageRejection),
    Stopped,
}

enum StorageServiceStatusState {
    Connecting,
    AwaitingInitialization,
    Ready,
    Unavailable,
    Rejected,
    Stopped,
}

struct StorageServiceStatus {
    state: StorageServiceStatusState,
}

enum StorageServiceEvent {
    ProcessingDbRetired(Arc<ValidatedDbClient>),
    ProcessingDbPublished(Arc<ValidatedDbClient>),
    ApiDbRetired(Arc<ValidatedApiDbClient>),
    ApiDbPublished(Arc<ValidatedApiDbClient>),
    Rejected(StorageRejection),
}

impl StorageService {
    async fn initialize_if_uninitialized(
        &self,
        network_id: NetworkId,
        genesis_hash: BlockHash,
    ) -> Result<(), StorageError>;

    async fn shutdown(&self) -> Result<(), StorageError>;
}
```

`AwaitingInitialization` means StorageService has opened, locked, and inspected
a never-initialized database but cannot yet publish it as usable. The state
remains pending until initialization is authorized and a validated node
identity supplies the complete immutable network binding.
`initialize_if_uninitialized` performs the authorized atomic transition from
`Uninitialized` when required. For an already compatible `Empty`,
`Initialized`, or `Inconsistent` database it is an idempotent no-op; it never
rebinds or erases that database. Its successful return does not carry a
validated client: usable generations are published only through the ordered
event stream.
Service state outlives individual validated DB generations.
`shutdown` is terminal and idempotent; successful completion means
StorageService is `Stopped` and has released its owned pools, database lock,
clients, and caches.
`StorageServiceStatus` is the component observation consumed by the
[API status contract](api-service.md#status-observation--settled). It contains no
validated database capability and never authorizes a database operation or
lifecycle transition.

`ValidatedDbClient` represents exactly one validated connection-pool
generation and owns that generation's caches. A processing run receives one
exact client generation; StorageService never rebinds the client underneath
the run. A replacement generation starts with fresh caches.

`ValidatedApiDbClient` is the storage handle for the read-only pool required by
the [API resource contract](api-service.md#resource-isolation-and-saturation--settled).
It exposes no processing mutation or processing cache.
Network binding, schema, and generation validation precede publication of
either handle. StorageService autonomously opens, reconnects, validates,
retires, and republishes both pool generations. It reports every lifecycle
transition through one reliable ordered `StorageServiceEvent` stream. The
event path must be installed before StorageService can publish either initial
generation or enter `Rejected`.

`ProcessingDbPublished` and `ApiDbPublished` carry the newly usable exact
generation. `ProcessingDbRetired` and `ApiDbRetired` carry the exact generation
that ceased to be usable. Repeated failure reports for an already retired
handle emit no duplicate retirement event or replacement attempt. A
replacement is newly validated and never republishes a retired `Arc`. For each
pool kind, StorageService never publishes a different generation while the
currently published generation remains usable; replacement publication follows
retirement of the prior generation.
`Rejected` reports the terminal service state and its typed reason; status
observation alone never substitutes for that event.
Entering `Rejected` retires any currently published processing and API
generations, emits their retirement events first, and then emits `Rejected`.
For an operation-detected generation failure, StorageService completes the
retirement and enqueues its event before returning the corresponding typed
operation error.

StorageService owns both generation lifecycles: retirement immediately starts
autonomous reconnection and validation, without a request from Supervisor,
ResyncEngine, or ApiService. Events report transitions; they do not initiate
them. Supervisor event handling and API forwarding belong to the
[processing lifecycle](processing-lifecycle.md#processing-session-and-resource-acquisition--settled).
StorageService also owns database-replacement exclusion. The
[ApiService binding contract](api-service.md#api-database-generation-binding--settled)
owns local generation binding and database-backed read admission, the
[API publication contract](api-publication.md#publication-state-and-revision--settled)
owns graph publication state, and the [API protocol](api-protocol.md) owns the
corresponding public responses.

Callers receive semantic read operations and complete domain transactions.
Storage exposes no connection pool, database connection, transaction object,
generic transaction closure, or partial write operations for identity
interning, block rows, parent rows, levels, coloring, or VSPC membership.

Transient connection and validation failures enter `Unavailable` and retry
indefinitely with nominal delays:

```text
1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
```

Each actual delay uses equal jitter from 50% through 100% of the nominal
delay. Every wait is shutdown-cancellable. Reset the sequence only after
StorageService has remained continuously Ready for 60 seconds. Network
binding mismatch and unsupported, newer, v1, partial, or unknown schema
states enter terminal `Rejected` and do not retry under unchanged
configuration.

A connection-level storage failure retires the validated generation, but does
not retroactively revoke independent operations already in flight. The
[processing lifecycle](processing-lifecycle.md) owns the resulting session
termination. Retirement emits the applicable exact-generation event before a
replacement can be published. Each operation reports its actual outcome:

- a definitely committed transaction remains successful and authoritative;
- connection loss before commit with a proven rollback is
  `ServiceGenerationLost(Storage)` rather than a persistence-transaction
  fault; and
- a connection loss with an ambiguous commit outcome remains ambiguous and is
  reported as `Persistence(AmbiguousCommit)`.

All persistent mutations are transactional. Cache entries become visible
only after definite commit. Outside the database-replacement gate below, no
connection epoch, per-operation revocation check, cache generation, or global
operation-completion barrier is required.

### API read exclusion during database replacement — settled

Every database phase performed through `ValidatedApiDbClient` holds a shared
database-replacement permit from before its read-only transaction begins until
the complete bounded projection has been detached into memory and the
transaction, connection, and permit have been released. Serialization,
compression, and response delivery occur outside this permit and cannot issue
follow-up database reads.

Before `rebuild_from_pruning_point` clears or replaces processing data,
StorageService atomically closes new API DB admission, retires the current
`ValidatedApiDbClient` generation, and boundedly drains or cancels all database
phases that still hold shared permits. Retirement emits the ordered
`StorageServiceEvent::ApiDbRetired` and starts autonomous replacement of the API
pool. StorageService then acquires the exclusive replacement permit. A request
that detached its projection before closure may finish returning that old
coherent image. Every other affected request reports database-backed read
unavailability; the [API protocol](api-protocol.md#anchored-graph-windows--settled)
owns its HTTP mapping. An API read cannot delay replacement without bound.

The exclusive permit remains held through the atomic replacement outcome and
cache publication or generation retirement. StorageService publishes no
replacement `ValidatedApiDbClient` until the database is again one coherent,
validated generation. It then emits `StorageServiceEvent::ApiDbPublished` for that
generation. Subsequent forwarding and public-read admission belong to the
linked processing and API lifecycle owners; publication itself does not open
public historical-request availability.

If replacement uses PostgreSQL `TRUNCATE`, transactional rollback does **not**
make it generally MVCC-safe for pre-existing snapshots. The gate must ensure a
request returns one detached old image, one coherent replacement-generation
image, or an unavailable read outcome, never mixed or partially rebuilt
tables. See PostgreSQL's
[`TRUNCATE`](https://www.postgresql.org/docs/current/sql-truncate.html) and
[MVCC caveat](https://www.postgresql.org/docs/current/mvcc-caveats.html)
documentation. Concrete permit, cancellation, and transaction primitives are
deferred in the [decision register](../decisions/deferred.md).

### Transaction retries

Only PostgreSQL SQLSTATE `40001` (serialization failure) and `40P01` (deadlock
detected) authorize a local retry of a processing semantic transaction. Retry
the **complete transaction** at most three times, after nominal delays `10ms`,
`50ms`, and `250ms`, each with equal jitter from 50% through 100%.

Return every other definite failure immediately. An unexpected nonretryable
transaction failure with a proven rollback and a still-valid database
generation is `Persistence(DefiniteFailure)`; semantic and representability
failures keep their more specific typed errors. Never retry a connection loss
during commit because its outcome is ambiguous. Exhaustion produces
`Persistence(RetryExhausted)` without retiring the still-valid database
generation. The [processing lifecycle](processing-lifecycle.md) owns every
cross-worker disposition. No cache state is published before definite commit.

### Internal concurrency

The storage implementation provides separate sequential materialization and
VSPC mutation lanes while allowing their transactions to overlap. Rebuild
gets exclusive mutation access after processor deactivation. Reads remain
concurrent.

## Database bootstrap and validation — settled

Startup distinguishes these states:

```text
Uninitialized
    no complete KGI schema and immutable network binding exists; this is a
    transient storage/service initialization condition, not a usable database

Empty
    a valid v2 schema has complete immutable (network_id, genesis_hash)
    binding and db_pp_blue_score = 0, with no PP or processing data

Initialized
    a compatible schema has coherent PP, PP score, and materialized VSPC sink;
    every block row satisfies the semantic Materialized invariant

Inconsistent
    schema and binding are valid, but processing contents, including any
    detected retained-past-closure violation, require Rebuild

Rejected
    network identity mismatch or unsupported, newer, v1, partial, or unknown
    schema
```

StorageService may connect, acquire its ownership lock, inspect contents, and
perform authorized preparatory work while the database is `Uninitialized`.
None of those actions turns it into `Empty`. Only one atomic initialization
transaction supplied with the validated node's `(network_id, genesis_hash)`
may cross the semantic `Uninitialized -> Empty` boundary and publish a usable
database. A crash rolls that transaction back to `Uninitialized`; it cannot
expose a partially bound `Empty` database. A structurally valid initialized
schema and binding with incoherent processing data is `Inconsistent`, while a
partial schema or binding is `Rejected`.

`--initialize-db` initializes only `Uninitialized`. It is idempotent for every
compatible existing database: retain `Empty`, `Initialized`, or `Inconsistent`
state without erasing data or changing its network binding. An `Inconsistent`
database remains usable only for Rebuild and is not misclassified as `Empty`.
Without the flag, first initialization requires interactive confirmation;
noninteractive startup fails with `InitializationConfirmationRequired` and
identifies `--initialize-db` as the unattended authorization.

The CLI confirmation identifies the database using safe endpoint information,
shows the exact network type and suffix plus the RPC-discovered Genesis hash,
and states that ordinary Rebuild cannot change that binding. It never displays
database credentials. `--initialize-db` is the explicit unattended equivalent;
a generic `--yes` does not authorize initialization. `--clear-db` expresses
stronger processing-data replacement intent and also authorizes first
initialization when the database is genuinely Uninitialized, but it never
authorizes rebinding or claiming an unknown, partial, or unsupported schema.
The CLI/bootstrap layer owns terminal interaction. StorageService owns state
detection, locking, revalidation, and the initialization transaction and never
reads standard input itself.

An existing database is never silently rebound to the CLI network, the
validated node's Genesis, or reset. A mismatch in either immutable binding
field is rejected rather than repaired through Rebuild.
`--clear-db` requests processing-data Rebuild under the existing compatible
network binding. Administrative reinitialization follows the separate settled
contract below.

StorageService acquires a dedicated PostgreSQL session advisory lock before
initialization, migration, or validation and holds it throughout the
validated-client lifetime. It rechecks state under that lock before first
initialization. Losing the lock connection retires the current processing
and API generations and emits both exact retirement events; the processing
lifecycle alone owns termination of a session using the processing generation.
No replacement generation is published until StorageService reconnects and
reacquires the lock. Failure to acquire the lock enters terminal
`Rejected(DatabaseAlreadyInUse)` and emits the corresponding `Rejected` event.
Process-local mutation guards and lane locks cannot replace this database-wide
ownership proof.

Schema migration versioning belongs to the selected migration framework's own
table, outside `NodeMetadata`. Under the advisory lock and before publishing a
validated client:

- an absent schema follows the authorized first-initialization path;
- a supported older v2 schema migrates through ordered, embedded, forward-only
  migrations and is fully revalidated afterward;
- the current schema is validated and published without migration;
- a schema newer than the binary is `Rejected(SchemaTooNew)`; and
- a v1 or otherwise unsupported schema is `Rejected(UnsupportedSchema)`.

Every ordinary migration is transactional. A failed migration publishes no
validated client. Automatic down migration, migration during a processing
session, online migration, and in-place v1-to-v2 migration are forbidden. A
migration that cannot complete transactionally is rejected by ordinary
startup.

The correctness metadata, called **node metadata**, is conceptually:

```rust
struct NodeMetadata {
    network_id: NetworkId,
    genesis_hash: BlockHash,
    db_pp_blue_score: u64,
}
```

`network_id` and `genesis_hash` form the immutable database network binding.
`NetworkId` includes the network type and suffix, and both are compared
exactly. No field is nullable and a partially bound `NodeMetadata` is invalid.
`db_pp_blue_score` is zero in an `Empty` database and in an initialized
Genesis-anchored database. PP presence distinguishes those states. For any
other initialized database it is the retained PP's blue score. First
initialization writes zero, `rebuild_from_pruning_point` replaces it atomically,
and ordinary block or VSPC processing never updates it.
`ValidatedDbClient` exposes the immutable `NodeMetadata` read from its exact
database generation for session binding checks.

NodeService never writes PostgreSQL. StorageService writes complete
`NodeMetadata` atomically during first initialization or administrative
reinitialization.
`rebuild_from_pruning_point` changes only `db_pp_blue_score` within its complete
processing-data replacement transaction. Observational node information is not
persisted.

The database PP is located by `(level=1, slot=0)`, never by assuming ID 1 or
the minimum ID. DB PP hash, committed VSPC sink, boundary seal threshold, and
PP-boundary phase are derived from persisted graph state, current consensus
parameters, or current-session state rather than duplicated in metadata. No
database generation or epoch is persisted; validated-client lifetime provides
that boundary.

Node server and RPC versions, node endpoint, current node PP and IBD state, and
consensus parameters are observational runtime information and are not stored
in `NodeMetadata`. They do not participate in database compatibility: only the
immutable `(network_id, genesis_hash)` binding does. The processing lifecycle
does use the current node PP and the current session's consensus parameters for
reconciliation and recovery without persisting them as node metadata.
The node-version observation used by ApiService comes through the
[NodeService status contract](node-service.md#nodeservice--settled). It is not
`NodeMetadata` and is not persisted in PostgreSQL.

An initialized database whose PP hash equals `NodeMetadata.genesis_hash` is a
valid Genesis anchor only when all of these invariants hold:

- the PP is materialized at `(level=1, slot=0)`, is in VSPC, and
  `NodeMetadata.db_pp_blue_score` is zero;
- the committed materialized VSPC sink exists and is coherent;
- Genesis has ORIGIN as its non-null selected-parent identity and has zero
  actual direct parents; and
- ORIGIN is the only `BoundaryIdentity`.

An extra boundary identity or another violation of these processing
invariants classifies the database as `Inconsistent` and requires Rebuild.

The coherent processing-state checks also require:

```text
Empty:
    block_identifiers, blocks, parents, and levels are all empty
    no PP and no committed materialized VSPC sink exist

Initialized:
    PP, db_pp_blue_score, levels(1), and a committed materialized VSPC sink
    all exist and agree
```

A PP without its level, a PP whose blue score disagrees with
`db_pp_blue_score`, a nonzero `db_pp_blue_score` without a PP, materialized
blocks without a PP, a missing committed sink, or an identity/materiality
violation is `Inconsistent`, not `Uninitialized`. A zero score without a PP is
valid only when every processing table is empty as required by `Empty`.
Unknown tables, v1, and partial v2 schemas are rejected rather than silently
claimed.

### Storage failure classification

| Condition | Storage classification |
|---|---|
| Database temporarily unreachable | `Unavailable`; retry connection under the service backoff policy |
| Validated pool or advisory-lock connection fails | `ServiceGenerationLost(Storage)` under the connection-level outcome contract above |
| Database already owned by another KGI | `Rejected(DatabaseAlreadyInUse)` |
| Immutable `(network_id, genesis_hash)` mismatch | `Rejected(NetworkMismatch)` |
| Schema newer than the binary | `Rejected(SchemaTooNew)` |
| Unsupported, v1, partial, or unknown schema | `Rejected(UnsupportedSchema)` |
| Compatible migration fails | Publish no validated client; report the typed startup storage failure |
| Processing contents are `Inconsistent` | Keep the compatible database usable for Rebuild; never permit Resync |
| Rebuild commit outcome is ambiguous | `Persistence(AmbiguousCommit)` under the transaction-outcome contract above |
| Domain invariant violation inside an operation | Typed operation/session failure for the owning caller |

## Administrative reinitialization — settled

Administrative reinitialization is a schema-lifecycle operation, not a
`RecoveryMode`. It is the only operation allowed to destroy a recognized KGI
schema, replace its migration history, and change the immutable network
binding. It has two entry forms:

```text
kgi database reinitialize --network-id <network> [--yes]
--reinitialize-db-token=<token>
```

The separate `database reinitialize` command is the preferred operator path.
Without `--yes`, it displays the database identity, existing binding if any,
the new exact `(network_id, genesis_hash)` binding, and the loss of all KGI
data and migration history, then requires interactive confirmation. It never
displays database credentials. A noninteractive invocation without `--yes`
fails rather than waiting for input. The command runs once and exits. `--yes`
is accepted only by this one-shot command; normal service startup has no
schema-reinitialization option.

Both entry forms obtain the target Genesis hash from a validated RPC client.
The CLI `NetworkId` must match that client's exact network type and suffix.
Before replacing anything, the operation:

1. acquires the database advisory lock;
2. runs before any `ValidatedDbClient` is published;
3. reinspects the database while holding the lock; and
4. accepts only an empty Uninitialized database or a recognized KGI v1/v2
   schema, never unknown user tables or an unrecognized partial schema.

One transaction removes the recognized KGI schema and data, installs the
latest v2 schema, writes complete `NodeMetadata` with the validated
`(network_id, genesis_hash)` and `db_pp_blue_score = 0`, leaves every processing
table empty, and records the supplied reinitialization token when applicable.
Definite commit produces a coherent network-bound `Empty` database. Rollback
leaves the previous database intact, and an ambiguous commit retires the
storage generation without reporting successful reinitialization. PostgreSQL
database ownership and configuration remain outside the replaced KGI schema.

Reinitialization is never triggered automatically by Resync, Rebuild,
inconsistent processing contents, corruption detection, or migration failure.
After the one-shot command exits, ordinary startup observes `Empty` and
requires Rebuild from the validated node's pruning point.

The declarative token supports persistent Docker, Compose, systemd, and
similar configuration without repeating destruction on every restart. Its
state is administrative metadata separate from `NodeMetadata`:

```rust
struct AdministrativeMetadata {
    last_reinitialization_token: Option<String>,
}
```

Its exact-match semantics are:

```text
no token supplied:
    never force reinitialization

token T supplied and stored token == T:
    continue ordinary startup without clearing anything

token T supplied and stored token != T:
    reinitialize once and atomically store T in the replacement schema
```

If an authorized first initialization receives a token, it stores that token
with the new schema; the token does not replace the separate authorization
required for `Uninitialized -> Empty`. Changing the configured token requests
one new reset. Restarting with the same token is idempotent. The token is not a
database generation and never participates in compatibility, reconciliation,
or recovery decisions.

## Persistent representation — settled

The [domain model](domain-model.md#identity-and-materiality-vocabulary--settled)
defines `Absent`, `BoundaryIdentity`, `Materialized`, and
the retained-past invariant included in Materialized. Storage represents
identity separately from materialized graph data:

```text
block_identifiers: BlockHash -> CompactId
blocks: materialized block data keyed by CompactId
```

Ordinary unresolved orphan hashes are never persisted as identifier-only
rows. Only references classified outside the retained PP boundary can become
permanent boundary identities, and such identities are never promoted into
`blocks`. PP bootstrap persists synthetic ORIGIN as one of these identities
when it is the PP's selected parent; `blocks.selected_parent_id` remains
non-null without inventing an actual Genesis parent.

The conceptual SQL schema is:

```sql
block_identifiers(
    id BIGINT PRIMARY KEY,
    hash BYTEA UNIQUE NOT NULL CHECK (octet_length(hash) = 32)
);

blocks(
    id BIGINT PRIMARY KEY REFERENCES block_identifiers(id),
    timestamp BIGINT NOT NULL,
    daa_score BIGINT NOT NULL
        CHECK (daa_score >= 0 AND daa_score < 9223372036854775807),
    level BIGINT NOT NULL CHECK (level > 0),
    slot BIGINT NOT NULL CHECK (slot >= 0),
    selected_parent_id BIGINT NOT NULL REFERENCES block_identifiers(id),
    color ... NOT NULL,
    is_in_vspc BOOLEAN NOT NULL,
    blue_merge_set BIGINT[] NOT NULL,
    red_merge_set BIGINT[] NOT NULL,
    UNIQUE (level, slot)
);

levels(
    level BIGINT PRIMARY KEY CHECK (level > 0),
    size BIGINT NOT NULL CHECK (size > 0),
    daa_score BIGINT NOT NULL DEFAULT 9223372036854775807
        CHECK (daa_score >= 0)
);

parents(
    child_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    child_level BIGINT NOT NULL,
    child_slot BIGINT NOT NULL,
    parent_level BIGINT NOT NULL,
    parent_slot BIGINT NOT NULL,
    PRIMARY KEY (child_id, parent_id),
    CHECK (child_level > 0),
    CHECK (child_slot >= 0),
    CHECK (parent_level >= 0),
    CHECK (parent_slot >= 0),
    CHECK (parent_level > 0 OR parent_slot = 0)
);
```

`blocks.timestamp` stores the complete domain-owned `Timestamp` in `BIGINT` by
preserving its exact 64-bit pattern. Writing reinterprets `u64` as `i64`; reading
reinterprets the stored `i64` as `u64`. Values through `i64::MAX` therefore
retain their ordinary positive representation, while larger informational
values appear negative only inside PostgreSQL and still round-trip exactly.
Storage never applies signed ordering or arithmetic to this column, and a
negative stored timestamp is not database inconsistency.

`parents.child_id` and `parents.parent_id` intentionally have no foreign keys.
The materialization transaction validates their identities and embeds both
endpoint coordinates. An outside-boundary parent uses the sentinel `(0,0)`.
`levels.size` is the number of allocated slots at the level.

There is no persistent `materialized` flag. In a processing-valid database
generation, a `blocks` row is the persistent representation of the semantic
Materialized state because PP bootstrap and every later insertion establish
retained-past closure. A row observed in unvalidated or Inconsistent contents
cannot be relied on by processing. Processors cannot begin against
Inconsistent contents; Rebuild atomically replaces them first. Ordinary
processing never deletes individual blocks or identities, and permanent
boundary identities are never promoted.
The parent table deliberately has neither foreign keys nor cascade deletion.
Merge-set arrays likewise have no element-level foreign keys; their IDs are
validated transactionally. `UNIQUE(level, slot)` is the final coordinate
backstop, and `levels.size` changes atomically with block insertion.

`levels.daa_score = i64::MAX` means that the level has no current VSPC block.
New levels use this sentinel, not zero. At most one current VSPC block occupies
a level, but a reorg can leave an existing level without one.

The database representation enforces the score ranges owned by the
[domain model](domain-model.md#shared-value-types--settled). The node-metadata
column for `db_pp_blue_score` is a nonnegative `BIGINT`; its signed upper bound
is `MAX_BLUE_SCORE`. Bind domain scores only through checked `i64::try_from`
conversion, and reject negative SQL values before converting them to `u64`.
Storage APIs defensively reject an out-of-range caller value as typed
`StorageError::ScoreOutOfRange(DaaScore)` or
`StorageError::ScoreOutOfRange(BlueScore)` before opening a mutation
transaction; the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns its fault disposition.

During database validation, a compatible bound schema containing a negative
score, the no-VSPC sentinel in `blocks.daa_score`, or another score outside its
semantic range is `Inconsistent` and is usable only for Rebuild. A current
schema's checks prevent KGI from creating such contents.

The committed materialized VSPC sink is derived as the maximum-ID materialized
block with
`is_in_vspc = true` and returned with its ID, hash, selected-parent hash, and
stored DAA score. Resolve the non-null `blocks.selected_parent_id` through
`block_identifiers`; for Genesis this yields synthetic ORIGIN.

Per-block blue work and blue score are not stored. `db_pp_blue_score` is the
only persisted blue-score metadata. The
[processing lifecycle](processing-lifecycle.md) owns node-header enrichment
and validation when constructing a `MaterializedSyncAnchor`.

## Caches and identity resolution — settled

The architecture fixes cache contents and publication rules rather than the
cache library.
Exact cache capacities remain deferred in the
[decision register](../decisions/deferred.md).

```rust
struct CachedIdentity {
    id: CompactId,
    materialized: bool,
}

enum ResolveMaterializedIdsError {
    Storage(StorageError),
    NonMaterialized {
        missing: Arc<[BlockHash]>,
        identity_only: Arc<[BlockHash]>,
    },
}

impl ValidatedDbClient {
    async fn block_presence(
        &self,
        hash: BlockHash,
    ) -> Result<BlockPresence, StorageError>;

    async fn resolve_materialized_ids(
        &self,
        hashes: &[BlockHash],
    ) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError>;
}
```

Caches contain:

- `BlockHash -> CachedIdentity`;
- `CompactId -> BlockCoordinate`; and
- `CompactId -> MergeSets`.

Do not cache negative identity results, mutable colors, or VSPC membership.
Cold identity batches may left-join `blocks` to obtain the materialized bit.
Publish cache entries only after the corresponding definite commit.

`block_presence` distinguishes all three shared `BlockPresence` states without
creating an identity. During an active processing run its
`BlockPresence::Materialized` result is an authoritative materiality result,
not a weaker row-existence result. The run cannot invoke it against
pre-Rebuild Inconsistent contents.

`resolve_materialized_ids` takes one ordered hash batch and returns one ID per
input position, including repeated hashes. It resolves cache hits, performs at
most one SQL read for all misses, and never writes, interns, or promotes
identities. Its `NonMaterialized` error reports absent and boundary-identity
hashes separately; only materialized hashes succeed.

## Rebuild transaction — settled

The sole processing-recovery API allowed to clear processing data is:

```rust
impl ValidatedDbClient {
    async fn rebuild_from_pruning_point(
        &self,
        pruning_point: ValidatedNodeBlock,
    ) -> Result<MaterializedSyncAnchor, StorageError>;
}
```

The pruning point is mandatory. Administrative schema reinitialization is a
separate [schema-lifecycle operation](#administrative-reinitialization--settled)
and is never implemented through this method. There is no processing-data
clear primitive without a pruning point.

### Preconditions and exclusive access

Before calling the method, the processing lifecycle has established all of
these conditions:

- ResyncEngine is executing `RecoveryMode::Rebuild` with one fixed
  `Arc<ValidatedRpcClient>` and this exact `Arc<ValidatedDbClient>`;
- the validated RPC `(network_id, genesis_hash)` equals this database
  generation's immutable `NodeMetadata` binding;
- the complete pruning-point block was obtained and validated through that RPC
  generation;
- both processors have completed Deactivate and no earlier processing session
  retains either validated client; and
- StorageService still owns the database advisory lock through its dedicated
  lock connection.

The method first establishes StorageService's API-read replacement exclusion
above. It then acquires the exclusive processing mutation guard without
waiting. Failure to acquire that guard after completed processor deactivation
is a lifecycle invariant violation; Rebuild must not wait behind an unexpected
processor mutation.

### Atomic replacement

One database transaction performs the complete replacement:

1. Clear `parents`, `blocks`, `levels`, and `block_identifiers`, and replace the
   PP-derived `db_pp_blue_score` within the same transaction.
2. Restart Compact-ID allocation.
3. Intern pruning-point hashes in the universal order:

   ```text
   red merge-set hashes
   blue merge-set hashes
   direct-parent hashes
   selected-parent hash
   pruning-point hash
   ```

   Deduplicate hashes by first occurrence before insertion.
4. Represent every retained-boundary-external pruning-point reference as a
   permanent `BoundaryIdentity`. Do not create placeholder `blocks` rows for
   parents, merge-set members, or other references.
5. Materialize the pruning point itself with:

   ```text
   coordinate  = (level=1, slot=0)
   color       = Gray
   is_in_vspc  = true
   ```

   Its block row stores timestamp, DAA score, the non-null selected-parent ID,
   and the ordered blue and red merge-set IDs.
6. Insert each actual direct-parent relation using the parent's interned ID and
   the outside-boundary coordinate sentinel `(0,0)`. The child coordinate is
   the pruning point's `(1,0)`.
7. Create `levels(level=1, size=1)` with the pruning point's DAA score because
   the pruning point is the current VSPC block at that level.
8. Store `NodeMetadata.db_pp_blue_score = pruning_point.blue_score` without
   changing the immutable network binding.
9. Commit all processing data and PP-derived metadata atomically.

Only a fully committed processing generation becomes visible.

Immediately after definite commit:

```text
database PP          = block at (level=1, slot=0)
committed VSPC sink  = pruning point
levels(1).size       = 1
```

For a Genesis pruning point, the bootstrap path stores synthetic ORIGIN as the
non-null selected-parent identity while storing zero actual direct parents.
ORIGIN is not a direct parent. Genesis never enters the ordinary BlockProcessor
or `PersistedBlock` delivery path.

### Return value

After definite commit, the method returns the current shared
`MaterializedSyncAnchor` populated as:

```rust
MaterializedSyncAnchor {
    point: VspcPoint {
        consensus_order: ConsensusOrder {
            blue_work: pruning_point.blue_work,
            hash: pruning_point.hash,
        },
        id: pruning_point_id,
    },
    selected_parent: pruning_point.selected_parent,
    blue_score: pruning_point.blue_score,
}
```

For Genesis, `selected_parent` is the ORIGIN hash. The anchor does not duplicate
the block hash outside `ConsensusOrder` and does not carry its storage
coordinate.

### Preserved state

The immutable `(network_id, genesis_hash)` binding and schema/migration state
survive. Rebuild atomically replaces only `db_pp_blue_score` with the supplied
pruning point's score and replaces only processing data. It cannot rebind the
database, replace its schema, change PostgreSQL ownership or configuration, or
perform administrative reinitialization.

An `Empty` or `Inconsistent` database becomes `Initialized` only through the
definite commit of this transaction. Rebuild of an already `Initialized`
database uses the same complete replacement.

### Cache publication

Only after definite commit, the current `ValidatedDbClient`:

1. clears its identity, coordinate, and merge-set caches;
2. seeds every newly interned pruning-point reference as
   `{ id, materialized: false }`;
3. seeds the pruning-point identity as `{ id, materialized: true }`; and
4. seeds the pruning-point coordinate and immutable merge sets.

No replacement cache state is visible before commit. A failed or ambiguous
attempt publishes none of these cache changes.

### Boundary state after commit

No database boolean records PP-boundary sealing. For a non-Genesis pruning
point, the transaction establishes the boundary but records no seal threshold
or processing phase. ResyncEngine owns
[threshold construction](processing-lifecycle.md#boundary-seal-threshold-construction),
and the [BlockProcessor contract](block-processing.md#pp-boundary-phase-behavior--settled)
owns local phase behavior. The Genesis transaction likewise stores no phase or
threshold; its committed boundary is intrinsically sealed.

Storage does not emit `PpBoundarySealed`. A successful rebuild commit alone
does not consume Supervisor's retained Rebuild requirement; the processing
lifecycle owns the subsequent exact-once milestone and recovery-intent change.
A definite successful rebuild replaces the database contents and publishes the
new processing caches within the same `ValidatedDbClient` generation. It emits
no processing-generation retirement or publication event.

### Failure outcomes

The complete-transaction retry policy above applies only to its authorized
SQLSTATEs. Each retry repeats the entire atomic replacement and publishes no
intermediate cache state.

A definite transaction failure rolls back to the previous database contents,
publishes no replacement cache state, and returns the typed database error.

If commit acknowledgement is lost, the outcome is ambiguous. The method does
not report success, publish cache changes, or retry blindly.
The operation returns `Persistence(AmbiguousCommit)`. StorageService retires
the current `ValidatedDbClient`, emits `ProcessingDbRetired`, and derives the
replacement generation from database truth. The processing lifecycle owns
session termination, the retained Rebuild obligation, and the resulting
recovery disposition.

The [processing lifecycle](processing-lifecycle.md) owns when this operation
may start. Storage owns both API-read exclusion and the atomic replacement.

## Block materialization transaction — settled

```rust
struct ParentCommitted {
    hash: BlockHash,
    coordinate: Option<BlockCoordinate>, // None at outside-PP boundary
}

struct LevelCommitted {
    level: u64,
    size: u64,
    daa_score: Option<u64>,
}

struct BlockCommitted {
    id: CompactId,
    hash: BlockHash,
    coordinate: BlockCoordinate,
    timestamp: Timestamp,
    daa_score: u64,
    selected_parent_index: Option<u32>,
    direct_parents: Arc<[ParentCommitted]>,
    blue_merge_set: Arc<[BlockHash]>,
    red_merge_set: Arc<[BlockHash]>,
    color: BlockColor,
    is_in_vspc: bool,
    level_snapshots: Arc<[LevelCommitted]>,
}

enum ReferencePolicy {
    AllowBoundaryIdentities,
    RequireMaterialized,
}

enum MaterializeBlockOutcome {
    Inserted {
        committed: BlockCommitted,
    },
    AlreadyMaterialized {
        id: CompactId,
        coordinate: BlockCoordinate,
    },
}

enum MaterializeBlockError {
    Storage(StorageError),
    IncomingBoundaryIdentity {
        hash: BlockHash,
    },
    NonMaterializedReferences {
        missing: Arc<[BlockHash]>,
        identity_only: Arc<[BlockHash]>,
    },
}

impl ValidatedDbClient {
    async fn materialize_block(
        &self,
        block: ValidatedNodeBlock,
        policy: ReferencePolicy,
    ) -> Result<MaterializeBlockOutcome, MaterializeBlockError>;
}
```

The shared [validated node block](domain-model.md#shared-value-types--settled)
is hash/consensus-level input and contains no DB ID, level, or slot. Storage
persists its hash, selected parent, direct parents, merge sets, timestamp, and
DAA score; blue score and blue work remain available to processing but are not
duplicated in the block row. Storage owns transactional ID resolution,
coordinate allocation, initial color, persistence, and construction of the
`BlockCommitted` value for a new insertion. Its `id` is the inserted block's
committed `CompactId`.

The parent payload contains actual direct parents only. For every non-Genesis
block, `selected_parent_index` is `Some(index)`, the index is representable as
`u32` and in bounds for `direct_parents`, and that entry is the block's selected
parent. Its coordinate can be `None` at the outside-PP boundary. Genesis has an
empty `direct_parents` payload and `selected_parent_index = None`. Synthetic
ORIGIN is Genesis's persisted selected-parent identity, but it is never inserted
into `direct_parents` or emitted as a graph parent. The payload therefore
identifies the selected-parent coordinate without duplicating its hash.

`level_snapshots` is non-repeating by level and contains complete post-commit
snapshots for the inserted block's level, always, and every distinct
materialized direct-parent level needed by its emitted edges. It contains no
entry for an outside-boundary parent. `None` represents storage's no-VSPC DAA
sentinel. The collection includes parent-level context even when the
materialization transaction did not mutate that parent level; it is therefore
not named or interpreted as a change list.

For every materialization, intern hashes in this order:

1. red merge-set hashes;
2. blue merge-set hashes;
3. direct-parent hashes;
4. selected-parent hash; and
5. the block's own hash.

Deduplicate by first occurrence within this sequence. Validate the
database-relative conditions atomically:

- semantic Materialized state for every retained reference under the selected
  policy;
- an already materialized own hash as a dedup outcome returning its ID and
  coordinate; and
- an own hash already classified as a permanent boundary identity as an
  explicit typed materiality violation.

`RequireMaterialized` requires every reference to be materialized and creates
no boundary identities. Before mutation, it classifies every nonmaterialized
reference and returns one `NonMaterializedReferences` result containing all
absent hashes in `missing` and all permanent boundary identities in
`identity_only`. The arrays are disjoint, contain each hash once, and preserve
first occurrence in the universal interning order above. The transaction rolls
back and publishes no cache state.

An own hash already represented by a permanent boundary identity takes
precedence and returns `IncomingBoundaryIdentity`; it is not included among
reference results. `AllowBoundaryIdentities` instead accepts existing boundary
references and may intern absent references as permanent outside-boundary
identities. When the own hash is admissible, it materializes the incoming block
rather than leaving it partial; it does not return
`NonMaterializedReferences`.

This establishes the invariant inductively. The rebuild pruning point is the
base: every nonretained reference is a permanent boundary identity. For each
later insertion, every retained reference is already Materialized and
therefore carries its own closed retained past; any newly permitted
`BoundaryIdentity` is an outside-boundary leaf. A successful insertion extends
that closure to the new block. A dedup consumes the same invariant already
preserved by the processing-valid database generation.

The PP bootstrap path handles the validated Genesis ORIGIN exception
separately. Ordinary `materialize_block` never receives Genesis.

Coordinate allocation is:

```text
no materialized direct parent:
    level = 1
    slot = next allocated slot at level 1

one or more materialized direct parents:
    level = max(materialized-parent levels) + 1
    slot = next allocated slot at that level
```

PP occupies `(level=1, slot=0)`. Retained PP-anticone roots with no
materialized parents occupy level 1 slots 1+. Other retained PP-anticone
blocks can have materialized anticone parents and follow the normal
parent-derived rule. "PP-anticone root" names only the first category.

Slot allocation is atomic, conceptually:

```sql
INSERT INTO levels(level, size, daa_score) VALUES ($1, 1, $no_vspc)
ON CONFLICT(level)
DO UPDATE SET size = levels.size + 1
RETURNING levels.size - 1 AS slot, levels.size, levels.daa_score;
```

A newly inserted block starts `Gray` and outside VSPC. Parent rows contain
each actual materialized coordinate or the outside-boundary sentinel `(0,0)`.
A newly created level starts with the no-VSPC DAA sentinel. Materialization at
an existing level preserves that level's current DAA score while increasing its
size.

For a new insertion, construct `BlockCommitted` inside the same transaction
from the validated block, its allocated coordinate, and every direct parent's
resolved storage state. A materialized parent has `Some(coordinate)` and its
complete level state appears once in `level_snapshots`; an outside-boundary
parent has `None` and contributes no level snapshot. Preserve the validated
direct-parent sequence, record its selected-parent index, and include the
persisted initial color and VSPC membership. The inserted block's resulting
level snapshot reflects every size or DAA-score value stored by the transaction.
Return the `Inserted` outcome, including that complete payload, only after
definite commit. The payload therefore describes the same committed state as
the insertion; no post-commit projection read is allowed. The sequential
materialization lane returns definite inserted outcomes in increasing
`BlockCommitted.id` order.

An already materialized own hash returns `AlreadyMaterialized` with its ID and
coordinate. It carries no `BlockCommitted` because deduplication creates no
graph mutation. Either outcome is an authoritative materiality result.
Processing owns delivery of the returned graph-update payload, any resulting
`PersistedBlock`, and the PP-boundary phase transition.

## Atomic VSPC transaction — settled

```rust
struct VspcCommitOutcome {
    destination: VspcPoint,
    level_snapshots: Arc<[LevelCommitted]>,
}

impl ValidatedDbClient {
    async fn apply_vspc_change(
        &self,
        change: ReadyVspcChange,
    ) -> Result<VspcCommitOutcome, StorageError>;
}

struct VspcPathConflict {
    child: BlockHash,
    expected_parent: BlockHash,
    stored_parent: BlockHash,
}

enum VspcMemberSetViolation {
    DuplicateChainMember,
    RemovedAddedIntersection,
}
```

Storage requires the supplied source to equal the currently committed sink.
Every block directly named in `removed` or `added` must be materialized; the
vectors contain no duplicates or intersection. As a defensive transaction
check, a violation returns the typed
`VspcMemberSetViolation(DuplicateChainMember)` or
`VspcMemberSetViolation(RemovedAddedIntersection)` before mutation. Storage
does not attribute that violation to an input source. Storage loads each added
block's merge sets internally. Before mutation it also loads the persisted
selected-parent identity for every directly named chain member and validates:

```text
removed is empty:
    selected_parent(added[0]) == source

removed is nonempty:
    removed[0] == source
    selected_parent(removed[i]) == removed[i + 1]
    selected_parent(removed.last()) == selected_parent(added[0])

for every added i > 0:
    selected_parent(added[i]) == added[i - 1]
```

Every admitted change has nonempty `added`, so each expression above is
defined. A failed `removed[0] == source` check returns the distinct typed
`VspcSourceDiscontinuity` storage error. For the first failed selected-parent
relationship in the order shown, return
`VspcPathDiscontinuity(VspcPathConflict)` before mutation. Storage supplies the
persisted evidence and expected relationship but does not attribute the
conflict to either the database or the candidate. The
[VspcProcessor contract](vspc-processing.md#readiness-and-materiality--settled)
owns that attribution.

The transaction applies, in order:

1. for removed blocks, set `is_in_vspc = false` and reset the relevant color
   to `Gray`;
2. for added blocks, set `is_in_vspc = true`;
3. for every added block in order, color materialized blue merge-set members
   `Blue`, then materialized red merge-set members `Red`; and
4. update every affected `levels.daa_score` to its final current-VSPC score or
   the `i64::MAX` sentinel after all removals and additions.

An identity-only merge-set member outside the retained boundary is ignored.
Applying red after blue resolves any merge-set overlap deterministically. A
remove/add sequence may temporarily alter and then restore a level score; only
the final value is persisted.

The outcome's `level_snapshots` is non-repeating by level and contains one
complete post-commit `LevelCommitted` for every distinct level whose final DAA
score the transaction evaluates in step 4. It contains final snapshots rather
than a change list: the consumer decides whether a supplied value changes its
projection. Construct the snapshots inside the same transaction and return
them only after definite commit. `None` represents storage's no-VSPC DAA
sentinel; `size` is the level's unchanged committed size.

The owned change is consumed so definite commit can return its destination
`VspcPoint` without cloning it. The result carries both the committed
destination and the authoritative final state of every affected level; no
post-commit projection read is allowed.

## Reconciliation snapshot — settled

```rust
enum ReconciliationState {
    Empty,
    NodePpNotMaterialized {
        hash: BlockHash,
    },
    Existing(ReconciliationSnapshot),
}

struct ReconciliationSnapshot {
    node_pp: StoredBlockPoint,
    db_pp: StoredBlockPoint,
    db_pp_blue_score: u64,
    committed_vspc_sink: StoredVspcSink,
}

struct StoredBlockPoint {
    hash: BlockHash,
    id: CompactId,
    coordinate: BlockCoordinate,
}

struct StoredVspcSink {
    hash: BlockHash,
    id: CompactId,
    selected_parent: BlockHash,
    daa_score: u64,
}

impl ValidatedDbClient {
    async fn reconciliation_snapshot(
        &self,
        current_node_pp: BlockHash,
    ) -> Result<ReconciliationState, StorageError>;
}
```

`reconciliation_snapshot` uses one read-only transaction to locate the
database PP exclusively at `(level=1, slot=0)`, read
`NodeMetadata.db_pp_blue_score`, derive the committed sink as the maximum-ID
materialized VSPC block, resolve its selected-parent hash, and verify that
the PP and sink are mutually coherent. For an initialized database it also
resolves the supplied current node PP as Materialized in that same snapshot.
The committed sink is Materialized by the database-generation invariant; the
snapshot supplies its stored point and selected parent.

Return `Empty` only for the coherent network-bound Empty state. Inconsistent
combinations return a typed `StorageError` rather than an incomplete snapshot.
Return `NodePpNotMaterialized` when the supplied hash is absent or
identity-only; this is reconciliation evidence rather than an operational
storage failure. A missing or incoherent committed sink is Inconsistent
storage content, not a weaker materiality state. `Existing` includes the
Materialized node PP as `node_pp` and the committed sink needed to construct
`MaterializedSyncAnchor`. The result contains no node-derived blue work or
blue score. ResyncEngine uses the run's exact validated RPC generation to
enrich and validate the stored sink before constructing the anchor.

## API graph projection reads — settled

The API-owned [database seed contract](api-publication.md#database-seed-extent-and-projection--settled)
defines `GraphViewSeedRequest`, `GraphViewSeedOutcome`, `GraphViewSeed`, and the
contents of the returned projection. The [public API values](api-protocol.md#public-api-values--settled)
define its anchors, resolution, and typed normal anchor-unavailability values.
Storage exposes that semantic read only through the separate read-only API
handle. Under the
[core crate structure](overview.md#core-crate-structure--settled),
these shared values come from `kgi-api-model`; `kgi-storage` does not depend on
`kgi-api-core`:

```rust
enum ApiReadError {
    QueryFailed,
    GenerationLost,
    InconsistentProjection,
}

impl ValidatedApiDbClient {
    async fn load_graph_view_seed(
        &self,
        request: GraphViewSeedRequest,
    ) -> Result<GraphViewSeedOutcome, ApiReadError>;
}
```

`load_graph_view_seed` resolves the requested anchor and materializes the
complete projection in one stable PostgreSQL snapshot, using one read-only
`REPEATABLE READ` transaction when more than one statement is required. It
also obtains the API-owned construction metadata, including the materialized-ID
cut, in that same snapshot. It finishes that transaction and releases its API
connection before response serialization or compression. The operation maps
stored identities, blocks, parents, merge sets, coordinates, levels, coloring,
VSPC membership, and construction metadata into the API-owned result. The
materialized-ID cut is the only compact ID exposed by this operation; no
database transaction object is exposed to ApiService.

`HeadPublication` returns `Loaded` or an `ApiReadError`; it has no anchor-miss
outcome. For a valid `AnchoredWindow`, resolve the anchor inside the same
stable transaction used for projection and return these normal outcomes before
constructing any partial seed:

- `LevelNotRetained { requested_level }` when no retained level row exists for
  that exact requested level;
- `BlockNotMaterialized { requested_hash }` when the hash is absent or resolves
  only to an identity without a materialized block row; and
- `NoRetainedDaaMatch { requested_score }` when the current-VSPC floor query
  finds no retained score at or below the requested score.

An anchor beyond the current VSPC DAA still resolves to the current VSPC level.
Successful resolution and the complete returned projection observe the same
snapshot. `AnchorUnavailable` is not `ApiReadError`, and no miss returns a
partial `GraphViewSeed`. `InconsistentProjection` applies only after an anchor
resolved and the requested complete projection proved structurally incoherent;
it never substitutes for an unavailable anchor.

A query failure that leaves the pool generation usable reports `QueryFailed`.
Loss of this API pool generation atomically retires that handle, emits the
ordered `StorageServiceEvent::ApiDbRetired`, starts autonomous reacquisition, and
reports `GenerationLost` to the operation without retiring the independent
processing handle. Publication of the validated replacement emits
`StorageServiceEvent::ApiDbPublished`. A structurally incomplete or internally
incoherent result reports `InconsistentProjection`; it never returns a partial
seed. The [ApiService construction contract](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled)
owns those errors during Head construction; the
[API protocol](api-protocol.md#anchored-graph-windows--settled) owns public
anchored-window dispositions.

## Historical read contracts — settled

For a query in the domain-owned DAA-score range, the indexed database DAA-floor
lookup selects the greatest current VSPC score not exceeding `q`, breaking
ties by the highest level:

```sql
SELECT level
FROM levels
WHERE daa_score <= $1
ORDER BY daa_score DESC, level DESC
LIMIT 1;
```

The sentinel is excluded naturally because no valid query reaches
`i64::MAX`. If no retained floor exists, return `NoRetainedDaaMatch`. A query
beyond the current VSPC DAA resolves to the current VSPC
level. Resolve a historical anchor and read its graph window in one consistent
database transaction, never from different revisions.
