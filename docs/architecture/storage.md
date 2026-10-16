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
generation and owns that generation's caches. Publication proves that
StorageService still owns the database, the schema is a recognized compatible
v2 schema, and the immutable `(network_id, genesis_hash)` binding is complete
and valid. It does not assert that the current processing contents are
coherent. The same single processing capability is published for compatible
`Empty`, `Initialized`, and `Inconsistent` contents; operation contracts admit
only the reads or mutations valid for the observed content state. No
`ValidatedDbClient` is published for `Uninitialized` or `Rejected` contents.
A processing run receives one exact client generation; StorageService never
rebinds the client underneath the run. A replacement generation starts with
fresh caches.

`ValidatedApiDbClient` is the storage handle for the read-only pool required by
the [API resource contract](api-service.md#resource-isolation-and-saturation--settled).
It exposes no processing mutation or processing cache. Each published handle
owns shared terminal validity state that is observed through every clone:

```rust
impl ValidatedApiDbClient {
    fn is_valid(&self) -> bool;
}
```

StorageService is the sole authority that changes this state. A newly
published handle is valid. Retirement changes it atomically to invalid before
the retirement event is emitted or an operation returns `GenerationLost`, and
an invalid handle can never become valid or be published again. Callers may
observe validity but cannot change it. Validity is distinct from read
readiness: a Rebuild replacement handle is valid and may be published while
the storage-owned replacement gate still prevents it from entering a database
transaction.
Network binding, schema, and generation validation precede publication of
either handle. StorageService autonomously opens, reconnects, validates,
retires, and republishes both pool generations. It reports every lifecycle
transition through one reliable ordered `StorageServiceEvent` stream. The
event path must be installed before StorageService can publish either initial
generation or enter `Rejected`.

`ProcessingDbPublished` and `ApiDbPublished` carry the newly validated exact
generation. A compatible `Inconsistent` database initially publishes its
single processing generation but no API generation. When Rebuild replacement
starts, StorageService keeps that processing generation and publishes a fresh
API generation behind the closed replacement gate. A definite successful
commit makes that same API generation readable; it does not publish another
one. `ProcessingDbRetired` and `ApiDbRetired` carry the exact generation
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

StorageService owns both generation lifecycles. Outside deliberate database
replacement, retirement immediately starts autonomous reconnection and
validation. Rebuild itself causes StorageService to create the gated API
generation described below; Supervisor, ProcessingSession, and ApiService never
request an API pool generation. Events report transitions; they do not
initiate them. Supervisor event handling and API forwarding belong to the
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
binding mismatch, a permanent migration failure, and unsupported, newer, v1,
partial, or unknown schema states enter terminal `Rejected` and do not retry
under the unchanged database and binary.

A connection-level storage failure retires the validated generation, but does
not retroactively revoke independent operations already in flight. The
[processing lifecycle](processing-lifecycle.md) owns the resulting session
termination. Retirement emits the applicable exact-generation event before a
replacement can be published. Each operation reports its actual outcome:

- a definitely committed transaction remains successful and authoritative;
- connection loss before commit with a proven rollback is
  `ProcessingDbError::GenerationLost`, inside the operation-specific wrapper
  when one exists, rather than a persistence-transaction fault; and
- a connection loss with an ambiguous commit outcome remains ambiguous and is
  reported as `ProcessingDbError::Persistence(AmbiguousCommit)`, inside the
  operation-specific wrapper when one exists.

The processing lifecycle maps the first result to
`ServiceGenerationLost(Storage)` and the second to
`Persistence(AmbiguousCommit)`; Storage owns the operation classification and
exact-generation retirement that precede those cross-worker faults.

All persistent mutations are transactional. Cache entries become visible
only after definite commit. Outside the database-replacement gate below, no
connection epoch, per-operation revocation check, cache generation, or global
operation-completion barrier is required.

### API read exclusion during database replacement — settled

```text
API_REPLACEMENT_DRAIN_TIMEOUT = 30 seconds
```

```rust
enum RebuildSetupFault {
    ApiGenerationPreparation,
    ApiReadExclusion,
}
```

`ApiGenerationPreparation` means that StorageService could not create and
validate the fresh read-only API generation required by this Rebuild attempt.
It does not include a failure that proves the current processing generation was
lost or a terminal database rejection; those retain their existing generation
loss or service-rejection classifications.

`ApiReadExclusion` means that the bounded cancellation and drain procedure
could not establish exclusive replacement access. An unexpectedly closed or
broken replacement-control primitive returns
`RebuildFromPruningPointError::ReplacementControlUnavailable { diagnostic }`.
This is an internal StorageService failure, not a recoverable setup fault and
not a persistence failure.

Every database phase performed through `ValidatedApiDbClient` holds a shared
database-replacement permit from before its read-only transaction begins until
the complete bounded projection has been detached into memory and the
transaction, connection, and permit have been released. Serialization,
compression, and response delivery occur outside this permit and cannot issue
follow-up database reads.

The replacement gate and its active-lease registry are owned by StorageService
and outlive individual `ValidatedApiDbClient` generations. Every API generation
holds only a shared reference to that same gate. Retiring a generation prevents
new database phases through it but does not remove a lease already held by an
admitted operation. Dropping a client reference neither resets the gate nor
removes such a lease. The gate retains no history of retired generations; it
retains only currently active database-read leases and their cancellation
paths until those leases are released.

Before `rebuild_from_pruning_point` clears or replaces processing data,
StorageService atomically closes new API DB admission. If a current
`ValidatedApiDbClient` generation exists, it retires that exact generation and
emits `StorageServiceEvent::ApiDbRetired(old)`. If none exists, including the
initial `Inconsistent -> Rebuild` path, it emits no retirement event and
creates no placeholder client. StorageService then creates and validates a
fresh read-only API pool generation and emits
`StorageServiceEvent::ApiDbPublished(fresh)`. The complete event sequence is
therefore `Retired(old), Published(fresh)` when an old generation exists and
only `Published(fresh)` when it does not.

The fresh generation is available for API construction as soon as it is
published, while remaining behind the closed gate. StorageService cancels and
boundedly drains every active database phase admitted before closure,
regardless of whether the client generation that admitted it is still current
or has already been retired. It acquires the exclusive replacement permit only
after all such leases have been released. From the cancellation request,
StorageService allows at most `API_REPLACEMENT_DRAIN_TIMEOUT` for those leases
to release and exclusive replacement ownership to be established. Expiry
returns `RebuildFromPruningPointError::Setup(ApiReadExclusion)`. Absence of a
current API generation does not skip this procedure and does not preclude
`ApiReadExclusion`; the drain is immediately complete only when the
service-owned active-lease registry is empty. A request that detached its
projection before closure may finish returning that old coherent image. Every
other affected old-generation request reports database-backed read
unavailability; the
[API protocol](api-protocol.md#anchored-graph-windows--settled) owns its HTTP
mapping. An API read cannot delay replacement without bound.

A non-cancellation setup failure starts no replacement transaction and returns
`RebuildFromPruningPointError::Setup(reason)`. For
`ApiGenerationPreparation`, no unvalidated API client is published. For
`ApiReadExclusion`, StorageService retires the already published fresh API
generation while its gate is still closed. Both outcomes leave the database,
processing caches, and current `ValidatedDbClient` generation unchanged. They
do not start an independent storage retry loop; the processing lifecycle owns
the complete-attempt retry.

Session or service cancellation during setup performs the same applicable API
generation cleanup and follows the ordinary cancellation path rather than
returning `RebuildFromPruningPointError::Setup`. If connection loss proves the
processing generation unusable, StorageService instead retires it and returns
`RebuildFromPruningPointError::Database(ProcessingDbError::GenerationLost)`;
the processing lifecycle maps that result to
`ServiceGenerationLost(Storage)`.

If replacement control becomes unavailable unexpectedly while StorageService
is required to operate, no replacement transaction starts. StorageService
keeps API database admission closed wherever the remaining control path permits,
retires the currently published API generation if one exists, and emits its
exact `ApiDbRetired` event before returning
`RebuildFromPruningPointError::ReplacementControlUnavailable`. It requests cancellation of
active API database phases on a best-effort basis but does not delay fault
reporting indefinitely through the broken control path. Database contents,
processing caches, and the current processing generation remain unchanged:
loss of API replacement control does not prove processing-generation loss.
The [processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns the resulting terminal component disposition.
If session or service cancellation has already been selected, the ordinary
cancellation path takes precedence over this fault.

The exclusive permit remains held through the atomic replacement outcome and
cache publication or generation retirement. A database phase started through
the newly published generation waits, without opening a transaction, for the
replacement gate to open or for that generation or its caller to be cancelled.
After definite commit and processing-cache publication, StorageService opens
the gate and those pending construction reads observe only the coherent
replacement contents. Publishing the generation does not open public
historical-request admission; that remains owned by the linked ApiService
lifecycle.

Session or service cancellation observed after the replacement transaction
has started must not drop or abort the replacement future. StorageService
continues to a definite success, definite failure, or ambiguous outcome and
performs the cache, generation, and replacement-gate publication or cleanup
owned by that outcome before the method completes. A definite success still
returns its coherent anchor even when the caller has requested cancellation;
the processing lifecycle decides whether to initialize processors or discard
that anchor. Definite and ambiguous failures retain their typed result rather
than becoming ordinary cancellation. This rule does not weaken the ordinary
cancellation behavior during setup before the transaction begins.

A final definite replacement failure rolls back the transaction and retires
the gated API generation before it can become readable. No replacement API
generation is published merely to expose the rolled-back contents; a later
Rebuild attempt creates its own fresh gated generation. An ambiguous commit
also retires that API generation, while the Rebuild failure contract below
additionally retires the processing generation and derives database truth
before another processing run.

If replacement uses PostgreSQL `TRUNCATE`, transactional rollback does **not**
make it generally MVCC-safe for pre-existing snapshots. The gate must ensure a
request returns one detached old image, one coherent replacement-generation
image, or an unavailable read outcome, never mixed or partially rebuilt
tables. See PostgreSQL's
[`TRUNCATE`](https://www.postgresql.org/docs/current/sql-truncate.html) and
[MVCC caveat](https://www.postgresql.org/docs/current/mvcc-caveats.html)
documentation.

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
gets exclusive mutation access after the predecessor processing session has
completed shutdown and before the Rebuild session constructs processors. Reads
remain concurrent. A materialization `LevelCommitted` is authoritative downstream for
its level's committed `size`; its `daa_score` is contextual evidence observed
by that transaction. A VSPC `LevelCommitted` is authoritative downstream for
its level's committed `daa_score`; its unchanged `size` is contextual evidence.
The [graph model](api-graph.md#graph-update-consumption--settled) owns how those
disjoint authorities are merged when independently ordered committed outcomes
reach one view. Storage does not serialize the two lanes merely to order their
contextual fields.

## Database bootstrap and validation — settled

Startup distinguishes these states:

```text
Uninitialized
    no complete KGI schema and immutable network binding exists; this is a
    transient storage/service initialization condition, not a usable database

Empty
    a valid v2 schema has a complete immutable (network_id, genesis_hash)
    binding, no ProcessingMetadata, and no PP or processing data

Initialized
    a compatible schema has valid ProcessingMetadata, a materialized PP, and a
    committed materialized VSPC sink sufficient to prepare Resync

Inconsistent
    schema and binding are valid, but the bounded processing-state inspection
    cannot construct the local values required to prepare Resync

Rejected
    network identity mismatch or unsupported, newer, v1, partial, or unknown
    schema, or a permanent migration failure prevents completion of a usable
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

The resolved persistent initialization authorization initializes only
`Uninitialized`. It is idempotent for every compatible existing database:
retain `Empty`, `Initialized`, or `Inconsistent` state without erasing data or
changing its network binding. An `Inconsistent` database remains usable only
for Rebuild and is not misclassified as `Empty`. Without that authorization,
first initialization requires interactive confirmation; noninteractive
startup fails with `InitializationConfirmationRequired` and identifies
`--initialize-db` as the unattended authorization.

The CLI confirmation identifies the database using safe endpoint information,
shows the exact network type and suffix plus the RPC-discovered Genesis hash,
and states that ordinary Rebuild cannot change that binding. It never displays
database credentials. The persistent initialization authorization is the
explicit unattended equivalent; a generic `--yes` does not authorize
initialization. `--clear-db` expresses
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
generation and, if one exists, the current API generation, then emits the
corresponding exact retirement event for each; the processing lifecycle alone
owns termination of a session using the processing generation.
No replacement generation is published until StorageService reconnects and
reacquires the lock. Failure to acquire the lock enters terminal
`Rejected(DatabaseAlreadyInUse)` and emits the corresponding `Rejected` event.
Process-local mutation guards and lane locks cannot replace this database-wide
ownership proof.

Schema migration versioning belongs to the selected migration framework's own
table, outside the binding and processing metadata. Under the advisory lock and before publishing a
validated client:

- an absent schema follows the authorized first-initialization path;
- a supported older v2 schema migrates through ordered, embedded, forward-only
  migrations and is fully revalidated afterward;
- the current schema is validated and published without migration;
- a schema newer than the binary is `Rejected(SchemaTooNew)`; and
- a v1 or otherwise unsupported schema is `Rejected(UnsupportedSchema)`.

Every ordinary migration is transactional. A failed migration publishes no
validated client. The migration adapter classifies an execution error caused
by connection loss, SQLSTATE `40001`, or SQLSTATE `40P01` as transient. The
complete startup and migration attempt enters `Unavailable` and retries under
the StorageService connection backoff; it does not use the local transaction
retry slots.

Every other migration-framework failure is permanent under the current
database and binary. This includes a dirty migration marker, a checksum or
version mismatch, a missing or invalid migration, migration-source failure,
and a nonretryable SQL execution failure. It is classified directly as:

```rust
StorageError::Rejected(StorageRejection::MigrationFailed {
    diagnostic: Arc<str>,
})
```

StorageService enters terminal `Rejected`, emits the typed `Rejected` event,
completes every pending initialization request with the same rejection, and
does not retry. No generic migration error may pass through `Unavailable`.
Successful later `shutdown()` reports completion of terminal cleanup; it does
not replace or erase the already emitted rejection. The
[processing lifecycle](processing-lifecycle.md#processing-session-and-resource-acquisition--settled)
owns the event's Fatal disposition.

Automatic down migration, migration during a processing session, online
migration, and in-place v1-to-v2 migration are forbidden. A migration that
cannot complete transactionally is rejected by ordinary startup.

The immutable network binding and mutable processing metadata are separate
semantic values with separate persisted rows:

```rust
struct DatabaseBinding {
    network_id: NetworkId,
    genesis_hash: BlockHash,
}

struct ProcessingMetadata {
    db_pp_blue_score: u64,
}
```

`DatabaseBinding` belongs to the database as a whole. `NetworkId` includes the
network type and suffix, and both fields are compared exactly. Neither field is
nullable. First initialization and administrative reinitialization write the
complete binding atomically; Rebuild and ordinary processing cannot change it.
Every `ValidatedDbClient` contains this validated value and exposes it through
`binding()` for session pairing.

`ProcessingMetadata` belongs to the replaceable KGI processing contents. It is
absent in `Empty`, present in coherent `Initialized`, and may be missing,
malformed, or inconsistent with the graph tables in `Inconsistent`. Its score
is zero for a Genesis-anchored initialized database and otherwise equals the
retained pruning point's blue score. Construct the semantic value only when the
stored score is representable as `u64`; a negative stored value is successful
evidence of `Inconsistent`, never a database-generation failure or an invented
score. `rebuild_from_pruning_point` replaces `ProcessingMetadata` atomically
with all processing tables. Ordinary block and VSPC processing never updates
it.

NodeService never writes PostgreSQL. Observational node information is not
persisted.

The database PP is located by `(level=1, slot=0)`, never by assuming ID 1 or
the minimum ID. DB PP hash, committed VSPC sink, boundary seal threshold, and
PP-boundary phase are derived from persisted graph state, current consensus
parameters, or current-session state rather than duplicated in metadata. No
database generation or epoch is persisted; validated-client lifetime provides
that boundary.

Node server and RPC versions, node endpoint, current node PP and IBD state, and
consensus parameters are observational runtime information and are not stored
in either metadata value. They do not participate in database compatibility:
only the immutable `(network_id, genesis_hash)` binding does. The processing
lifecycle uses the current node PP only to prepare Rebuild and uses the current
session's consensus parameters for recovery without persisting them.
The node-version observation used by ApiService comes through the
[NodeService status contract](node-service.md#nodeservice--settled). It is not
persisted in PostgreSQL.

An initialized database whose PP hash equals `DatabaseBinding.genesis_hash`
retains these storage invariants:

- the PP is materialized at `(level=1, slot=0)`, is in VSPC, and
  `ProcessingMetadata.db_pp_blue_score` is zero;
- the committed materialized VSPC sink exists and is coherent;
- Genesis has ORIGIN as its non-null selected-parent identity and has zero
  actual direct parents; and
- ORIGIN is the only `BoundaryIdentity`.

Storage mutations, transaction boundaries, relational constraints, and
write-time validation maintain the complete materiality, coordinate,
parent-relation, level, Genesis, and retained-past-closure invariants
inductively.

### Bounded processing-state classification — settled

Initial processing-state classification and `load_session_state` must remain
cheap, bounded operations. Each performs a fixed number of indexed point or
existence reads. Its database work is independent of the number of retained
blocks, identities, parent relations, levels, and revisions, and of the depth
of retained history.

The classifier may only:

1. read the singleton `ProcessingMetadata` value;
2. use `EXISTS` queries to distinguish fully empty processing tables from
   partial contents;
3. locate the database PP through the unique `(level, slot) = (1, 0)` index
   and resolve its identity and level through indexed point reads;
4. locate the committed materialized VSPC sink through
   `blocks_vspc_sink_idx` with `LIMIT 1` and resolve its identity and selected
   parent through indexed point reads; and
5. validate the scalar values returned by those reads while decoding them.

It must not scan all blocks, identities, parents, coordinates, or levels;
recount blocks per level; recompute level sizes; traverse retained-past
closure; reconstruct the parent graph; or perform any other work proportional
to retained graph size. A complete or proportional retained-graph integrity
audit during either classification operation is prohibited. Introducing one
is an architecture change, not an implementation detail or defensive
validation improvement.

The bounded processing-state checks require:

```text
Empty:
    block_identifiers, blocks, parents, and levels are all empty
    no ProcessingMetadata, PP, or committed materialized VSPC sink exists

Initialized:
    ProcessingMetadata, PP, levels(1), and a committed materialized VSPC sink
    all exist and agree
```

For these operations, `Inconsistent` is exhaustively limited to defects
observable through the bounded reads above:

- metadata exists while all processing tables are empty;
- processing contents exist without metadata;
- the metadata value cannot construct valid `ProcessingMetadata`;
- the database PP at `(1, 0)` is absent or cannot construct the required
  materialized PP;
- the PP's required level is absent;
- the Genesis PP representation conflicts with the Genesis-specific metadata
  rule;
- no committed materialized VSPC sink exists;
- the sink or its selected-parent identity cannot construct
  `StoredVspcSink`; or
- a required PP or sink scalar violates its semantic range.

A PP without its level, any ProcessingMetadata without a PP, processing data
without ProcessingMetadata, or a missing committed sink is therefore
`Inconsistent`, not `Uninitialized`. Unknown tables, v1, and partial v2 schemas
are rejected rather than silently claimed.

The classifier deliberately does not search for global corruption outside
those bounded reads. Storage transactions, insertion rules, relational
constraints, and write-time validation maintain the global invariants. If a
later storage operation encounters a violation, that operation reports its
ordinary typed invariant or persistence failure through the applicable session
disposition.

### Storage failure classification

| Condition | Storage classification |
|---|---|
| Database temporarily unreachable | `Unavailable`; retry connection under the service backoff policy |
| Validated pool or advisory-lock connection fails | Retire the exact generation; a processing operation returns `ProcessingDbError::GenerationLost`, and the processing lifecycle maps the retirement to `ServiceGenerationLost(Storage)` |
| Database already owned by another KGI | `Rejected(DatabaseAlreadyInUse)` |
| Immutable `(network_id, genesis_hash)` mismatch | `Rejected(NetworkMismatch)` |
| Schema newer than the binary | `Rejected(SchemaTooNew)` |
| Unsupported, v1, partial, or unknown schema | `Rejected(UnsupportedSchema)` |
| Migration execution loses its connection or reports SQLSTATE `40001` or `40P01` | Publish no validated client; enter `Unavailable` and retry the complete startup attempt under the service backoff |
| Migration has a dirty marker, checksum/version mismatch, missing or invalid migration, source failure, or other nonretryable failure | `Rejected(MigrationFailed { diagnostic })`; publish no validated client and do not retry |
| Processing contents are `Inconsistent` | Keep the compatible database usable for Rebuild; never permit Resync |
| Rebuild commit outcome is ambiguous | `Persistence(AmbiguousCommit)` under the transaction-outcome contract above |
| Domain invariant violation inside an operation | Typed operation/session failure for the owning caller |

## Administrative reinitialization — settled

Administrative reinitialization is a schema-lifecycle operation, not a
`RecoveryMode`. It is the only operation allowed to destroy a recognized KGI
schema, replace its migration history, and change the immutable network
binding. It has two entry forms. Their complete configuration sources and
command selection belong to the
[process configuration contract](overview.md#process-configuration-and-command-entry--settled):

```text
kgi database reinitialize [network selection] [--yes]
kgi [network selection] --reinitialize-db-token=<token>
```

The separate `database reinitialize` command is the preferred operator path.
Without `--yes`, it displays the database identity, existing binding if any,
the new exact `(network_id, genesis_hash)` binding, and the loss of all KGI
data and migration history, then requires interactive confirmation. It never
displays database credentials. A noninteractive invocation without `--yes`
fails rather than waiting for input. The top-level
[command-entry contract](overview.md#command-surface-and-startup-only-actions)
owns the command's one-shot lifecycle, option availability, and exit behavior.

Both entry forms obtain the target Genesis hash from a validated RPC client.
The resolved `NetworkId` must match that client's exact network type and suffix.
Before replacing anything, the operation:

1. acquires the database advisory lock;
2. runs before any `ValidatedDbClient` is published;
3. reinspects the database while holding the lock; and
4. accepts only an empty Uninitialized database or a recognized KGI v1/v2
   schema, never unknown user tables or an unrecognized partial schema.

One transaction removes the recognized KGI schema and data, installs the
latest v2 schema, writes the complete validated `DatabaseBinding`, leaves
`ProcessingMetadata` and every processing table empty, and records the supplied
reinitialization token when applicable.
Definite commit produces a coherent network-bound `Empty` database. Rollback
leaves the previous database intact, and an ambiguous commit retires the
storage generation without reporting successful reinitialization. PostgreSQL
database ownership and configuration remain outside the replaced KGI schema.

Reinitialization is never triggered automatically by Resync, Rebuild,
inconsistent processing contents, corruption detection, or migration failure.
A definitely committed reinitialization leaves `Empty`; a later ordinary
startup observes that state and requires Rebuild from the validated node's
pruning point.

The declarative token supports persistent Docker, Compose, systemd, and
similar configuration without repeating destruction on every restart. Its
state is administrative metadata separate from both binding and processing
metadata:

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
network_metadata(
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    network_id TEXT NOT NULL,
    genesis_hash BYTEA NOT NULL CHECK (octet_length(genesis_hash) = 32)
);

processing_metadata(
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    db_pp_blue_score BIGINT NOT NULL CHECK (db_pp_blue_score >= 0)
);

block_identifiers(
    id BIGINT PRIMARY KEY,
    hash BYTEA UNIQUE NOT NULL CHECK (octet_length(hash) = 32)
);

blocks(
    id BIGINT PRIMARY KEY REFERENCES block_identifiers(id),
    timestamp BIGINT NOT NULL,
    daa_score BIGINT NOT NULL
        CHECK (daa_score >= 0 AND daa_score < 9223372036854775807),
    level BIGINT NOT NULL CHECK (level > 0 AND level <= 4294967295),
    slot INTEGER NOT NULL CHECK (slot >= 0 AND slot <= 65534),
    selected_parent_id BIGINT NOT NULL REFERENCES block_identifiers(id),
    color ... NOT NULL,
    is_in_vspc BOOLEAN NOT NULL,
    blue_merge_set BIGINT[] NOT NULL,
    red_merge_set BIGINT[] NOT NULL,
    UNIQUE (level, slot)
);

levels(
    level BIGINT PRIMARY KEY
        CHECK (level > 0 AND level <= 4294967295),
    size INTEGER NOT NULL CHECK (size > 0 AND size <= 65535),
    daa_score BIGINT NOT NULL DEFAULT 9223372036854775807
        CHECK (daa_score >= 0)
);

parents(
    child_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    child_level BIGINT NOT NULL,
    child_slot INTEGER NOT NULL,
    parent_level BIGINT NOT NULL,
    parent_slot INTEGER NOT NULL,
    PRIMARY KEY (child_id, parent_id),
    CHECK (child_level > 0 AND child_level <= 4294967295),
    CHECK (child_slot >= 0 AND child_slot <= 65534),
    CHECK (parent_level >= 0 AND parent_level <= 4294967295),
    CHECK (parent_slot >= 0 AND parent_slot <= 65534),
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

PostgreSQL has no unsigned 32-bit integer, so graph levels use `BIGINT` while
the Storage boundary checks and converts the complete domain-owned `u32`
range. Slots and level sizes use `INTEGER` because PostgreSQL `SMALLINT` cannot
represent their domain-owned `u16` maxima. The SQL checks exactly enforce the
shared `MAX_LEVEL_SLOT` and `MAX_LEVEL_SIZE` bounds.

`parents.child_id` and `parents.parent_id` intentionally have no foreign keys.
The materialization transaction validates their identities and embeds both
endpoint coordinates. An outside-boundary parent uses the sentinel `(0,0)`.
`levels.size` is the number of allocated slots at the level.

Storage represents direct parents as child-parent relations, so one child has
at most one relation to a given parent. The shared `ValidatedNodeBlock` already
supplies canonical non-repeating direct-parent, blue-merge-set, and
red-merge-set sequences. Rebuild and ordinary materialization preserve those
sequences for reference classification, coordinate allocation, parent-row
insertion, persisted merge-set arrays, and `BlockCommitted`; Storage neither
retains the raw node sequences nor performs a second relationship-vector
canonicalization. The same hash may remain in multiple vectors.
The shared-value contract also guarantees that the incoming block's own hash
is absent from every relationship vector; Storage relies on that guarantee and
does not perform another own-reference check.

The reference-first interning order guarantees only that every distinct
relationship hash is allocated before the incoming block's own hash. Because
cross-vector overlap is valid, the selected parent can first occur in the
insertion sequence while interning a merge set or while interning direct
parents. No storage operation may infer that the selected parent has the
greatest reference ID, immediately precedes the block's ID, or has any other
arithmetic relation to it. Persisted `selected_parent_id` and canonical
direct-parent relations are the authorities for those relationships.

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
[domain model](domain-model.md#shared-value-types--settled). The
`processing_metadata.db_pp_blue_score` column is a nonnegative `BIGINT`; its signed upper bound
is `MAX_BLUE_SCORE`. Bind domain scores only through checked `i64::try_from`
conversion, and reject negative SQL values before converting them to `u64`.
Storage APIs defensively reject an out-of-range caller value as typed
`ProcessingDbError::ScoreOutOfRange(DaaScore)` or
`ProcessingDbError::ScoreOutOfRange(BlueScore)` before opening a mutation
transaction; the
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
owns its fault disposition.

During bounded processing-state inspection, a negative processing-metadata
score, the no-VSPC sentinel on the derived committed sink, or another inspected
anchor value outside its semantic range is `Inconsistent` and is usable only
for Rebuild. The current schema checks and write operations prevent KGI from
creating out-of-range values elsewhere; session loading does not scan every
stored score.

The committed materialized VSPC sink is derived as the maximum-ID materialized
block with
`is_in_vspc = true` and returned with its ID, hash, selected-parent hash, and
stored DAA score. Resolve the non-null `blocks.selected_parent_id` through
`block_identifiers`; for Genesis this yields synthetic ORIGIN.

Per-block blue work and blue score are not stored.
`ProcessingMetadata.db_pp_blue_score` is the only persisted blue-score
metadata. The
[processing lifecycle](processing-lifecycle.md) owns node-header enrichment
and validation when constructing a `MaterializedSyncAnchor`.

## Validated processing-operation errors — settled

`StorageError` is StorageService's internal and service-control error domain.
It is not exposed as the result of a `ValidatedDbClient` processing operation.
Every such operation exposes its semantic outcomes together with this closed
common error:

```rust
enum ProcessingDbError {
    GenerationLost,
    QueryFailed {
        diagnostic: Arc<str>,
    },
    InvalidStoredState {
        diagnostic: Arc<str>,
    },
    Persistence(PersistenceFault),
    ScoreOutOfRange(ScoreRangeFault),
    ServiceUnavailable {
        diagnostic: Arc<str>,
    },
}
```

Storage performs this normalization before an error crosses into processing:

| Internal observation | `ValidatedDbClient` result |
|---|---|
| The exact generation was already retired | `GenerationLost` |
| Connection loss followed by successful exact-generation retirement | `GenerationLost` |
| A nonmutating query fails while the generation remains valid | `QueryFailed` |
| A mutation produces a classified transaction outcome | `Persistence(...)` |
| A later bounded read discovers invalid persisted processing data | `InvalidStoredState` |
| Defensive score conversion rejects a caller value | `ScoreOutOfRange(...)` |
| The retirement/control path is unavailable, or Storage cannot safely continue after an internal or non-persisted representation failure | `ServiceUnavailable` |

`QueryFailed` proves that the failed operation performed no mutation. A
mutation must instead return its classified `PersistenceFault`; it must not
erase a definite, exhausted, or ambiguous transaction outcome into a query
failure. `InvalidStoredState` is limited to evidence read from persisted
processing contents. It does not classify invalid caller input, an impossible
internal postcondition, or a value rejected before persistence by the
representation bounds. Those cases use an existing semantic or range result
when applicable and otherwise return `ServiceUnavailable`.

`Rejected`, `EventPathClosed`, `ControlUnavailable`, `WorkerFailed`, raw
`Database`/`ConnectionLost`, and rebuild-only setup or replacement-control
errors never cross this boundary. A terminal StorageService rejection or
worker/event-path failure retires any published generations through the
service lifecycle. If retirement coordination itself is unavailable, the
operation returns `ServiceUnavailable`. The
[processing lifecycle](processing-lifecycle.md#supervisor-and-recovery-intent--settled)
solely owns cross-worker fault kinds and dispositions for every common variant.

## Caches and identity resolution — settled

Every processing-generation cache owned by `ValidatedDbClient` uses a separate
`moka::sync::Cache` instance. Moka is the settled implementation for this
storage cache layer: it provides concurrent access and bounded eviction without
an outer mutex serializing all cache operations. Separate instances preserve
independent value domains and capacity policies. A replacement processing
generation constructs fresh empty instances rather than sharing or invalidating
its predecessor's caches.

This requirement does not apply to API publication caches. Their response
reuse and single-flight behavior remain owned by the API architecture.
Exact cache capacities are tunable implementation choices recorded in the
[implementation choices](../implementation/choices.md#9-october-2026-storage-persistence-operations).

```rust
struct CachedIdentity {
    id: CompactId,
    materialized: bool,
}

struct CachedMaterializedBlock {
    coordinate: BlockCoordinate,
    selected_parent_id: CompactId,
}

struct CachedVspcSink {
    id: CompactId,
    hash: BlockHash,
}

enum ResolveMaterializedIdsError {
    Database(ProcessingDbError),
    NonMaterialized {
        missing: Arc<[BlockHash]>,
        identity_only: Arc<[BlockHash]>,
    },
}

impl ValidatedDbClient {
    async fn block_presence(
        &self,
        hash: BlockHash,
    ) -> Result<BlockPresence, ProcessingDbError>;

    async fn resolve_materialized_ids(
        &self,
        hashes: &[BlockHash],
    ) -> Result<Box<[CompactId]>, ResolveMaterializedIdsError>;
}
```

Caches contain:

- `BlockHash -> CachedIdentity`;
- `CompactId -> CachedMaterializedBlock`;
- `CompactId -> MergeSets`; and
- one capacity-one `CachedVspcSink` entry.

The three block-scaled domains use capacity eviction without a time-to-live.
The current-sink entry stores only the authoritative endpoint used to validate
the next VSPC source; it does not cache arbitrary VSPC membership. Cache
retention is never a processing-validity or accepted-reorganization bound.
Every incomplete lookup falls back to PostgreSQL.

`CachedIdentity.materialized` remains independent from
`CachedMaterializedBlock`. A materialized forward identity whose block metadata
entry was independently evicted is a cache miss, not an identity-only block or
database inconsistency. Boundary identities have forward identity entries but
no `CachedMaterializedBlock`.

Definite block materialization publishes the forward identity, materialized
block metadata, and immutable merge sets after commit. A cold identity or
block-metadata read loads all required immutable fields in one batch and may
populate the same domains. VSPC direct-member loading requires one
`CachedMaterializedBlock` for each distinct member:

```text
member ID
    -> CachedMaterializedBlock
        -> level through coordinate
        -> selected-parent ID
```

A missing block-metadata entry puts that member in one batched SQL fallback.
The returned coordinate and selected-parent ID populate the block-metadata
cache. Normal VSPC path validation compares member and selected-parent IDs and
does not resolve their hashes. The
[atomic VSPC transaction](#atomic-vspc-transaction--settled) owns lazy hash
resolution for exceptional conflict evidence. Concurrent cold misses may
duplicate the metadata read; they do not introduce waiting or cache
single-flight coordination.

The current-sink entry is seeded after a successful fresh
`load_session_state()` read and after a definite Rebuild commit. A VSPC
transaction loads the sink from PostgreSQL on a cache miss. Immediately after
a definite VSPC commit, while still ordered by the VSPC mutation lane, Storage
replaces the entry with the committed destination without an intervening
await. Rollback or ambiguous commit publishes neither a destination nor other
transaction-derived cache updates.

Rebuild replaces all four cache domains together after definite replacement
commit because Compact IDs can be reused. A new processing generation likewise
starts with four fresh empty domains. No predecessor or pre-Rebuild entry may
be observed through the replacement cache state.

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
enum RebuildFromPruningPointError {
    Database(ProcessingDbError),
    Setup(RebuildSetupFault),
    ReplacementControlUnavailable {
        diagnostic: Arc<str>,
    },
}

impl ValidatedDbClient {
    async fn rebuild_from_pruning_point(
        &self,
        pruning_point: ValidatedNodeBlock,
    ) -> Result<MaterializedSyncAnchor, RebuildFromPruningPointError>;
}
```

The pruning point is mandatory. Administrative schema reinitialization is a
separate [schema-lifecycle operation](#administrative-reinitialization--settled)
and is never implemented through this method. There is no processing-data
clear primitive without a pruning point.

### Preconditions and exclusive access

Before calling the method, the processing lifecycle has established all of
these conditions:

- ProcessingSession is executing `RecoveryMode::Rebuild` with one fixed
  `Arc<ValidatedRpcClient>` and this exact `Arc<ValidatedDbClient>`;
- the validated RPC `(network_id, genesis_hash)` equals this database
  generation's immutable binding exposed by `ValidatedDbClient`;
- the complete pruning-point block was obtained and validated through that RPC
  generation;
- the predecessor ProcessingSession has completed its consuming shutdown, no
  earlier session retains either validated client, and this Rebuild session
  has not initialized or launched its already-constructed inert processors
  under the
  [processing lifecycle](processing-lifecycle.md#processor-construction); and
- StorageService still owns the database advisory lock through its dedicated
  lock connection.

The method first establishes StorageService's API-read replacement exclusion
above. It then acquires the exclusive processing mutation guard without
waiting. Failure to acquire that guard after completed predecessor-session
shutdown is a lifecycle invariant violation; Rebuild must not wait behind an
unexpected processor mutation.

### Atomic replacement

One database transaction performs the complete replacement:

1. Clear `parents`, `blocks`, `levels`, `block_identifiers`, and the previous
   `ProcessingMetadata` within the same transaction.
2. Restart Compact-ID allocation.
3. Intern pruning-point hashes in the storage order:

   ```text
   red merge-set hashes
   blue merge-set hashes
   direct-parent hashes
   pruning-point hash
   ```

   Deduplicate reference hashes by first occurrence through the first three
   groups, then append the pruning point's own hash unconditionally. For an
   ordinary non-Genesis pruning point, do not add a separate selected-parent
   group: the shared `ValidatedNodeBlock` contract guarantees that its selected
   parent already occurs in canonical `direct_parents`, and an earlier
   cross-vector occurrence may already have interned the same hash. For exact
   Genesis, the three relationship groups are empty; intern synthetic ORIGIN
   explicitly before the Genesis hash so the block row retains its required
   non-null selected-parent identity.
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
   and the canonical ordered blue and red merge-set IDs.
6. Insert each canonical direct-parent relation once using the parent's
   interned ID and the outside-boundary coordinate sentinel `(0,0)`. The child
   coordinate is the pruning point's `(1,0)`.
7. Create `levels(level=1, size=1)` with the pruning point's DAA score because
   the pruning point is the current VSPC block at that level.
8. Store new `ProcessingMetadata` with
   `db_pp_blue_score = pruning_point.blue_score` without changing the immutable
   `DatabaseBinding`.
9. Commit all processing data and PP-derived metadata atomically.

Only a fully committed processing generation becomes visible.

Immediately after definite commit:

```text
database PP          = block at (level=1, slot=0)
committed VSPC sink  = pruning point
levels(1).size       = 1
```

For a Genesis pruning point, the bootstrap path stores the explicitly interned
synthetic ORIGIN as the non-null selected-parent identity while storing zero
actual direct parents. ORIGIN is not a direct parent. Genesis never enters the
ordinary BlockProcessor or `PersistedBlock` delivery path.

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

The immutable `DatabaseBinding` and schema/migration state survive. Rebuild
atomically replaces `ProcessingMetadata` with the supplied pruning point's
score and replaces all processing data. It cannot rebind the
database, replace its schema, change PostgreSQL ownership or configuration, or
perform administrative reinitialization.

An `Empty` or `Inconsistent` database becomes `Initialized` only through the
definite commit of this transaction. Rebuild of an already `Initialized`
database uses the same complete replacement.

### Cache publication

Only after definite commit, the current `ValidatedDbClient`:

1. replaces all four processing-cache domains with fresh empty instances;
2. seeds every newly interned pruning-point reference in the forward identity
   cache as `{ id, materialized: false }`;
3. seeds the pruning point's forward identity entry,
   `CachedMaterializedBlock`, and immutable merge sets; and
4. seeds `CachedVspcSink` from the committed pruning point.

No replacement cache state is visible before commit. A failed or ambiguous
attempt publishes none of these cache changes.

### Boundary state after commit

No database boolean records PP-boundary sealing. For a non-Genesis pruning
point, the transaction establishes the boundary but records no seal threshold
or processing phase. ProcessingSession owns
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
publishes no replacement cache state, retires the still-gated API generation
under the replacement-exclusion contract above, and returns the typed database
error.

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
    level: u32,
    size: u16,
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
    Database(ProcessingDbError),
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
persists its hash, selected parent, canonical direct-parent relations,
canonical merge sets, timestamp, and DAA score; blue score and blue work remain
available to processing but are not duplicated in the block row. Storage owns
transactional ID resolution, coordinate allocation, initial color,
persistence, and construction of the `BlockCommitted` value for a new
insertion. Its `id` is the inserted block's committed `CompactId`.

The parent payload contains the canonical direct-parent sequence only. For
every non-Genesis block, `selected_parent_index` is `Some(index)`, the index is
representable as `u32` and in bounds for `direct_parents`, and that entry is the
block's selected parent. The index addresses the shared value's canonical
first-occurrence position. Its coordinate can be `None` at the outside-PP
boundary. Genesis has an empty `direct_parents` payload and
`selected_parent_index = None`. Synthetic ORIGIN is Genesis's persisted
selected-parent identity, but it is never inserted into `direct_parents` or
emitted as a graph parent. The payload therefore identifies the selected-parent
coordinate without duplicating its hash.

`level_snapshots` is non-repeating by level and contains complete post-commit
snapshots for the inserted block's level, always, and every distinct
materialized direct-parent level needed by its emitted edges. It contains no
entry for an outside-boundary parent. `None` represents storage's no-VSPC DAA
sentinel. The collection includes parent-level context even when the
materialization transaction did not mutate that parent level; it is therefore
not named or interpreted as a change list. Its committed `size` is the
materialization lane's authoritative downstream field. Its `daa_score` records
the value observed by this transaction but does not supersede a VSPC outcome
at the graph consumer.

For every materialization, intern hashes in this order:

1. red merge-set hashes;
2. blue merge-set hashes;
3. direct-parent hashes;
4. the block's own hash.

Deduplicate reference hashes by first occurrence through the first three
groups, then append the block's own hash unconditionally. Do not add a separate
selected-parent group: every ordinary block's selected parent already occurs in
its canonical `direct_parents`, and cross-vector first occurrence remains
authoritative for identity allocation. The
[`ValidatedNodeBlock`](domain-model.md#shared-value-types--settled) contract
guarantees that it did not occur among those references. Validate the
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
WHERE levels.size < 65535
RETURNING levels.size - 1 AS slot, levels.size, levels.daa_score;
```

A newly inserted block starts `Gray` and outside VSPC. Parent rows contain
each actual materialized coordinate or the outside-boundary sentinel `(0,0)`.
A newly created level starts with the no-VSPC DAA sentinel. Materialization at
an existing level preserves that level's current DAA score while increasing its
size.

Deriving a child above `u32::MAX`, allocating a slot above
`MAX_LEVEL_SLOT`, or increasing a level beyond `MAX_LEVEL_SIZE` is an
impossible internal invariant and panics. These conditions have no typed
storage, processing, or public API disposition. Level derivation uses checked
addition. At the size cap the allocation statement returns no row and the
operation panics; transaction rollback leaves persistent state unchanged.

For a new insertion, construct `BlockCommitted` inside the same transaction
from the validated block, its allocated coordinate, and every canonical direct
parent's resolved storage state. A materialized parent has `Some(coordinate)`
and its complete level state appears once in `level_snapshots`; an
outside-boundary parent has `None` and contributes no level snapshot. Preserve
the canonical direct-parent sequence, record its selected-parent index, and
include the persisted initial color and VSPC membership. The inserted block's
resulting level snapshot reflects every size or DAA-score value stored by the
transaction. Return the `Inserted` outcome, including that complete payload,
only after definite commit. The payload therefore describes the same committed
state as the insertion; no post-commit projection read is allowed. The
sequential materialization lane returns definite inserted outcomes in
increasing `BlockCommitted.id` order.

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

enum ApplyVspcChangeError {
    Database(ProcessingDbError),
    VspcSourceDiscontinuity,
    VspcMemberNotMaterialized {
        id: CompactId,
    },
    VspcPathDiscontinuity(VspcPathConflict),
}

impl ValidatedDbClient {
    async fn apply_vspc_change(
        &self,
        change: ReadyVspcChange,
    ) -> Result<VspcCommitOutcome, ApplyVspcChangeError>;
}

struct VspcPathConflict {
    child: BlockHash,
    expected_parent: BlockHash,
    stored_parent: BlockHash,
}
```

Storage requires the supplied source to equal the currently committed sink.
Every block directly named in `removed` or `added` must be materialized.
Storage checks materiality and loads persisted relationship data once per
distinct supplied `CompactId` while applying every original position in the
ordered ID vectors. It performs no member-set rejection merely because a
member repeats or appears in both vectors. The normal source, materiality, and
selected-parent path checks still apply. For an admitted change, the ordered
mutation rules below determine final state. Storage loads each added block's
merge sets internally. Before mutation it also loads the persisted
selected-parent ID for every directly named chain member and validates:

```text
removed is empty:
    selected_parent_id(added[0]) == source.id

removed is nonempty:
    removed[0] == source.id
    selected_parent_id(removed[i]) == removed[i + 1]
    selected_parent_id(removed.last()) == selected_parent_id(added[0])

for every added i > 0:
    selected_parent_id(added[i]) == added[i - 1]
```

Every admitted change has nonempty `added`, so each expression above is
defined. A failed `removed[0] == source.id` check returns the distinct typed
`VspcSourceDiscontinuity` storage error. For the first failed selected-parent
relationship in the order shown, Storage identifies the child,
expected-parent, and stored-parent IDs. It resolves the distinct evidence IDs
to hashes with one bounded `block_identifiers` query inside the same
transaction, then returns `VspcPathDiscontinuity(VspcPathConflict)` before
mutation and rolls back. This exceptional evidence query does not populate a
general reverse-hash cache. This keeps ordinary VSPC validation entirely
ID-based and confines hash reads to a recovery-triggering exceptional outcome.
An incomplete or invalid evidence resolution returns
`ApplyVspcChangeError::Database(ProcessingDbError::InvalidStoredState)` instead
of manufacturing conflict evidence.
Storage supplies the persisted evidence and expected relationship but does not
attribute the conflict to either the database or the candidate. The
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
sentinel. The committed `daa_score` is the VSPC lane's authoritative downstream
field. `size` is the unchanged value observed by this transaction and does not
supersede a materialization outcome at the graph consumer.

The owned change is consumed so definite commit can return its destination
`VspcPoint` without cloning it. The result carries both the committed
destination and the authoritative final DAA state of every affected level; no
post-commit projection read is allowed.

## Stored session state — settled

```rust
enum StoredSessionState {
    Empty,
    Inconsistent,
    Initialized(StoredSessionSnapshot),
}

struct StoredSessionSnapshot {
    db_pp_hash: BlockHash,
    db_pp_blue_score: u64,
    committed_vspc_sink: StoredVspcSink,
}

struct StoredVspcSink {
    hash: BlockHash,
    id: CompactId,
    selected_parent: BlockHash,
    daa_score: u64,
}

impl ValidatedDbClient {
    async fn load_session_state(
        &self,
    ) -> Result<StoredSessionState, ProcessingDbError>;
}
```

`load_session_state` uses one repeatable-read, read-only transaction and the
[bounded processing-state classifier](#bounded-processing-state-classification--settled).
The result contains no node-derived value, recovery mode, current node PP, or
retained validity state.

The classifier owns the exact `Empty`, `Inconsistent`, and `Initialized`
classification boundary. `Inconsistent` is a semantic result distinct from an
operational query, decode, generation-loss, or cancellation error.

StorageService uses the same classifier when deciding initial generation
publication. It publishes a processing generation for `Empty`, `Initialized`,
and `Inconsistent`, but initially publishes an API generation only for `Empty`
or `Initialized`. It discards any snapshot produced by initial classification.
A Resync attempt calls `load_session_state` again and receives a fresh value;
`ValidatedDbClient` never stores or keeps this processing state synchronized.

The initialized snapshot contains only the values consumed during Resync
preparation. The
[processing lifecycle](processing-lifecycle.md#resync-preparation) owns its
node-side enrichment, component initialization, and disposal; ValidatedDbClient
retains no copy.

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

Canonical relationship validation belongs to the accepted Storage mutation
paths, not to this projection read. Rebuild and ordinary materialization
persist only the domain-owned canonical relationship sequences described by
the schema contract above, and the API projection consumes those persisted
relationships as already validated. It does not perform a second exhaustive
audit of per-vector uniqueness, own-hash exclusion, Genesis relationship
shape, or other relationship invariants established before persistence, and
it does not repair or canonicalize persisted relationships while reading.
Permitted cross-vector overlap remains unchanged.

`InconsistentProjection` still covers a failure encountered while decoding or
resolving the stored values required to construct the requested complete
projection, including an absent required identity, level, selected-parent
position, or construction-metadata value. It does not promise exhaustive
detection of schema-valid relationship state that accepted Storage mutation
paths cannot produce. Direct or out-of-band mutation of processing tables is
outside the validated-generation contract and does not create a requirement
for projection-time relationship revalidation.

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
reports `GenerationLost` to the operation only after `is_valid()` is false,
without retiring the independent processing handle. Publication of the
validated replacement emits
`StorageServiceEvent::ApiDbPublished`. A result covered by the read-local
resolution rules above reports `InconsistentProjection`; it never returns a
partial seed. The
[publication construction contract](api-publication.md#head-publication-lifecycle-and-stream-alignment--settled)
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
