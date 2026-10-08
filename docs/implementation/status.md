# Implementation status

Updated 8 October 2026.

The architecture bootstrap, post-handoff reconciliation, focused-document
extraction, and two independent losslessness reviews are complete. The focused
documents named by `docs/architecture/README.md` are the normative KGI v2
architecture; the superseded handoffs are historical provenance.

The implementation foundation is in progress from reviewed architecture
baseline commit `f2cab146a3633964a69ebcdcb045d4729c657f0a`. The buildable
virtual Cargo workspace, settled crate dependency skeleton, storage migration
location, Vite/React Web workspace, fixture locations, and atomic `xtask`
bundle command are present. The retained KGI v1 browser source and replay
fixtures have been imported from source commit
`2ec895375f1161af1e57a58ed94db5977584d97d`; its Create React App build seam
has been replaced by Vite and the imported source builds with the workspace's
React 19 and TypeScript 7 toolchain. The shared `kgi-model` domain, lifecycle,
fault, status, VSPC, and committed graph-update values are implemented against
the selected rusty-kaspa `v2.1.0` value crates. The session-scoped graph-update
ingress implements the shared pre-seal gate, nonblocking ordinary offers,
lossless lifecycle markers, coalescing gap observation, and ingress metrics.
Processing and API service crates remain behavior-free scaffolds. The first
StorageService implementation slice selects SQLx with PostgreSQL and Rustls,
embeds the forward-only metadata and processing-schema migrations, acquires the
dedicated advisory ownership lock, classifies database bootstrap states, and
performs the atomic idempotent `Uninitialized -> Empty` network binding. Its
post-migration physical-layout fingerprint validates column defaults and
identity mode, constraints, constraint-backed indexes, and required query-path
indexes before any database generation can be published.
The validated-generation storage slice adds processing and read-only API pool
generations without exposing SQLx resources across the crate boundary. The
processing pool is independently capped at four connections, the API pool at
eight, and API sessions default to read-only. One `ValidatedDbClient` type is
used for compatible Empty, Initialized, and Inconsistent contents; it proves
generation ownership, compatible schema, and immutable `DatabaseBinding`.
Immutable network metadata and mutable `ProcessingMetadata` use separate
singleton tables; Empty has no processing-metadata row, and invalid processing
metadata produces semantic Inconsistent without invalidating the client. Only
coherent Empty and Initialized contents initially produce an API capability.
API validity is shared and terminal, while retirement authority remains private
to storage. The processing client also loads a fresh storage-owned session
state in one repeatable-read transaction. Initial publication and session
loading share a bounded classifier based on existence checks, indexed PP and
VSPC-sink lookup, and primary-key identity resolution; they perform no retained
graph audit. The permanent autonomous StorageService lifecycle now owns
connection, advisory-lock health, initialization authorization, exact
processing and API generation publication and retirement, independent pool
replacement, terminal rejection, retry with equal jitter and its Ready reset,
and terminal idempotent shutdown. The implemented session-state operation
reports connection loss through a private exact-generation retirement barrier,
so the ordered retirement event is enqueued before `GenerationLost` returns.
Deterministic lifecycle tests cover the nominal retry sequence and reset
boundary; PostgreSQL integration tests cover publication order, exact and
independent replacement, advisory-lock contention, lock loss with both
two-generation and processing-only states, lock loss during blocked replacement
opening, the pre-publication ownership recheck, nonblocking replacement while a
retired pool still has a checked-out connection, terminal joining of that pool
drain, missing required index and constraint rejection, initialization COMMIT
acknowledgement loss with truth-based reconnect classification, and fatal
event-path closure.
The gated Rebuild-start API publication, replacement exclusion, and bounded
drain/cancellation remain for the database-replacement-safety increment
together with their dependent storage operations. No open architecture
requirement currently blocks that work.

The imported browser still uses the v1 graph data source, models, and update
behavior, so it is not yet compatible with the v2 HTTP, SSE, publication, and
graph contracts. Runtime Web configuration, concrete Docker and conventional
installation files, and the file-log directory model remain unimplemented.

The public [graph wire format](../architecture/api-protocol.md#common-transport-dto-rules--settled)
and [mandatory gzip delivery path](../architecture/api-protocol.md#graph-http-compression--settled)
are settled. Exact gzip quality remains deferred; optional later binary-format
benchmarking does not block implementation.

The first production tranche is complete through graph-update ingress commit
`4ed0d2f`. Its initial module
boundaries, upstream value-crate pin, graph-update channel and gap primitives,
and error conventions are recorded in
[implementation choices](choices.md#5-october-2026-shared-model-and-graph-update-ingress).
The shared model and session-scoped graph-update ingress portions are complete.
Workspace formatting, Cargo check, Nextest, doctests, and Clippy with warnings
denied pass. The Web Vitest baseline passes with no test files present, and the
Vite production build passes with its existing large-chunk warning. This
increment is ready for review before worker or API consumers are added.

The next infrastructure increment now implements the immutable `kgi-core`
configuration values, protected database and token values, explicit TOML,
environment, and CLI source resolution, static validation, and the top-crate
command grammar. It also implements the `kgi-core::signals` adapter with its
weak shutdown target, first- and second-signal graceful requests, and
third-signal forced exit. Process composition and dispatch remain pending, as
does the Supervisor-owned installation that connects the signal adapter to the
global shutdown lifecycle.

The NodeService increment is implemented through its validated-generation
lifecycle. The accepted rusty-kaspa `v2.1.0` PUAR is present, and
the `kgi-node` module boundaries, direct dependencies, lifecycle primitives,
and deterministic RPC, clock, and jitter test seams are recorded in
[implementation choices](choices.md#6-october-2026-nodeservice-implementation-foundation).
`kgi-node` now resolves and validates local consensus assumptions and contains
the generation-bound raw-response normalizer for full blocks, recovery headers,
pruning-point and Catchup samples, GetBlocks pages, VSPC V2 responses, and
BlockAdded notifications. The private normalizer is shared by each validated
client and its stable notification router. It preserves the minimized
trusted-node boundary. RPC API compatibility and Genesis discovery are private
steps in NodeService connection validation rather than standalone public
helpers.
The architecture-level GetBlock not-found representation blocker is resolved
by the NodeService-owned
[compatibility contract](../architecture/node-service.md#getblock-not-found-compatibility-classification)
and the lifecycle-owned opaque RPC-failure disposition.
Validated-generation RPC execution now binds every operation and composite
request to one physical connection, applies the runtime concurrency bound,
constructs the required requests, normalizes their responses, classifies the
pinned exact-message GetBlock compatibility forms, and linearizes completion
against cancellation or retirement. Malformed recovery responses use an
exact-generation retirement request and completion barrier; opaque RPC failures
and range faults do not retire the generation. The private
`NotificationRouter` implements the synchronous rusty-kaspa callback,
generation-context BlockAdded normalization, empty and malformed virtual-chain
filtering, bounded nonblocking delivery, and shared disablement after malformed
input, saturation, or unexpected endpoint loss. Each validated generation now
owns ordered all-or-nothing subscription activation, rollback, deactivation,
and retirement on incomplete remote cleanup. The permanent NodeService worker
connects with upstream automatic reconnection disabled, validates the node,
waits through IBD, publishes and retires exact `Arc` generations, preserves
last-validated status, applies jittered reconnect backoff with its Ready reset,
and provides terminal idempotent shutdown. Deterministic tests cover the owner
loop, subscription failure paths, publication and retirement order, IBD gate,
retry sequence and reset boundary, and the operation-retirement completion
barrier.

The non-normative [implementation sequence](sequence.md) records
the proposed work order and prerequisites.
