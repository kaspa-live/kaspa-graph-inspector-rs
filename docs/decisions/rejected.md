# Rejected designs

These proposals were considered and not accepted.

| Rejected proposal | Concise reason | Current owner(s) |
|---|---|---|
| Go KGI dependency recursion is depth-limited. | The reference `ProcessBlockAndDependencies` recursively covers arbitrary depth. | [Block processing](../architecture/block-processing.md) |
| One monolithic block and VSPC worker. | It collapses distinct ownership and sequencing responsibilities. | [Overview](../architecture/overview.md) |
| Commit VSPC strictly by arrival order, or rewind and replay recent history. | Arrival order alone cannot establish readiness or continuity. | [VSPC processing](../architecture/vspc-processing.md) |
| Persist ordinary orphan-only hashes. | Orphan topology is transient processing state. | [Block processing](../architecture/block-processing.md), [storage](../architecture/storage.md) |
| Use broad notification epochs or a timer-based latecomer grace period. | Transport delivery cannot support a reliable epoch assignment. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Use local or adjacent GetBlocks order decrease as a Catchup trigger. | Valid topological output may decrease locally. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Add a dedicated mixed-view recovery protocol for GetBlocks assembled while Virtual moves. | Existing typed recovery already covers a material omission. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Derive a destination from a nonempty removed chain with an empty added path. | The malformed shape has no valid destination. | [VSPC processing](../architecture/vspc-processing.md) |
| Admit a wholly empty `VirtualChainChanged` to VspcProcessor. | It is a valid transport no-op with no processor work. | [NodeService](../architecture/node-service.md) |
| Enable local routing before both remote subscriptions start. | It does not close the remote subscription gap. | [NodeService](../architecture/node-service.md) |
| Require zero unresolved orphans before Live. | Unrelated valid pending work need not block Live eligibility. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Add a VSPC coverage phase, freeze acknowledgement, checkpoint, or synthetic-stream terminal marker. | The existing phase transition requires none of them. | [Processing lifecycle](../architecture/processing-lifecycle.md), [VSPC processing](../architecture/vspc-processing.md) |
| Escalate an arbitrary count of failed Resync attempts to Rebuild. | Recovery strength follows typed evidence. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Retry services or recovery immediately or without a rate bound. | It violates the bounded retry policy. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Transparently retry an ambiguous database commit. | The commit outcome is unknowable. | [Storage](../architecture/storage.md) |
| Use a persistent destructive `--reinitialize-db --yes` service option. | It can repeat destructive replacement unattended. | [Storage](../architecture/storage.md) |
| Expose `NodeService::persist_node_metadata()`. | It violates storage ownership of database metadata. | [Storage](../architecture/storage.md) |
| Persist a dedicated VSPC checkpoint or sink table. | It creates a second authority for derivable state. | [Storage](../architecture/storage.md) |
| Persist PP identity separately by CompactId. | It duplicates the canonical retained PP representation. | [Storage](../architecture/storage.md) |
| Add committed-index waiters. | Committed identity already flows through the owned delivery path. | [Block processing](../architecture/block-processing.md) |
| Add a separate `resolve_materialized_dependencies()` storage operation. | It separates reference validation from the transaction that depends on it. | [Block processing](../architecture/block-processing.md), [storage](../architecture/storage.md) |
| Add a `Satisfied(hash)` resolver queue protocol. | The existing pending-set transition already owns completion. | [Block processing](../architecture/block-processing.md) |
| Add `ReadyAddedBlock`. | It carries no semantics beyond the owned ready and storage inputs. | [VSPC processing](../architecture/vspc-processing.md), [storage](../architecture/storage.md) |
| Add `ProcessingResources`. | It adds no semantics to the prepared session inputs. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
| Add a redundant `PendingVspcChange` wrapper. | It adds no semantics to `VspcChange`. | [VSPC processing](../architecture/vspc-processing.md) |
| Infer parent-row foreign-key validity merely from identity existence. | Identity does not prove materiality. | [Storage](../architecture/storage.md) |
| Expose CompactId publicly. | It is database-local identity. | [API protocol](../architecture/api-protocol.md) |
| Add a v1-style `/blockHashesByIds` public endpoint. | Every graph response carries the complete response-local hash dictionary needed to decode it. | [API protocol](../architecture/api-protocol.md) |
| Assume coordinates are stable across instances. | Coordinates belong to one database allocation. | [Domain model](../architecture/domain-model.md), [API protocol](../architecture/api-protocol.md) |
| Permit partially populated retained head-view levels. | It breaks the view-completeness invariant. | [API graph model](../architecture/api-graph.md) |
| Add a revision-count or byte-size cap to `GraphHistory`. | History is level-scoped, and Kaspa cannot produce unbounded revisions while remaining indefinitely at one fixed level. | [API graph model](../architecture/api-graph.md) |
| Add an independent encoded-byte cap to the Head delta cache. | Publication ownership, retained source boundaries, and target-level distance bound its lifetime and extent. | [API protocol](../architecture/api-protocol.md) |
| Negotiate multiple graph wire formats in v2. | The single settled JSON format avoids parallel browser codecs and representation-specific cache variants. | [API protocol](../architecture/api-protocol.md) |
| Serve successful graph responses through identity encoding or multiple compressed variants. | One mandatory gzip path keeps each retained response in one final transfer representation and avoids cache variants or cache-hit recompression. | [API protocol](../architecture/api-protocol.md#graph-http-compression--settled) |
| Add distance-adaptive pacing to Head-backed fixed views. | The shared canonical-delta convergence and wakeup flow already groups work; another scheduler adds lag and increases exposure to history expiry. | [Web](../architecture/web.md#fixed-views--settled) |
| Schedule SSE wakeups from measured client round-trip cohorts. | A fixed graph-revision convergence interval provides deterministic wake coordination without RTT estimation. | [API protocol](../architecture/api-protocol.md) |
| Promote or repeatedly replace a completed source-keyed `CachedDelta` as Head advances. | Immutable entries, revision-gated wakeups, and heat-selected destinations provide convergence without reassessing cache hits. | [API protocol](../architecture/api-protocol.md) |
| Apply a Head-generated `GraphDelta` directly to an internal `Fixed` `GraphView`. | Internal Fixed mutation requires the original event plus post-update Head context. Browser presentation instead filters the public canonical representation and obtains missing retained levels explicitly. | [API graph model](../architecture/api-graph.md), [Web](../architecture/web.md) |
| Escalate API graph-update loss directly into processing recovery. | API projection availability is outside processing correctness. | [API ingress](../architecture/api-ingress.md), [API graph publication](../architecture/api-publication.md) |
| Serve API reads from a partially rebuilt database. | It can expose mixed database generations. | [Storage](../architecture/storage.md), [ApiService](../architecture/api-service.md) |
| Move pruning-point or reconciliation preparation into Supervisor. | It violates recovery-component ownership. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
