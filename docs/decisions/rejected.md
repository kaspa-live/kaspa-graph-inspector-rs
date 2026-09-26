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
| Expose CompactId publicly. | It is database-local identity. | [API](../architecture/api.md) |
| Add a v1-style `/blockHashesByIds` public endpoint. | Every graph response carries the complete response-local hash dictionary needed to decode it. | [API](../architecture/api.md) |
| Assume coordinates are stable across instances. | Coordinates belong to one database allocation. | [Domain model](../architecture/domain-model.md), [API](../architecture/api.md) |
| Permit partially populated retained head-view levels. | It breaks the view-completeness invariant. | [API](../architecture/api.md) |
| Add a revision-count or byte-size cap to `GraphHistory`. | History is level-scoped, and Kaspa cannot produce unbounded revisions while remaining indefinitely at one fixed level. | [API](../architecture/api.md) |
| Escalate graph-observer loss directly into processing recovery. | Observer availability is outside processing correctness. | [API](../architecture/api.md) |
| Serve API reads from a partially rebuilt database. | It can expose mixed database generations. | [API](../architecture/api.md) |
| Move pruning-point or reconciliation preparation into Supervisor. | It violates recovery-component ownership. | [Processing lifecycle](../architecture/processing-lifecycle.md) |
