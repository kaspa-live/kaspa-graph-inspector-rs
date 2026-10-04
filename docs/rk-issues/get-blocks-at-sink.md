# GetBlocks can mix virtual views between sink capture and anticone collection

## Status

Local issue record. Not yet filed upstream.

Verified against rusty-kaspa revision
`c338d495bec29e4dc8b5149f99e8db6fa916ed4a`.

## Priority

- **Global impact: G1 — High.** A public RPC response can combine two virtual
  views without exposing which view it represents, producing ambiguous or
  incomplete results for any client that incrementally follows GetBlocks.
- **KGI v2 impact: K1 — High.** The race can omit material needed by KGI's
  Resync/Catchup scan and force another recovery cycle. KGI has safe rolling
  marker and `Require(Resync)` recovery behavior, so it is not a K0 blocker.
- **Overall priority: P1.** The more urgent of G1 and K1 is P1.

## Summary

`GetBlocks` first reads the current sink and later asks consensus for that
sink's anticone. Those operations independently load the current virtual
state. The virtual can advance or reorganize between them, so one response can
combine:

- a selected-chain scan ending at sink `S0` from virtual state `V0`; and
- the anticone of `S0` calculated against the parents of a later virtual state
  `V1`.

rusty-kaspa should ensure that one GetBlocks response is derived from one
immutable virtual-state snapshot. This does not require stopping virtual
processing for the duration of the RPC.

## Current implementation

In `rpc/service/src/service.rs`:

1. `get_blocks_call` acquires a guarded consensus session at line 530.
2. It captures `sink_hash` with `async_get_sink()` at line 543.
3. It obtains the selected-chain/merge-set sequence up to that hash at line
   548.
4. When `high_hash == sink_hash`, it calls `async_get_anticone(sink_hash)` at
   line 556.

The guarded session does not freeze the virtual. Its contract in
`components/consensusmanager/src/session.rs:103-111` provides consistency by
preventing pruning between calls. Ordinary virtual-state commits do not take
the session write lock.

The two relevant consensus methods load virtual state separately:

- `consensus/src/consensus/mod.rs:736-738`: `get_sink()` loads
  `lkg_virtual_state` and returns its selected parent.
- `consensus/src/consensus/mod.rs:1333-1337`: `get_anticone(hash)` loads
  `lkg_virtual_state` again and uses the newly loaded parent set as the
  traversal context.

`LkgVirtualState` is an `ArcSwap` based lock-free last-known-good state, so the
second load can legitimately observe a different committed virtual state.

## Race sequence

```text
GetBlocks                         Virtual processor
---------                         -----------------
load V0
capture sink S0
hashes_between(low, S0)
                                  commit V1 with sink S1 and parents P1
get_anticone(S0)
  -> internally loads V1
  -> computes anticone(S0, P1)
return chain-to-S0 + V1-based anticone
```

If `S1` descends from `S0`, the response does not contain the selected-chain
suffix from `S0` to the sink current at response completion. Those descendants
are not in the anticone of `S0`. If the selected chain reorganizes, the later
parent set can instead change which new-branch blocks are considered anticone
members of `S0`. In either case, the response has no single virtual snapshot
whose sink and parent context explain all of its construction.

Append-only DAG properties and the pruning guard may make many instances
benign, but they do not provide the RPC-level snapshot invariant. Clients
cannot determine which virtual view the response represents.

## Required behavior

One GetBlocks call should capture one immutable virtual state `V` and use:

```text
sink = V.ghostdag_data.selected_parent
virtual_parents = V.parents
```

for both the scan endpoint and the sink-anticone traversal. Virtual processing
may publish newer states while the RPC runs; they must not alter that call's
endpoint or anticone context.

The preferred implementation is a consensus operation that loads
`LkgVirtualState::load_full()` once and returns all data needed by GetBlocks,
or accepts the captured sink and parent set for the traversal. Holding a
virtual write-excluding lock across the RPC and optional block conversion is
unnecessarily broad.

The existing pruning/session guard should remain responsible for keeping the
captured snapshot's referenced DAG data available during the call.

## Response metadata

The current `GetBlocksResponse` exposes only `block_hashes` and optional
`blocks`. A caller cannot directly tell whether the page reached the sink used
by the server. A separate `GetSink` call cannot answer that question reliably
because it observes another point in time.

Add response metadata derived from the same captured virtual snapshot:

```text
sink_hash: Hash
reached_sink: bool
```

Define `reached_sink` precisely as:

```text
high_hash == sink_hash
```

It means that the response includes the captured snapshot sink and that the
snapshot-consistent anticone was eligible to be appended. It must not mean
that the response reached whichever sink happens to be current when encoding
or sending the response.

Returning `sink_hash` with the flag is preferable to returning the flag alone:

- the client can identify the exact sink position before the appended
  anticone;
- the meaning remains inspectable in logs and fixtures;
- the client can distinguish successive snapshot sinks across calls; and
- the result removes the need for a racy companion `GetSink` request.

An optional `sink_index` could encode presence and position in one field, but
`sink_hash + reached_sink` is simpler across RPC transports. The wire change
would require coordinated updates to the RPC core model and serializer, gRPC
protobuf and converters, wRPC/WASM interfaces, mocks, and serialization tests.

Response metadata improves observability but does not repair the mixed-view
race by itself. Snapshot-consistent construction remains required.

## Impact on KGI v2

KGI could treat each GetBlocks page as one coherent node view and use
`sink_hash + reached_sink` as direct evidence that the page reached its
snapshot sink. This would remove the racy companion `GetSink` observation and
the need to infer sink presence from response contents.

KGI would still retain the rolling-sink/VSPC membership and DAA-distance rules
to avoid entering Catchup too early while the node advances, as well as the
fixed-tip Live-admission coverage for subscription-activation drops. Adoption
would require pinning a rusty-kaspa revision with the new response contract and
adding RPC normalization and compatibility fixtures.

## Acceptance tests

1. Insert a deterministic virtual update between sink capture and anticone
   collection. Assert that both are still derived from the original snapshot.
2. Cover a normal extension from `S0` to descendant `S1` during the call.
3. Cover a selected-chain reorganization during the call.
4. When the page limit stops below the captured sink, assert
   `reached_sink == false` and no anticone is appended.
5. When the page reaches the captured sink, assert `reached_sink == true`, the
   returned `sink_hash` occurs exactly once, and all appended anticone entries
   use the captured virtual parent context.
6. Cover `low_hash == sink_hash`; the inclusive low hash is the captured sink,
   so `reached_sink == true` and duplicate filtering still keeps it once.
7. Verify backward/forward compatibility for every supported RPC transport.
