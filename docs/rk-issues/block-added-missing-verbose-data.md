# BlockAdded can silently omit required verbose block data

## Status

Local issue record. Not yet filed upstream.

Verified against rusty-kaspa revision
`c338d495bec29e4dc8b5149f99e8db6fa916ed4a`.

## Priority

- **Global impact: G2 — Moderate.** The partial notification is limited to an
  enrichment failure and clients can detect missing verbose data, but the RPC
  reports success while omitting consensus metadata that some subscribers
  require.
- **KGI v2 impact: K1 — High.** KGI cannot materialize the notification and
  must request Resync. Repeated enrichment failures can repeatedly interrupt
  Live operation, although the recovery policy preserves correctness.
- **Overall priority: P1.** The more urgent of G2 and K1 is P1.

## Summary

The RPC service normally enriches a `BlockAdded` notification with
`RpcBlockVerboseData`. If that enrichment fails, it still emits a successful
notification containing the block but sets `verbose_data` to `None`.

This turns an internal enrichment failure into a structurally valid public
notification without telling the subscriber that consensus metadata is
missing. Consumers that require the selected parent, blue merge set, or red
merge set cannot materialize the block or safely continue the notification
stream.

rusty-kaspa should either guarantee verbose data on `BlockAdded` notifications
or expose the enrichment failure as an observable subscription/RPC failure. It
should not silently downgrade the payload.

## Current implementation

`rpc/service/src/converter/consensus.rs` converts the consensus notification:

```rust
consensus_notify::Notification::BlockAdded(msg) => {
    let session = self.consensus_manager.consensus().unguarded_session();
    // If get_block fails, rely on the infallible From implementation which will lack verbose data
    let block = Arc::new(
        self.get_block(&session, &msg.block, true, true)
            .await
            .unwrap_or_else(|_| (&msg.block).into()),
    );
    Notification::BlockAdded(BlockAddedNotification { block })
}
```

The fallback conversion preserves the header and transactions but has no
`RpcBlockVerboseData`.

The same file's `get_block` enrichment obtains data that is not present in the
base block:

- GHOSTDAG data, including selected parent, blue score, blue merge set, and
  red merge set;
- block status;
- children; and
- current selected-chain membership.

Any error from those lookups therefore produces a partial notification while
hiding the error that caused it.

In `rpc/core/src/model/block.rs`, the missing fields are carried only by the
optional `RpcBlockVerboseData`:

```rust
pub struct RpcBlockVerboseData {
    pub hash: RpcHash,
    pub selected_parent_hash: RpcHash,
    pub blue_score: u64,
    pub merge_set_blues_hashes: Vec<RpcHash>,
    pub merge_set_reds_hashes: Vec<RpcHash>,
    // ...
}
```

## Required behavior

An emitted `BlockAdded` notification should have one unambiguous contract:

```text
successful BlockAdded delivery => block.verbose_data is present and complete
```

The preferred fix is to make enrichment reliable for the notification's
block, using a consensus/session view that keeps the required records
available until conversion completes. If enrichment still fails, propagate an
observable notification or connection failure so subscribers know continuity
was lost.

Possible implementations include carrying the required immutable GHOSTDAG
data with the internal notification or performing conversion under an
appropriate guarded consensus session. The implementation should preserve the
ordering of successful `BlockAdded` notifications and must not emit the
partial fallback as success.

## Impact on KGI v2

KGI materializes each block with its selected parent and blue/red merge sets.
It cannot reconstruct those fields from the base header. Its current safe
policy is therefore to treat an Enabled `BlockAdded` without verbose data as
notification loss and request a full Resync.

An upstream guarantee would let KGI trust every successful `BlockAdded` as
directly materializable. This would:

- avoid an otherwise unnecessary Resync after a transient enrichment failure;
- preserve the Live notification stream without a KGI-specific refetch path;
- keep notification/synthetic overlap accounting tied to usable block data;
  and
- remove a valid partial-payload case from KGI's router and processor fault
  handling after KGI pins the fixed rusty-kaspa revision.

Until such a revision is adopted, KGI must retain its `Require(Resync)` policy.

## Acceptance tests

1. Force each enrichment lookup used by `get_block` to fail and verify that no
   successful partial `BlockAdded` notification is emitted.
2. Verify that every successfully delivered `BlockAdded` has
   `verbose_data.is_some()` and that its hash matches the header hash.
3. Verify selected parent and blue/red merge sets against the block's stored
   GHOSTDAG data.
4. Exercise pruning or session turnover during conversion and verify that the
   subscriber either receives a complete notification or an observable
   continuity failure.
5. Verify that the fix preserves notification order.
