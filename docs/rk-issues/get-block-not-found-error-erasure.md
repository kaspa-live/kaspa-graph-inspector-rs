# gRPC erases the structured GetBlock not-found error

## Status

Local issue record. Not yet filed upstream.

Verified against rusty-kaspa revision
`01b532e8b553523216471682649693af92f0fd16`.

## Priority

- **Global impact: G2 — Moderate.** The gRPC protocol reduces structured server
  errors to human-readable text, so downstream clients cannot reliably branch
  on recoverable semantic outcomes. Clients can use conservative generic-error
  handling, but lose useful API meaning.
- **KGI v2 impact: K1 — High.** KGI requires definitive GetBlock absence for
  recovery and attribution decisions. Without a compatibility workaround the
  selected production client cannot implement that distinction; a failed or
  changed workaround can delay progress or force unnecessary recovery.
- **Overall priority: P1.** The more urgent of G2 and K1 is P1.

## Summary

The RPC service obtains a typed `ConsensusError::BlockNotFound(hash)`, and
`RpcError` can retain it through its transparent `ConsensusError` variant.
The gRPC boundary discards that identity: `RPCError` contains only a message,
the server serializes `RpcError` with `to_string()`, and the client converts the
message back into `RpcError::General(String)`.

Consequently, `ConsensusError::BlockNotFound` is indistinguishable by type from
other remote server failures through `kaspa-grpc-client`.

## Current implementation

`rpc/service/src/service.rs` implements GetBlock by propagating the consensus
lookup error:

```rust
let block = session
    .async_get_block_even_if_header_only(request.hash)
    .await?;
```

The consensus error is typed and includes the requested hash:

```rust
#[error("cannot find full block {0}")]
BlockNotFound(Hash),
```

The gRPC schema in `rpc/grpc/core/proto/rpc.proto` exposes only:

```proto
message RPCError {
  string message = 1;
}
```

The conversion in `rpc/grpc/core/src/convert/error.rs` serializes every
`RpcError` with `to_string()` and reconstructs the received message through
`RpcError::from(String)`. That conversion produces `RpcError::General`.

The public gRPC client also converts all of its own `Error` variants, including
Tonic status, timeout, endpoint connection, channel, missing-payload, and
not-connected failures, into `RpcError::General(error.to_string())`. A caller
therefore cannot use the resulting variant to distinguish remote application
failure from a local transport or client failure.

## Required behavior

The production RPC path should expose a structured GetBlock-not-found
discriminator. It may be a backward-compatible error code or a GetBlock-owned
result variant, provided the server sets it from the typed consensus error and
the Rust client preserves it without interpreting diagnostic text.

An older server that does not send the discriminator must remain an opaque RPC
failure rather than being misclassified.

## KGI v2 workaround

KGI v2 uses a pinned, exact-message compatibility adapter owned by the
normative NodeService contract. It compares the complete remote message with
the display form of `ConsensusError::BlockNotFound(requested_hash)` and turns
only an exact match into KGI's typed definitive-not-found outcome. All other
messages become KGI's opaque typed `RpcRequestFailed` outcome.

This workaround is intentionally narrow and must be removed after a structured
upstream result is available and accepted by a new pinned-upstream review.

## Upstream issue or fix

No upstream issue or fix is recorded yet.
