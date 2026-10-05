# KGI v2 implementation choices

This non-normative record captures durable implementation choices made under
the constraints in the focused architecture. It does not amend those
contracts. Choices are recorded when their first dependent implementation
tranche begins; unresolved choices remain in the
[deferred register](../decisions/deferred.md).

## 5 October 2026: shared model and graph-update ingress

### Initial module boundaries

`kgi-model` starts with these modules:

- `block`: block identity aliases, timestamps, colors, validated node blocks,
  consensus order, database-local coordinates, materiality, sync anchors, and
  persisted-block delivery values;
- `graph_update`: committed block/VSPC projection payloads and the ordered
  graph-update enum;
- `lifecycle`: recovery modes, cross-component fault values, and component
  status observations; and
- `vspc`: normalized and ready VSPC transition values.

`lib.rs` exposes these as public modules without re-exporting their contents at
the crate root. Consumers use qualified imports such as
`kgi_model::lifecycle::RecoveryMode`, preserving the semantic concern in each
type's path. These boundaries can split when a module gains independent
behavior; crate ownership and the acyclic dependency graph remain unchanged.

`kgi-api-ingress` starts with:

- `channel`: channel construction, producer and receiver capabilities, the
  producer gate, offer outcomes, and producer errors; and
- `gap`: the session-local coalescing continuity-generation reporter and
  consumer observation.

`lib.rs` exposes these as public modules without re-exporting their contents at
the crate root, matching `kgi-model`'s module-qualified import style.
Operational counters stay next to the channel state until a common metrics
adapter is implemented.

### Upstream value crates and revision

Use the following workspace dependency declarations for Kaspa value crates:

```toml
kaspa-hashes = { git = "https://github.com/kaspanet/rusty-kaspa.git", tag = "v2.0.1" }
kaspa-math = { git = "https://github.com/kaspanet/rusty-kaspa.git", tag = "v2.0.1" }
```

`kgi-model::BlockHash` aliases
`kaspa_hashes::Hash`; `kgi-model::BlueWork` aliases
`kaspa_math::Uint192`. This keeps upstream representation and ordering while
avoiding a dependency from `kgi-model` on consensus, RPC, or service crates.
The committed Cargo lockfile records the resolved source commit. The upstream
tag currently resolves to `cfafeb4c093fa37a303f1b9f19c58f986b870ce3`.

This tag is the implementation dependency selection for the initial model.
PUAR acceptance and runtime node-compatibility scope remain owned by the
[verification contract](../architecture/verification.md); a later PUAR run will
handle the next upstream review.

### Tokio channel, gate, and gap primitives

Use `tokio::sync::mpsc::channel` for the bounded graph-update channel. Its
cloneable `Sender` and single `Receiver` match the session capability split,
and `try_send` distinguishes full capacity from receiver closure.

Use `std::sync::Mutex<GraphUpdateGateState>` for the producer gate. Every
ordinary classification and `try_send` occurs in one short, non-async critical
section. Lossless Live-marker delivery releases this mutex before awaiting
channel capacity, so no Tokio task may block while holding it. Mutex poisoning
is an internal invariant failure rather than a recoverable session condition.

Use `tokio::sync::watch<u64>` for gap observation. The value is a monotonically
increasing session-local generation starting at zero. Reporting a gap advances
the generation and publishes the latest value with `send_replace`; multiple
reports may coalesce into one wakeup while the changed generation remains
observable. The consumer records the last generation it has handled and uses
`borrow_and_update` plus `changed` so registration cannot lose a report between
inspection and waiting. Counter overflow is an internal invariant failure.

The channel, gate, and watch endpoints are created together. The receiver owns
both consumer endpoints, so they cannot be closed independently through the
public API.

### Error conventions

Library crates use explicit typed error enums derived with `thiserror::Error`.
They do not return `anyhow::Error`, boxed dynamic errors, or select control flow
from error strings. Component-local errors may retain concrete upstream
sources; before an error crosses an ownership boundary it is classified into
the shared typed fault vocabulary, with `Arc<str>` used only for diagnostic
context.

The top binary and `xtask` may use `anyhow` for command-boundary context where
no caller branches on the error. They must still preserve typed component
errors until command dispatch has selected the process result, and they must
apply the architecture's secret-redaction requirements.

Tests use direct typed matching rather than formatted-message matching.

### Rust test runner

Use `cargo nextest run --workspace --locked` for workspace unit and integration
tests, following rusty-kaspa's test-runner split. Run
`cargo test --doc --workspace --locked` separately because Nextest does not run
rustdoc tests. Targeted development runs may narrow the package or test filter
while retaining Nextest. No custom Nextest profile is added until a concrete
test needs repository-specific retry, timeout, or grouping behavior.
