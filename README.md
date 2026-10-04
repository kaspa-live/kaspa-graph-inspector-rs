# kaspa-graph-inspector-rs
Kaspa Graph Inspector v2

The [architecture index](docs/architecture/README.md) identifies the focused
documents that collectively form the current normative KGI v2 architecture.
Decision status and remaining choices are tracked in
[docs/decisions](docs/decisions/README.md). Earlier handoffs are retained as
[historical provenance](docs/history/handoffs/README.md).

rusty-kaspa issues found during KGI v2 development are tracked in the
[local issue register](docs/rk-issues/README.md). These records are
non-normative and have no KGI collaboration-role authority.

## Development

Check the Rust workspace with:

```text
cargo check --workspace --locked
```

The Web workspace uses the Node version recorded in `web/.nvmrc`:

```text
cd web
npm ci
npm run build
```

Build the portable application bundle with:

```text
cargo xtask bundle
```
