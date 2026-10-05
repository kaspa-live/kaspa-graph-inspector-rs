# Implementation status

Updated 5 October 2026.

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
the selected rusty-kaspa `v2.0.1` value crates. The session-scoped graph-update
ingress implements the shared pre-seal gate, nonblocking ordinary offers,
lossless lifecycle markers, coalescing gap observation, and ingress metrics.
The remaining crates are still behavior-free scaffolds; production service
behavior and database migrations have not started. Open architecture
requirements continue to block only their dependent work.

The imported browser still uses the v1 graph data source, models, and update
behavior, so it is not yet compatible with the v2 HTTP, SSE, publication, and
graph contracts. Runtime Web configuration, concrete Docker and conventional
installation files, and the file-log directory model remain unimplemented. The
`kgi-core::signals` module exists, while its Supervisor-owned termination
adapter remains unimplemented.

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

The non-normative [implementation sequence](sequence.md) records
the proposed work order and prerequisites.
