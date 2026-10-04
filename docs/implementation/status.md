# Implementation status

Updated 5 October 2026.

The architecture bootstrap, post-handoff reconciliation, focused-document
extraction, and two independent losslessness reviews are complete. The focused
documents named by `docs/architecture/README.md` are the normative KGI v2
architecture; the superseded handoffs are historical provenance.

No production Rust implementation, tests, or migrations have started. The
reviewed architecture baseline for implementation is commit
`f2cab146a3633964a69ebcdcb045d4729c657f0a`. Open architecture requirements
continue to block only their dependent work.

The Cargo repository layout, storage-owned migration placement, retained KGI
v1 React source with a Vite build, Web test tooling, atomic release bundle,
runtime Web configuration, Docker and conventional installation layouts, and
file-log directory model are settled but not yet implemented. The
Supervisor-owned `kgi-core::signals` termination adapter is also settled but
not yet implemented.

The public [graph wire format](../architecture/api-protocol.md#common-transport-dto-rules--settled)
and [mandatory gzip delivery path](../architecture/api-protocol.md#graph-http-compression--settled)
are settled. Exact gzip quality remains deferred; optional later binary-format
benchmarking does not block implementation.

The non-normative [implementation sequence](sequence.md) records
the proposed work order and prerequisites.
