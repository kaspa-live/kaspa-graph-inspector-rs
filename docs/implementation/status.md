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
bundle command are present. The crates remain behavior-free scaffolds;
production service behavior, component tests, and database migrations have not
started. Open architecture requirements continue to block only their dependent
work.

Importing the retained KGI v1 React source, runtime Web configuration, concrete
Docker and conventional installation files, and the file-log directory model
remain unimplemented. The `kgi-core::signals` module exists, while its
Supervisor-owned termination adapter remains unimplemented.

The public [graph wire format](../architecture/api-protocol.md#common-transport-dto-rules--settled)
and [mandatory gzip delivery path](../architecture/api-protocol.md#graph-http-compression--settled)
are settled. Exact gzip quality remains deferred; optional later binary-format
benchmarking does not block implementation.

The non-normative [implementation sequence](sequence.md) records
the proposed work order and prerequisites.
