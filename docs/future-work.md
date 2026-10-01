# Future work

These items are explicitly outside the KGI v2 requirement and are not open v2 architecture questions.

They do not weaken or delay the accepted v2 contract. A candidate enters v2
only through an accepted architecture decision that updates its focused owner
and removes or narrows the entry here. Record new future candidates when they
are accepted; do not introduce them implicitly during implementation.

## KGI v2.1 candidate list

- low-frequency Live VSPC consistency probe;
- administrative/API-triggered recovery through Supervisor;
- investigate deterministic per-level slot ordering derived from block data,
  aiming for stable coordinates where instances share the same retained DAG;
  the [domain model](architecture/domain-model.md#shared-value-types--settled)
  owns current coordinate semantics, and the ordering and migration
  consequences need a separate design;
- an explicit offline administrative import from a KGI v1 database into a
  separate KGI v2 database; the
  [storage contract](architecture/storage.md#database-bootstrap-and-validation--settled)
  owns current startup behavior, while network validation, resumability, and
  transactional publication remain to be designed. Any future migration that
  cannot be transactional must likewise be an explicit offline administrative
  operation;
- historical graph-window response caching, if usage measurements justify it;
  the [API protocol](architecture/api-protocol.md#anchored-graph-windows--settled)
  owns current behavior, and any future cache must account for VSPC reorgs and
  graph changes within one API publication epoch;
- other improvements must be recorded explicitly rather than silently entering
  the v2 implementation.
