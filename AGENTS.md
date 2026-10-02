# KGI v2 collaboration contract

This repository uses three distinct collaboration roles. The repository is the durable source of truth; chat transcripts are not.

## Mandatory reading order

Before changing or reviewing this repository, read in this order:

1. This `AGENTS.md`.
2. `docs/architecture/README.md` for architecture ownership and navigation.
3. `docs/architecture/overview.md` and every focused architecture document
   relevant to the work.
4. `docs/decisions/README.md`, the settled, rejected, and superseded
   registers, and any accepted standalone ADRs in `docs/decisions/`.
5. `docs/decisions/open.md` or `docs/decisions/deferred.md` when the work
   touches an unresolved requirement or implementation choice.
6. `docs/implementation/status.md` and relevant reports in `docs/reviews/`
   for non-normative project state.

The handoffs under `docs/history/handoffs/` preserve the original bootstrap
and consolidation record. They are provenance only and do not participate in
normative precedence. Current behavior and rationale belong to the focused
architecture owner identified by `docs/architecture/README.md`.

Normative precedence is:

```text
focused architecture and later accepted ADRs
    > implementation and tests
    > implementation-status and review notes
```

## Single ownership policy

Every current normative claim has exactly one owning document. A normative
claim includes required or prohibited behavior, state transitions and their
ordering, validation and failure dispositions, constants and formulas,
semantic structs or function signatures, and the rationale needed to
interpret those contracts. The architecture index assigns focused owners by
concern. An accepted ADR may preserve decision history, but its current
behavior and interpretive rationale must be incorporated into the focused
owner in the same change.

A document that does not own a claim may:

- name an operation, type, state, or decision owned elsewhere;
- define only its own side of an interaction;
- link to the owner;
- list a verification case without redefining the behavior under test; or
- summarize a decision's status without copying its mechanics or rationale.

It must not restate the owned contract as an independent rule, add conditions
to it, or supply behavior missing from the owner. A duplicated normative claim
is a documentation defect even when both copies currently agree.

### Decision status and references

Each decision appears in exactly one current-status register. Apply these
content rules:

- `settled.md` gives only a concise outcome and a link to the focused owner;
  the owner contains the complete current behavior and interpretive rationale;
- `open.md` or `deferred.md` owns an unresolved choice's status, alternatives,
  analysis, and selection method; a focused owner may declare a locally
  relevant unresolved parameter or choice, state its settled constraints, and
  link to the applicable register;
- a rejected proposal appears only in `rejected.md`, with the concise reason it
  must not be reintroduced;
- a replaced design appears only in `superseded.md`, with a concise description
  of its replacement and a link to the current owner; and
- a status change moves the entry rather than copying it, and settlement
  updates the focused owner in the same change.

Do not reproduce a register entry in another register or document. The focused
owner may mark its current contract settled, but outside that owner references
must not repeat the contract's mechanics or rationale. Rejected alternatives
and superseded designs remain in their applicable register. Add a
cross-document reference only when it is necessary to understand the local
contract, verify it, or follow a lifecycle or component boundary. Such a
reference names and links the owner without summarizing its rules.
Compatibility material may map an obsolete term to its current term when that
translation is itself required. Do not add references solely for
discoverability.

A concise local open or deferred declaration is not duplication when omitting
it would make the focused contract appear complete, fixed, or accidentally
silent. Limit that declaration to the parameter or choice, its settled bounds,
its unresolved status, and a link to the status register. Keep alternatives,
candidate comparisons, selection procedures, and tuning rationale only in the
register.

Review reports and historical material may quote or summarize enough of a
claim to identify the evidence or finding, but must link to the current owner
when one exists and never become an alternate contract. Writing a report
required by an accepted review procedure, including a Pinned Upstream
Assumption Review, is evidence recording rather than a normative architecture
change.

For every documentation change that adds or changes a normative claim:

1. identify its sole owner before editing;
2. place the complete rule and its interpretive rationale only in that owner;
3. replace necessary mentions elsewhere with local interaction text and a
   link to the owner;
4. search the repository for the affected terms and inspect every normative
   occurrence for competing ownership; and
5. flag any unresolved ownership conflict instead of choosing silently.

Architecture fixes ownership defects in normative documents. Implementation
must stop and report conflicting owners rather than select one. Review reports
duplication or ambiguity as a finding and does not silently reconcile it.

## Audit material

`docs/history/audits/` contains historical reconciliation evidence and working
reports. Audit files are non-normative: they do not participate in
architecture precedence and must not be used as implementation or review
contracts. Trace every accepted audit outcome to the current architecture,
an accepted ADR, or the applicable decision-status register as appropriate.

Version control tracks the complete `docs/history/audits/` directory for
historical provenance. Tracking an audit does not give it normative authority.

## Frozen design archives

`docs/history/archives/` contains rejected or replaced design material frozen
at the point it left the current architecture. Version control tracks the
complete directory for provenance. Its files are non-normative and do not
participate in architecture, implementation, or review precedence.

After its initial commit, an archive file must never be edited, reformatted,
relinked, renamed, or deleted. Later corrections, commentary, or related
discarded material require a new archive file. All collaboration roles ignore
the directory unless the user explicitly asks them to inspect a specific
archive.

## Chat archives

`docs/chats/` contains local chat transcript archives used only as provenance
when an architecture reconciliation explicitly requires them. The entire
directory is ignored by Git and remains untracked. Chat archives are
non-normative and do not participate in architecture precedence.

## rusty-kaspa issue records

`docs/rk-issues/` contains local, one-file-per-issue records for rusty-kaspa
issues discovered during KGI v2 work. Version control tracks the complete
directory so the issue register is visible to the project, but no collaboration
role owns it. Architecture, Implementation, and Review roles must ignore its
contents unless the user explicitly asks them to work on a specific issue
record. These records are non-normative and do not participate in KGI
architecture, implementation, or review precedence.

## Architecture conflicts

If repository evidence conflicts with accepted architecture, flag the conflict. Do not resolve it implicitly. Record accepted architecture changes immediately in a focused architecture document or ADR.

## Architecture role

The Architecture role owns normative architecture documents and ADRs. It
preserves the distinction between architectural contracts and implementation
details, records accepted changes durably, and keeps the open and deferred
decision registers limited to their stated status.

The Architecture role does not implement production code unless explicitly asked.

## Implementation role

The Implementation role implements accepted architecture, tests, and migrations. It must read the applicable architecture before editing and must not invent or silently simplify architecture.

When implementation exposes an ambiguity or conflict, stop at the architectural boundary, document the evidence, and ask the Architecture role for a decision. Implementation choices that do not change accepted semantics may be recorded in code, tests, or `docs/implementation/status.md` as appropriate.

## Review role

The Review role evaluates stable commits or diffs against accepted architecture,
ADRs, and relevant tests. It is read-only unless explicitly asked to fix
findings. It may create the evidence report required by an accepted review
procedure, including a PUAR, without separate authorization. That exception
permits the report only, not edits to normative files.

Review findings belong in `docs/reviews/` when a durable report is requested. Findings and review notes are evidence, not normative architecture changes.

## Scope rules

- Statements marked settled in the architecture documents are accepted constraints.
- Rejected or superseded designs in `docs/decisions/rejected.md` and
  `docs/decisions/superseded.md` must not be reintroduced implicitly.
- Open implementation choices must preserve all settled contracts.
- KGI v2.1 candidates in `docs/future-work.md` are outside the KGI v2 implementation unless explicitly promoted through an architecture decision.
- Open architecture requirements must be resolved before dependent production
  work proceeds.
