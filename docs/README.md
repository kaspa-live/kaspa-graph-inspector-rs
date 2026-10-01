# KGI v2 documentation

This directory contains the KGI v2 architecture, decision records,
implementation planning, project evidence, and historical material.

## Current authority

The focused documents listed in
[`architecture/README.md`](architecture/README.md) collectively form the
normative KGI v2 architecture. Each contract has one focused owner. Accepted
architecture changes update that owner and the applicable decision register in
the same change.

Open and deferred decisions, implementation plans, reviews, audits, historical
handoffs, issue records, and chat archives do not silently alter the current
contract. The complete reading order and precedence rules are defined in
[`AGENTS.md`](../AGENTS.md).

## Document classes

| Area | Purpose | Current authority |
|---|---|---|
| `architecture/` | Current focused system contracts and ownership index | Normative according to `AGENTS.md` |
| `decisions/` | Settled, open, deferred, rejected, and superseded decision registers plus any standalone ADRs | Accepted ADRs and rejected/superseded status constrain work; current behavior remains in the focused owner |
| `future-work.md` | Work explicitly outside KGI v2 | Non-normative |
| `implementation/` | Planned implementation order, current status, and implementation analyses | Non-normative |
| `reviews/` | Review evidence and findings | Non-normative |
| `history/audits/` | Historical reconciliation evidence | Non-normative |
| `history/handoffs/` | Superseded architecture handoffs retained for provenance | Non-normative |
| `rk-issues/` | Tracked rusty-kaspa issue records | Non-normative and outside normal role scope |
| `chats/` | Untracked local chat provenance | Non-normative |

## Organization status

The documentation reorganization, authority cutover, and historical moves are
complete.
