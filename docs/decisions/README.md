# Architecture decision register

This directory indexes KGI v2 decisions by current status. The project-wide
[decision status and references policy](../../AGENTS.md#decision-status-and-references)
defines what each entry may contain, how status changes are recorded, and when
another document may reference a decision.

| File | Meaning |
|---|---|
| [settled.md](settled.md) | Accepted outcomes and their current architecture owners. |
| [open.md](open.md) | Requirements that still need an architecture decision. |
| [deferred.md](deferred.md) | Constrained implementation choices intentionally left to implementation work. |
| [rejected.md](rejected.md) | Proposals considered and never accepted. |
| [superseded.md](superseded.md) | Former designs replaced by a later accepted design. |

`docs/future-work.md` remains separate: it contains work outside KGI v2 rather
than unresolved v2 decisions.
