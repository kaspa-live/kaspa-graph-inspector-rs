# Reviews

Review reports belong in this directory. They are non-normative evidence about
stable commits or diffs and do not amend architecture contracts or ADRs.

## Single ownership in reports

Reports follow the project-wide
[single ownership policy](../../AGENTS.md#single-ownership-policy). A report
may summarize a requirement only as needed to identify what was checked or to
explain a finding, and must link that requirement to its normative owner. It
must not complete, reinterpret, or duplicate a missing architecture contract.

If review finds duplicated, conflicting, or ambiguous ownership, that is a
finding for the Architecture role. The report records the evidence and does
not choose a replacement contract or silently reconcile the owners.

Reports produced by a
[Pinned Upstream Assumption Review](../architecture/verification.md#pinned-upstream-assumption-review-policy)
use one file per run:

```text
YYYY-MM-DD-rusty-kaspa-<short-sha>-assumptions.md
```

Each report records source-analysis results for one exact full rusty-kaspa SHA.
Creating that required report is permitted evidence recording by the Review
role. It does not change the pinned reference revision, amend any KGI contract,
or give the report normative ownership of an upstream assumption.
