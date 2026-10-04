# rusty-kaspa issues found during KGI v2 development

This directory holds local records of rusty-kaspa issues discovered while
developing KGI v2.

- Use one Markdown file per issue.
- Give each file a short descriptive kebab-case name.
- Record the affected rusty-kaspa revision, observed behavior, expected
  behavior, reproduction evidence, KGI impact, and any upstream issue or fix.
- Record the global impact, KGI v2 impact, overall priority, and a short
  rationale for both impact ratings.
- Keep KGI architecture decisions in the normative KGI documents rather than
  relying on these records.

## Priority ratings

Every issue uses two impact ratings and one derived priority:

```text
Global impact: G0-G3
KGI v2 impact: K0-K3
Priority: P0-P3
```

### Global impact

- **G0 — Critical:** consensus, security, corruption, or widespread node
  failure.
- **G1 — High:** incorrect public behavior or data loss that can affect
  multiple downstream clients.
- **G2 — Moderate:** bounded reliability, compatibility, or performance
  problem with a safe workaround.
- **G3 — Low:** diagnostics, observability, ergonomics, or minor optimization.

### KGI v2 impact

- **K0 — Blocker:** KGI correctness cannot be guaranteed and no safe
  workaround exists.
- **K1 — High:** the issue can break stream continuity or force Resync/Rebuild;
  a safe but expensive recovery exists.
- **K2 — Moderate:** the issue adds bounded complexity, latency, or unnecessary
  recovery without threatening correctness.
- **K3 — Low:** convenience, diagnostics, or future improvement with no
  material v2 effect.

### Overall priority

Use the more urgent of the two impact ratings:

```text
P = min(global rating number, KGI rating number)
```

For example, `G2 + K1` gives `P1`. Each issue must briefly justify both impact
ratings. Likelihood belongs in that rationale and does not silently lower the
impact rating.

Version control tracks this directory so the issue register is visible to the
project, but no collaboration role owns it. Architecture, Implementation, and
Review roles must ignore it unless the user explicitly asks them to work on a
specific issue record. The records are non-normative and do not participate in
KGI architecture, implementation, or review precedence.
