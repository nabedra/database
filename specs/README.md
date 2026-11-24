# Design specs

TLA+ specs of Nabedra's designs, one folder per module. The
[design review skill](../.agents/skills/design-review/SKILL.md) says when a
plan needs a spec and how to check it. [Writing specs](../.agents/skills/design-review/references/tla.md)
covers the layout and conventions.

## Tools

| Tool | Version | Use |
| --- | --- | --- |
| TLC (`tla2tools.jar`) | Not pinned yet | Required checker |
| Apalache | Not pinned yet | Symbolic checks of larger bounds, when needed |
| TLA+ Community Modules | Not pinned yet | `Json` and `IOUtils` for trace validation |

The tooling pull request pins each version with its SHA-256 and adds the
Docker image the `checks` job uses. Until then, results are
`local, unattested`. See the [tooling status](../.agents/skills/design-review/SKILL.md#tooling-status).

## Specs

| Module | Spec | Guarantees | T checks | Plan |
| --- | --- | --- | --- | --- |

No spec exists yet. Existing protocols get one on next touch.
