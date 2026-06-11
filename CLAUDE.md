# Claude Instructions for agora-data-tools

## Project Rules

At the start of any task, read all rule files in `.cursor/rules/` before planning or writing code:

```
Glob: .cursor/rules/**/*.mdc
```

These files contain authoritative step-by-step workflows, constraints, and pitfalls for this project (e.g. GX validation suite authoring). They are not derivable from the code alone — missing them leads to incorrect approaches and rework.
