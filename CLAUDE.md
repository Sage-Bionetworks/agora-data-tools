# CLAUDE.md

Guidance for Claude Code working in this repository.

## Project conventions live in `.cursor/rules/`

This repo stores its working conventions as rule files in `.cursor/rules/*.mdc` (shared with
Cursor users). **These files are the source of truth. Before doing related work, open and read the
relevant rule file in full and follow it.** Read the current file each time rather than relying on
memory of it — these rules are updated over time, and each branch may have its own version.

| Rule file | When it applies |
|---|---|
| `.cursor/rules/git-commit.mdc` | **Always** — before any `git commit` |
| `.cursor/rules/python-env.mdc` | **Always** — before running any code or tests (activate the correct environment and `pip install .`) |
| `.cursor/rules/gx-validation-suite-authoring.mdc` | Before authoring or modifying any Great Expectations (GX) validation suite — i.e. any work touching `gx_suite_definitions/`, `src/agoradatatools/great_expectations/`, or GX settings in `configs/*.yaml` |

### Great Expectations work

When a task involves adding or changing GX expectations, read
`.cursor/rules/gx-validation-suite-authoring.mdc` first and follow it. In particular, note its
**"Keep Value Sets in Sync with the ui_config Notebooks"** section: whenever you add or change an
`expect_column_values_to_be_in_set` or `expect_column_values_to_have_list_members` expectation,
check the ui_config R notebooks for a parallel filter-value list and prompt the user to keep the
two aligned (and to record any newly discovered pair in that rule's "Parallel lists" tables).
