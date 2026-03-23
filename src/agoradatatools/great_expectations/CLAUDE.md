<!-- Last reviewed: 2026-03 -->

## Project

Great Expectations integration for data validation. Contains expectation suites (JSON), custom expectation plugins (Python), JSON schemas for nested field validation, and the GX runner that orchestrates validation.

## Architecture

```
gx.py (runner)
  ├── gx/expectations/*.json         — 20 expectation suite definitions (GENERATED)
  ├── gx/json_schemas/*/*.json       — JSON schemas for nested field validation
  ├── gx/plugins/expectations/*.py   — 8 custom expectation classes + utils.py
  ├── gx/checkpoints/*.yml           — checkpoint templates
  └── gx/great_expectations.yml      — GX context configuration
```

Source notebooks that generate expectation suites live in `gx_suite_definitions/*.ipynb` at repo root.

## Conventions

### Custom expectations — side-effect imports

Custom expectation classes are imported in `gx.py.__init__()` purely for their side effects — the imports register the expectations with GX's internal registry. The imported objects are not stored or referenced. Do not remove these "unused" imports.

Two categories of custom expectations:
- **ColumnMapExpectation** (per-row validation, returns boolean): `ExpectColumnValuesToHaveListLength`, `ExpectColumnValuesToHaveListLengthInRange`, `ExpectColumnValuesToHaveListMembers`, `ExpectColumnValuesToHaveListMembersOfType`, `ExpectColumnValuesToHaveListOfDictWithExpectedValues`
- **ColumnAggregateExpectation** (column-level ratio): `ExpectColumnNestedObjectNotNull`, `ExpectColumnNestedObjectStringLength`, `ExpectColumnNestedObjectRegexRule`

Aggregate expectations use threshold parameters (strictly between 0 and 1) and return ratios rounded to 2 decimals.

### Nested column handling

Columns declared in config `gx_nested_columns` contain Python dicts/lists in memory. Before GX validation, `GreatExpectationsRunner.convert_nested_columns_to_json()` converts them to JSON strings. Custom expectations then use `safe_parse()` from `plugins/expectations/utils.py` to deserialize.

`safe_parse` edge cases:
- String `"null"` → returns `[]`
- Parsed non-list → returns `[]`
- Empty lists → ignored entirely in aggregate counts (not counted as failures)

### Warnings vs failures

In `set_warnings_and_failures()`:
- **Warning**: `result["success"] == True` but `partial_unexpected_list` is non-empty
- **Failure**: `result["success"] == False`

### Silent pass for missing suites

If no expectation suite exists for a dataset, `run()` returns early without error — the dataset passes validation silently.

## Constraints

- Never edit JSON files in `gx/expectations/` — regenerate from `gx_suite_definitions/*.ipynb` notebooks instead
- `gx.py` is excluded from ruff and vulture pre-commit hooks — because GX internals trigger false positives
- GX logger is set to WARNING level globally to suppress verbose output
- JSON schemas use Draft 2019-09 — use `allOf` for multiple constraints (AND logic, not OR)
- Checkpoint `expectation_suite_name` and `run_name_template` are empty in YAML — filled at runtime
- `#ephemeral_pandas_asset` in checkpoints indicates data is passed at runtime, not pre-configured
- Custom expectation `member_type` parameter accepts string names ("int", "float", "str", etc.), not Python types
