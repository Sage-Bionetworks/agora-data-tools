<!-- Last reviewed: 2026-03 -->

## Project

Core ETL modules: extract data from Synapse, transform via pluggable functions, serialize to JSON/CSV, and upload back to Synapse.

## Conventions

### extract.py — Format-based dispatch

`get_entity_as_df(syn_id, source, syn)` dispatches to format-specific readers based on `source` parameter:
- `csv`, `tsv`: `pd.read_csv()` with `float_precision="round_trip"` — preserves exact float values, prevents rounding errors
- `feather`: `pd.read_feather()`
- `json`: `pd.read_json()`
- `table`: `syn.tableQuery()` then `.asDataFrame()`
- `yaml`: Returns DataFrame with columns `"key"` and `"items"`. If all values are lists, the DataFrame is **exploded** so each list item becomes a row. This is fundamentally different from other formats in shape.

Synapse IDs include versions via dot notation (`syn12345.4`). The version is parsed by splitting on `.` and passed to `syn.get()`.

File extension validation is strict — reading a `.tsv` file with `source="csv"` raises ValueError.

### load.py — Three serialization paths

Different return types from transforms get serialized differently:
- `df_to_json(data_as_df)`: DataFrame → `replace({np.nan: None})` → `to_dict("records")` → `remove_non_values()` → JSON
- `dict_to_json(data_as_dict)`: Dict → wrapped in single-element list → `remove_non_values()` → JSON
- `list_to_json(data_as_list)`: List of dicts → JSON as-is (no null removal)

The NaN→None replacement in `df_to_json` is critical — pandas `to_dict("records")` preserves NaN, which is not valid JSON.

### NumpyEncoder

Custom JSON encoder that handles `np.integer` → `int`, `np.floating` → `float`, `np.ndarray` → `list`. Required because standard `json.dumps` cannot serialize numpy types.

### remove_non_values()

Recursively removes null/NaN keys from nested dicts and lists. Behavior:
- Dict values that are None/NaN: removed entirely
- Dict values that are empty dicts (after recursion): removed
- List elements that are None/NaN: removed
- List elements that are empty dicts (after recursion): removed
- Non-null values: preserved

### load() — Synapse upload with provenance

`load(file_path, provenance, destination, syn)` wraps the file in a Synapse `Activity` for data lineage tracking. Returns `(file_id, file_version)` tuple.

## Constraints

- `float_precision="round_trip"` in CSV reading is intentional — do not remove. Prevents silent float rounding.
- `dict_to_json` wraps a single dict in a list for JSON output — downstream consumers expect this.
- `list_to_json` does NOT run `remove_non_values()` — only `df_to_json` and `dict_to_json` do. If your transform returns list[dict], handle nulls in the transform itself.
