<!-- Last reviewed: 2026-03 -->

## Project

Transform modules for the ADT ETL pipeline. Each module implements a specific data transformation. There are two families: Agora transforms (gene/protein data) and Model AD transforms (mouse model data).

## Conventions

### Registration

Every new transform function must be:
1. Imported in `__init__.py`
2. Added to the `__all__` list in `__init__.py`

The function name in `__init__.py` must exactly match the `custom_transformations` value in the config YAML.

### Function signatures

The dispatcher in `process.py` uses `inspect.signature()` to introspect each transform's parameters and passes only matching ones from this set:
- `df`: The primary dataset DataFrame (keyed by `dataset_name` in the datasets dict)
- `datasets`: Dict of all DataFrames loaded for this dataset config entry
- `dataset_name`: String name of the current dataset

Additional parameters can be defined in config under `custom_transformations` as a dict (function_name → params dict). These are merged with the standard params.

### Return types

- Most transforms return `pd.DataFrame` (e.g., `transform_gene_info`, `transform_overall_scores`, `transform_proteomics`)
- `transform_disease_correlation`, `transform_model_overview`, `transform_model_details` return `list[dict]`
- Match the return type to existing transforms of the same kind — the pipeline serializes each type differently

### Input validation

Use `check_required_datasets_and_columns(datasets, required_input)` from `etl/utils.py` at the top of transforms that accept multiple datasets. Define a `REQUIRED_INPUT` dict constant mapping dataset names to required column lists.

### Null handling

Use `normalize_null_values(df, boolean_columns=[], empty_string_columns=[])` from `etl/utils.py` to standardize NaN/None values before returning. Three tiers: boolean → False, string → "", everything else → None.

## Reusable Utilities

### In `etl/utils.py`:
- `nest_fields(df, grouping, new_column, drop_columns, nested_field_is_list)` — Collapse rows into nested dicts/lists grouped by column(s). Use `nested_field_is_list=False` only for guaranteed 1:1 relationships.
- `calculate_distribution(df, grouping, distribution_column)` — Compute quartile stats with IQR-adjusted min/max. Returns DataFrame with min, max, first_quartile, median, third_quartile.
- `check_required_datasets_and_columns(datasets, required_input)` — Validate datasets and columns exist before processing.
- `delim_string_to_list(str_obj, delim)` — Convert comma-delimited string to list. Use with `.apply()` on Series.
- `normalize_null_values(df, boolean_columns, empty_string_columns)` — Three-tier null normalization.
- `flatten_list(lst)` — Recursively flatten nested lists.
- `remove_duplicates_keep_order(lst)` — Deduplicate preserving order.
- `convert_numpy_types(obj)` — Recursively convert numpy types to Python natives for JSON.
- `normalize_zero(value)` — Convert -0.0 to 0.0.
- `extract_age_numeric(age)` — Extract numeric value from age strings like "8 months".

### In `model_ad_transform_utils.py` (Model AD only):
- `zero_pad_jax_ids(series)` — Convert JAX IDs to 6-digit zero-padded strings. Handles Int64/float64 quirks.
- `preprocess_model_info(model_info_df, model_results_df)` — Common preprocessing: merge, fillna, pad JAX IDs, convert delim strings to lists.
- `build_gene_expression_url(row)` — Build comparison URL with optional categories and model list.
- `process_genetic_info(model_info_df, allele_info_df, human_transgene_map_df)` — Map human transgene alleles to ensembl IDs, override gene_symbol when human_ensembl_id is available.

## Architecture

- `gene_info.py` is the most complex transform — merges 10+ source datasets on `ensembl_gene_id` with multiple nested fields (target_nominations, median_expression, druggability, biodomains)
- `immunohisto_transform.py` is shared between standalone immunohisto output and `model_details` (biomarkers/pathology sections). It adds missing age entries to ensure complete coverage.
- `model_overview.py` and `model_details.py` both use `model_ad_transform_utils.py` for shared preprocessing
