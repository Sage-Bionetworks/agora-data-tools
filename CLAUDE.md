<!-- Last reviewed: 2026-03 -->

## Project

Configuration-driven ETL pipeline for Agora (gene/protein research data) and Model AD (mouse model data). Extracts tabular data from Synapse, applies transforms, validates with Great Expectations, and uploads JSON to Synapse. Powers the Agora and Model AD Explorer web applications.

## Stack

- Python 3.10-3.11 (`python_requires = >=3.10, <3.12`)
- pandas ~2.0, numpy ~1.21, pyarrow ~14.0.1
- synapseclient 4.9.0 (Synapse API client)
- Great Expectations 0.18.1 (data validation)
- Typer CLI (entry point: `adt`)
- Pipenv for dependency management

## Commands

```bash
pipenv install --dev                              # Install all dependencies
pipenv shell                                      # Activate virtualenv
pytest tests/ --cov=agoradatatools --cov-report=html  # Run tests with coverage
pre-commit run --all-files                        # Run all linting/formatting hooks
adt configs/agora_preprod.yaml                    # Run pipeline locally (no upload)
adt configs/agora_preprod.yaml --upload --token $SYNAPSE_AUTH_TOKEN  # Run with upload
```

## Architecture

Pipeline flow for each dataset defined in config YAML:

1. **Extract**: Load source files from Synapse as DataFrames (`extract.get_entity_as_df`)
2. **Standardize**: `standardize_column_names` (lowercase, clean special chars) + `standardize_values` (replace N/A with NaN) — applied automatically to ALL extracted data
3. **Pre-rename**: Apply `column_rename` from config (before transform)
4. **Transform**: Dispatch `custom_transformations` function by name from config via `apply_custom_transformations` in `process.py`
5. **Post-rename**: Apply `agora_rename` from config (after transform)
6. **Serialize**: Convert to JSON — handles DataFrame, list[dict], or dict return types
7. **Validate**: Run Great Expectations if `gx_enabled: true` in config
8. **Upload**: Push JSON to Synapse destination folder if `--upload` flag set

Transform functions are registered in `src/agoradatatools/etl/transform/__init__.py`. The config `custom_transformations` value must match the registered function name exactly. The dispatcher in `process.py` introspects function signatures to pass only matching parameters (`df`, `datasets`, `dataset_name`).

See `configs/CLAUDE.md` for config file conventions and `src/agoradatatools/etl/CLAUDE.md` for extract/load details.

## Conventions

- Transform results can be `DataFrame`, `list[dict]`, or `dict` — the pipeline serializes each differently. Match return type to existing transforms of the same kind.
- `column_rename` runs pre-transform, `agora_rename` runs post-transform. Use the right one based on when renaming should happen relative to the transformation logic.
- `_`-prefixed functions in `etl/utils.py` (`_login_to_synapse`, `_get_config`) are NOT truly private — they are used across modules. There is a TODO to remove the prefix.
- `gx.py` is excluded from ruff and vulture pre-commit hooks — because it interfaces with Great Expectations internals that trigger false positives.
- Boolean columns use `is_*` naming convention (e.g., `is_igap`, `is_tep`, `is_eqtl`).
- Model AD transforms use `name` (not `model`) as the primary model identifier column — this was renamed in PR #202.
- Docstring coverage enforced at 85% by interrogate (pre-commit hook). Add docstrings to all new public functions.

## Data Models

Key data flow patterns:

- **Nested JSON structures**: Several transforms use `nest_fields()` from `etl/utils.py` to collapse DataFrame rows into nested dicts/lists grouped by a key column (e.g., `ensembl_gene_id`). The `nested_field_is_list` parameter controls whether the nested value is a list of dicts or a single dict.
- **JSON schemas**: Located in `src/agoradatatools/great_expectations/gx/json_schemas/`. Define expected shapes for nested fields (druggability, ensembl_info, target_nominations, median_expression, gene_biodomains, team members, distribution scores).
- **GX expectation suites**: 20 JSON files in `great_expectations/gx/expectations/` — generated from `gx_suite_definitions/*.ipynb` notebooks. 8 custom expectation plugins in `gx/plugins/expectations/`.

## Constraints

- Never edit JSON files in `src/agoradatatools/great_expectations/gx/expectations/` — they are generated from `gx_suite_definitions/*.ipynb` notebooks. Edit the notebooks instead.
- Never commit contents of `staging/` — it contains generated output.
- `standardize_values` regex patterns MUST use `^...$` anchors (e.g., `r"^N/A$"`) — because without anchors, "N/A" substrings within legitimate text get replaced (e.g., "Snx1*D465N/APOE4" contains "N/A").
- Synapse entity IDs include versions (`syn12345.3`) — provenance IDs and file IDs referencing the same entity must have matching versions. `check_provenance_id_file_id_consistency` validates this.
- `normalize_null_values` has three tiers: boolean columns → `False`, string columns → `""`, all remaining columns → `None`. The tiers must not overlap.
- `nest_fields` with `nested_field_is_list=False` raises `ValueError` if multiple rows exist per group — only use for guaranteed one-to-one relationships.
- JAX IDs must be 6-digit zero-padded strings (e.g., `"004807"` not `4807`) — use `zero_pad_jax_ids()` from `model_ad_transform_utils.py`.
- `data_analysis/` contains Jupyter notebooks excluded from all linting — these are exploratory and not part of the pipeline.

## Testing

See `tests/CLAUDE.md` for testing conventions.

CI runs 4 sequential jobs: pre-commit → pytest (matrix: 3.10, 3.11) → integration build (runs `adt` against preprod and model_ad configs) → Docker publish to GHCR.

## Related Systems

- **Synapse** (synapse.org): Data repository where source files are stored and output JSON is uploaded. Authentication via `SYNAPSE_AUTH_TOKEN` env var or `--token` CLI flag.
- **Agora Explorer**: Web app consuming the JSON output of agora configs.
- **Model AD Explorer**: Web app consuming the JSON output of model_ad configs.
- **AD Knowledge Portal** (adknowledgeportal.synapse.org): Referenced in generated URLs for study data links.
