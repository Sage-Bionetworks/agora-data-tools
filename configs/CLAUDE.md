<!-- Last reviewed: 2026-03 -->

## Project

YAML configuration files that drive the ETL pipeline. Each config defines which Synapse entities to extract, how to transform them, and where to upload results.

## Conventions

### Two config families

- `agora_*.yaml` — Gene/protein data for Agora Explorer (~15 datasets)
- `model_ad_*.yaml` — Mouse model data for Model AD Explorer (~20 datasets)

Each family has `*_preprod.yaml` (staging/testing) and `*_prod.yaml` (production) variants. Preprod and prod configs point to different Synapse destination folders but share the same transform logic.

### Config structure

```yaml
destination: &dest syn12345678        # Synapse folder ID (YAML anchor for reuse)
staging_path: ./staging               # Local temp directory
gx_folder: syn12345679                # GX HTML reports destination
gx_table: syn12345680                 # GX structured results table
team_images_id: syn12345681           # Optional, agora only

sources:                              # Source file definitions with anchors
  - source_group_name:
    anchor_name: &anchor_name
      - name: entity_name             # Key used in datasets dict
        id: syn12345678.3             # Synapse ID with version
        format: csv                   # csv, tsv, feather, json, yaml, table

datasets:                             # Dataset processing definitions
  - dataset_name:
      files: *anchor_name             # Reference source anchor
      final_format: json              # Output format
      custom_transformations: transform_function_name  # Must match __init__.py
      column_rename:                  # Applied BEFORE transform
        old_col: new_col
      agora_rename:                   # Applied AFTER transform
        old_col: new_col
      destination: *dest              # Where to upload output
      gx_enabled: true                # Run Great Expectations validation
      gx_nested_columns:              # Columns containing nested structures
        - nested_col_name
      provenance:                     # Additional lineage tracking IDs
        - syn12345678.3
```

### YAML anchors

Anchors (`&name`) and references (`*name`) are used heavily for DRY config. Sources define anchors, datasets reference them. The `destination` field commonly uses an anchor defined at the top level.

### Key distinctions

- `column_rename` runs **before** the transform function — use for standardizing input column names
- `agora_rename` runs **after** the transform function — use for renaming output columns to match frontend expectations
- `custom_transformations` can be a string (function name only) or a dict (function name → params dict) for transforms that need config-defined parameters
- `gx_nested_columns` must list every column containing nested dicts/lists — these get converted to JSON strings before GX validation

## Constraints

- Synapse entity IDs MUST include version numbers (`syn12345678.3`) — versionless IDs will fetch the latest version, which breaks reproducibility
- When the same Synapse ID appears in both `files` and `provenance`, their versions must match — `check_provenance_id_file_id_consistency` validates this at runtime
- The `custom_transformations` value must exactly match a function name exported in `src/agoradatatools/etl/transform/__init__.py`
- File `format` must match the actual file type — extract validates extensions strictly
