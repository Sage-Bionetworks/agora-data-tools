# RNA DE Individual Transform

## Overview

The `rna_de_individual` transform processes individual-level RNA expression (normalized expression) data from Model AD mouse models. It transforms raw individual expression measurements into a structured format that groups data by gene, tissue, and effective_model_group, with individual data points organized by age.

**Module Location:** `src/agoradatatools/etl/transform/rna_de_individual.py`

## Purpose

This transform serves to:
1. Consolidate individual RNA expression data from multiple mouse models
2. Group individual measurements by model group to support comparison displays (e.g., one model vs. one control, or multiple related models sharing the same control group)
3. Enrich raw data with gene symbols, genotype display labels, and model metadata
4. Create age-based groupings with individual data points for visualization

## Required Input Datasets

The transform requires three types of input:

### 1. `rnaseq_genotype_label_map`
**Required columns:**
- `model`: Model name (e.g., "5XFAD (UCI)", "Jax.IU.Pitt_APOE4")
- `genotype`: Genotype identifier in the data (e.g., "5XFAD_carrier", "APOE4_carrier")
- `display_label`: Human-readable label for the genotype (e.g., "5xFAD (UCI)", "APOE4")
- `model_group`: Grouping of models for display purposes (e.g., "5XFAD", "APOE4"; may be empty string)
- `result_order`: Integer determining display order (lower values indicate controls)

**Purpose:** Maps genotype identifiers to human-readable labels and organizes models into groups.

### 2. `mouse_gene_metadata`
**Required columns:**
- `ensembl_gene_id`: Ensembl gene identifier (e.g., "ENSMUSG00000000001")
- `gene_symbol`: Gene symbol (e.g., "Gnai3")

**Purpose:** Provides gene symbols for Ensembl gene IDs.

### 3. Data Files (one or more CSV files)
**Required columns** (defined by the `DATA_FILE_REQUIRED_COLUMNS` module constant):
- `ensembl_gene_id`: Ensembl gene identifier
- `expression`: Normalized expression value (Log2 Counts per Million)
- `model`: Model name (must match values in rnaseq_genotype_label_map)
- `genotype`: Genotype identifier (must match values in rnaseq_genotype_label_map)
- `age`: Age timepoint as string (e.g., "3 months", "6 months")
- `sex`: Sex identifier (e.g., "M", "F")
- `tissue`: Tissue name (e.g., "Right Cerebral Hemisphere", "hippocampus")
- `individualid`: Unique identifier for each individual sample

**Purpose:** Contains individual expression measurements for each sample.

## Data Processing Pipeline

### Step 1: Metadata Preparation

1. **Gene Metadata Dictionary Creation** (`create_gene_metadata_dict`)
   - Imported from `rna_de_individual_utils` module
   - Maps Ensembl gene IDs to gene symbols
   - **Purpose:** Enriches output with human-readable gene names

### Step 2: File Grouping and Preprocessing

Before transform-specific processing, the main function groups input files by their `effective_model_group` and preprocesses each file individually.

#### 2.1 File Grouping by effective_model_group
- Reads the `model` column of each input file to determine its `effective_model_group` using the genotype metadata lookup
- Groups files with the same `effective_model_group` together (e.g. UCI models whose data is split across two CSV files)
- Single-file groups are processed without any concatenation overhead
- This strategy keeps memory usage proportional to the largest group rather than the total dataset size
- **Single-model-per-file validation:** After grouping, checks whether any file contains rows from more than one `effective_model_group`. If so, a `ValueError` is raised identifying the file and the conflicting groups. This is a hard failure because `result_order` and `matched_control` cannot be computed correctly when a file spans multiple groups (see [Key Assumptions: Single Model per File](#8-single-model-per-file))

#### 2.2 Common Preprocessing (`preprocess_data_file`)
Applied to each file individually before it is combined within its group:
- **Logging:** Logs file name, global index, row count, column count, and memory usage
- **Empty file validation:** Raises error if file is empty
- **Column validation:** Checks all required columns are present (defined by `DATA_FILE_REQUIRED_COLUMNS`)
- **Gene filtering:** Filters to mouse genes only (keeps `ENSMUSG*`, removes `ENSG*`)
- **Numeric rounding:** Rounds all numeric columns to 5 decimal places

After all files in a group are preprocessed, they are concatenated (via `pd.concat`) into a single DataFrame that is passed to `_process_individual_data_file_core`. Memory is explicitly freed (via `del` and `gc.collect()`) after each group is processed.

#### 2.3 Transform-Specific Processing (`_process_individual_data_file_core`)

After preprocessing and concatenation, the individual transform applies its specific logic:

**Genotype Enrichment (Vectorized Merge):**
- Converts genotype metadata dictionary to DataFrame
- Performs left join on `(model, genotype)` to add:
  - `display_label`: Human-readable genotype label
  - `result_order`: Ordering value for display
  - `model_group`: Explicit model group (empty string if none)
  - `effective_model_group`: `model_group` when set, otherwise `model` name
- **Fallback for unmapped genotypes:**
  - `display_label` = original `genotype` value
  - `result_order` = 999 (treated as non-control)
- **Merge validation:** Uses `validate="many_to_one"` to ensure each `(model, genotype)` maps to exactly one label

**name Field Assignment:**
- `name` is set to `effective_model_group` (the model_group when explicitly set, or the model name for solo models)
- This consolidates multi-file model_groups (e.g. all UCI models sharing "Trem2-R47H_NSS") under a single display name while preserving solo-model names

**Dropping Unmatched Rows:**
- After the left merge with the label map, any row whose `(model, genotype)` pair had no match receives NA for `effective_model_group`
- Those rows are removed with `dropna(subset=["effective_model_group"])`
- **Purpose:** Ensures only genotype combinations that exist in the label map are processed
- **Example:** If model_group "5XFAD" has genotypes ["5XFAD_carrier", "5XFAD_noncarrier"], any rows with a different genotype receive NA and are dropped
- **All-rows-filtered case:** If every row is dropped (i.e., no genotype in the file matched the label map at all), a `WARNING` is logged and the function returns `[]` for that group. This is distinct from an empty input file (which raises `ValueError`) — it means the file had data but none of its genotypes were recognised.

### Step 3: Grouping and Output Entry Creation

#### 3.1 Grouping Strategy
- Groups data by all five columns: `(ensembl_gene_id, tissue, name, age, model_group)`
  - `ensembl_gene_id`: Ensembl gene identifier
  - `tissue`: Tissue name (post-mapping and sentence-case normalization)
  - `name`: Set to `effective_model_group` — the `model_group` when explicitly provided, or the model name for solo models
  - `age`: Age timepoint (e.g., `"4 months"`, `"12 months"`)
  - `model_group`: Explicit model group name (normalized to `None` if empty)
- Each unique combination of these five columns defines one output row
- **Design decision:** `name` is set to `effective_model_group` rather than the raw model name so that all models sharing the same `model_group` (including data split across multiple input files) produce a single consolidated output entry

#### 3.2 Transform-Specific Processing (`_process_individual_data_file_core`)

For each grouped combination, this function directly creates output entries (one per age timepoint) with:

**Gene Information:**
- `ensembl_gene_id`: Original Ensembl ID
- `gene_symbol`: Gene symbol from metadata (empty string if not found)

**Tissue Information:**
- `tissue`: Tissue name with transformations applied:
  - **Special transformation:** "Right Cerebral Hemisphere" → "Hemibrain"
  - **Sentence case conversion:** All tissue names converted to sentence case (e.g., "hippocampus" → "Hippocampus", "CORTEX" → "Cortex")

**Model Information:**
- `name`: `effective_model_group` value — equals `model_group` when explicitly set, or the model name for solo models
- `model_group`: Explicit model group name (normalized to `None` if empty string)

**Control Identification:**
- `matched_control`: Display label of the control genotype
- **Logic:** Finds the genotype with the LOWEST `result_order` value present in the actual data
  - Scans all data in the group
  - Identifies minimum result_order value
  - Selects corresponding genotype's display label
- **Assumption:** Lower result_order values always represent controls

**Display Ordering:**
- `result_order`: List of display labels in correct order for this model_group
- **Determination logic:**
  - Scans all genotypes belonging to the effective model_group in metadata
  - Sorts by result_order value (ascending)
  - Returns list of display labels in sorted order
  - **Purpose:** Enables consistent genotype ordering in visualization

**Age Information:**
- `age`: Age timepoint as string (e.g., "3 months")
- `age_numeric`: Numeric age value extracted from age string for sorting

**Measurement Information:**
- `units`: Fixed value "Log2 Counts per Million"
- `data`: List of individual data points containing:
  - `genotype`: Display label (from `display_label`, renamed for output)
  - `sex`: Sex identifier
  - `individual_id`: Sample identifier (converted to string)
  - `value`: Expression value (converted to float)

**Processing Steps:**
1. Groups the input data by age within each (gene, tissue, effective_model_group) combination
2. For each age group, creates a complete output entry with all metadata fields
3. Sorts output entries by numeric age value for consistent ordering
4. Returns one output entry per age timepoint

**Unnesting Decision:** Unlike some transforms that nest age data, this transform creates **one output entry per age** (unnested structure). Each entry has a single age with its associated data points.

### Step 4: Consolidation
- Combines output entries from all processed effective_model_groups
- Returns single list of all entries

## Key Assumptions

### 1. Gene Filtering
- **Assumption:** Input data may contain both mouse and human genes
- **Decision:** Only mouse genes (ENSMUSG*) are relevant; human genes (ENSG*) are filtered out

### 2. Result Order and Control Identification
- **Assumption:** Lower `result_order` values always represent control genotypes
- **Assumption:** The genotype with the minimum result_order in actual data is the matched control
- **Authoritative source:** `result_order` from `rnaseq_genotype_label_map` is the sole signal used to determine which genotype is the control. Any external model_info file is not consulted. Currently, `result_order` assignments and model_info file designations agree for all `effective_model_groups` with only two genotypes, but `result_order` takes precedence if they ever diverge.
- **Limitation for 4-genotype UCI studies:** Some DE analyses pair each case genotype with a *different* control (e.g., `Trem2-R47H_NSS.5xFAD` vs `Trem2-R47H_NSS` rather than vs `C57BL/6J`). The `matched_control` field cannot represent this per-case-genotype pairing — it always contains the single genotype with the lowest `result_order` for the group, which is a simplification for these multi-control scenarios.
- **Implication:** If data is missing control samples, the matched_control field may be empty

### 3. Model Grouping Strategy
This transform is designed to handle two distinct experimental scenarios:

**Scenario A: Single Model vs. Control**
- One model compared to one control (e.g., `Jax.IU.Pitt_APOE4` vs. `C57BL/6J`)
- The `model_group` may be empty or equal to the model name
- Display shows one experimental genotype vs. one control genotype

**Scenario B: Multiple Related Models Sharing Controls**
- Multiple variants of the same model type share a common control group
- Example: Several 5XFAD variants (`5XFAD (UCI)`, `5XFAD (JAX)`, etc.) all use the same control genotypes
- The `model_group` field (e.g., "5XFAD") links these related models together
- Display can show multiple model variants alongside their shared controls

**Implementation Details:**
- **Effective Model Group:** When `model_group` is empty, the model itself serves as its own group (`effective_model_group = model`)
- **Name Field:** The `name` field is set to `effective_model_group`, consolidating multi-file model_groups (e.g. UCI models) under a single display name while preserving solo-model names
- **File Grouping:** Input files are first grouped by `effective_model_group`; only files within the same group are concatenated before processing
- **Grouping Key:** Data is grouped by `(ensembl_gene_id, tissue, name, age, model_group)` to produce one consolidated output entry per group, regardless of how many input files contributed data

### 4. Genotype Mapping Completeness
- **Assumption:** Most genotypes in data files have entries in rnaseq_genotype_label_map
- **Fallback:** Unmapped genotypes use the original genotype value as display_label
- **Treatment:** Unmapped genotypes get result_order=999 (treated as non-controls)

### 5. Tissue Name Standardization
- **Assumption:** JAX models use "Right Cerebral Hemisphere" which should be standardized
- **Transformation:** "Right Cerebral Hemisphere" → "Hemibrain"
- **Sentence case conversion:** All tissue names are converted to sentence case for consistency
- **Purpose:** Ensures consistency across different data sources and standardizes capitalization

### 6. Age Format
- **Assumption:** Age values follow the format `"[N] months"` (e.g., `"3 months"`, `"6 months"`), where `N` is a non-negative integer. Every age string in the data **must** contain at least one digit.
- **Constraint (hard failure):** The `age_numeric` field is derived by extracting the first digit sequence from the `age` string (regex `\d+`) and casting it to `int`. If any age value contains no digits (e.g., `"string"`, `"P7"`, or a blank string), a `ValueError` is raised with a message listing the offending values. This is an intentional fail-fast behaviour — there is no graceful fallback.
- **Current state:** All production data uses the `"N months"` format, so this has not been triggered in practice. The constraint is validated explicitly before the cast to provide a clear error message if non-standard values are ever introduced.

### 7. Expression Units
- **Fixed assumption:** All expression values are "Log2 Counts per Million"
- **Implication:** No unit conversion is performed; assumes preprocessing has normalized data

### 8. Single Model per File
- **Assumption:** Each input data file contains rows for exactly one model, and therefore belongs to exactly one `effective_model_group`
- **Rationale:** The file-grouping step (Step 5) assigns each file to a group based on the first `model` value it finds. If a file contains rows from two models that map to *different* `effective_model_group`s, the entire file is assigned to only the first group. Inside `_process_individual_data_file_core` the per-row merge still labels every row with its correct group (via the label-map merge on `(model, genotype)`), but `result_order` and `matched_control` are computed once per function call from the combined DataFrame — so the secondary group's rows receive the wrong ordering list and the wrong control label.
- **Validation:** The code checks `df["model"].unique()` on every file and raises a `ValueError` if more than one `effective_model_group` is represented. This is a deliberate fail-fast behaviour — silent data corruption (wrong `result_order` and `matched_control`) is worse than an explicit error.
- **Current state:** All production input files contain data for a single model, so this check has never been triggered in practice.

## Filtering Decisions

### 1. Mouse Gene Filtering
- **What:** Keeps only genes with IDs starting with "ENSMUSG"
- **Why:** Model AD focuses on mouse models; human genes are not relevant
- **Impact:** Significantly reduces data volume if input contains human genes

### 2. Dropping Unmatched Rows
- **What:** Drops rows whose `(model, genotype)` pair had no match in the label map (NA `effective_model_group` after left merge); if all rows are dropped a `WARNING` is logged and the group returns `[]`
- **Why:** Prevents unrecognised genotype combinations from being processed
- **Example:** If processing model_group "5XFAD" with genotypes [A, B], rows with genotype C receive NA and are dropped
- **Impact:** Ensures data integrity and prevents mismatched comparisons

### 3. Empty File Filtering
- **What:** Raises error if data file is empty
- **Why:** Empty files indicate data pipeline issues
- **Impact:** Fails fast to alert of upstream problems

### 4. Unmapped Genotype Handling
- **What:** Retains genotypes not in rnaseq_genotype_label_map with fallback values
- **Why:** Prevents data loss due to incomplete metadata
- **Impact:** Allows processing to continue but marks data as non-control

### 5. Missing Metadata Handling
- **Gene symbols:** Returns empty string if not found (doesn't fail)
- **Genotype labels:** Uses original genotype value if not found (doesn't fail)
- **Why:** Prioritizes data availability over completeness
- **Trade-off:** May produce entries with missing display information

## Data Merging Decisions

### 1. Genotype Metadata Merge Strategy
- **Method:** Left join on (model, genotype)
- **Validation:** many-to-one relationship enforced
- **Why:** Ensures each (model, genotype) combination has exactly one display label
- **Failure mode:** Raises error if same (model, genotype) maps to multiple labels

### 2. Grouping Strategy
- **Primary group:** (ensembl_gene_id, tissue, name, age, model_group)
- **Secondary group:** age (within each primary group)
- **Why:** Organizes data hierarchically for efficient display and consolidates multi-file model_groups
- **Impact:** Creates one output entry per (gene, tissue, effective_model_group, age) regardless of how many input files contributed data

### 3. Cross-File Merging
- **Method:** Files are grouped by `effective_model_group`; within each group, preprocessed DataFrames are concatenated with `pd.concat` before core processing; memory is explicitly freed after each group
- **Why:** Minimizes peak memory usage — only files belonging to the same group are held in memory simultaneously
- **Trade-off:** No cross-group validation or deduplication

### 4. Model to Model Group Mapping
- **Method:** Extracts from genotype metadata (one entry per model)
- **Assumption:** All genotypes for a model have the same model_group
- **Validation:** Pre-validated by `validate_model_group_consistency`
- **Impact:** Ensures consistent model_group assignment

## Output Structure

Each output entry represents a unique combination of (gene, tissue, effective_model_group, age) with the following schema:

```json
{
  "ensembl_gene_id": "ENSMUSG00000000001",
  "gene_symbol": "Gnai3",
  "tissue": "Hemibrain",
  "name": "APOE4",
  "model_group": "APOE4",
  "matched_control": "C57BL/6J",
  "units": "Log2 Counts per Million",
  "age": "6 months",
  "age_numeric": 6,
  "result_order": ["C57BL/6J", "APOE4"],
  "data": [
    {
      "genotype": "C57BL/6J",
      "sex": "M",
      "individual_id": "sample001",
      "value": 8.12345
    },
    {
      "genotype": "APOE4",
      "sex": "F",
      "individual_id": "sample002",
      "value": 7.98765
    }
  ]
}
```

### Field Descriptions

- **ensembl_gene_id**: Mouse gene Ensembl identifier (ENSMUSG*)
- **gene_symbol**: Human-readable gene symbol (empty if not found in metadata)
- **tissue**: Tissue name (with JAX transformation and sentence case applied)
- **name**: `effective_model_group` value — equals `model_group` when explicitly set, or the model name for solo models
- **model_group**: Model group for display purposes (null if empty)
- **matched_control**: Display label of the control genotype (empty if no control present in data)
- **units**: Always "Log2 Counts per Million"
- **age**: Age timepoint as string (e.g., "3 months")
- **age_numeric**: Numeric age value for sorting
- **result_order**: Ordered list of genotype display labels for this model_group
- **data**: Array of individual data points with:
  - **genotype**: Display label for genotype
  - **sex**: Sex identifier
  - **individual_id**: Unique sample identifier
  - **value**: Normalized expression value (rounded to 5 decimal places)

## Performance Optimizations

1. **Vectorized Operations:** Uses pandas merge instead of row-by-row lookups
2. **Pre-computed Dictionaries:** Creates lookup dictionaries once before file processing
3. **Group-Based File Processing:** Groups input files by `effective_model_group` and processes one group at a time; single-file groups incur no concatenation overhead
4. **Memory Cleanup:** Explicitly deletes DataFrames and runs `gc.collect()` after each group is processed, keeping peak memory proportional to the largest group rather than the total dataset
5. **Efficient Grouping:** Uses pandas groupby instead of manual iteration

## Validation and Error Handling

### Pre-processing Validation
1. Required datasets and columns are checked
2. Model group consistency is validated (each model must have consistent model_group)
3. Empty files trigger errors

### Processing Validation
1. Merge validation ensures one-to-one genotype label mapping
2. Age sorting handles unexpected formats gracefully (falls back to original order)
3. If all rows in a group are dropped after the genotype label map merge (no genotypes matched), a `WARNING` is logged and that group contributes no output entries
4. **Mixed-group file check:** Before processing, each file is checked to confirm it contains only one `effective_model_group`. If multiple groups are detected, a `ValueError` is raised identifying the file and the conflicting groups (see Key Assumption 8)

### Error Scenarios
- **Missing required datasets:** ValueError with dataset name
- **Missing required columns:** ValueError with column names
- **Empty data files:** ValueError with file name
- **Non-standard age strings (no digits):** ValueError listing the offending values; see Key Assumption 6
- **All genotypes unrecognised (post-merge empty):** WARNING logged, group skipped (not an error)
- **Invalid merge relationships:** pandas MergeError with details
- **File contains multiple effective_model_groups:** ValueError raised with the file name and the set of conflicting groups; see Key Assumption 8

## Related Transforms

- **rna_de_aggregate:** Processes aggregated differential expression (log2FC, adj p-value) data
- **Utility functions:** `rna_de_individual_utils.py` contains utility functions extracted from this transform for better code organization and potential future reuse

## Example Usage

```python
from agoradatatools.etl.transform.rna_de_individual import (
    transform_rna_de_individual,
    REQUIRED_INPUT,
    DATA_FILE_REQUIRED_COLUMNS,
)
import pandas as pd

datasets = {
    "rnaseq_genotype_label_map": pd.read_csv("genotype_labels.csv"),
    "mouse_gene_metadata": pd.read_csv("gene_metadata.csv"),
    "APOE4_expression": pd.read_csv("APOE4_normalized_expression.csv"),
    "5XFAD_expression": pd.read_csv("5XFAD_normalized_expression.csv"),
}

# Default usage — uses REQUIRED_INPUT and DATA_FILE_REQUIRED_COLUMNS module constants
output = transform_rna_de_individual(datasets)

# Custom schema validation — override either or both constants if needed
output = transform_rna_de_individual(
    datasets,
    required_input=REQUIRED_INPUT,
    data_file_required_columns=DATA_FILE_REQUIRED_COLUMNS,
)
# output is a list of dictionaries with the structure described above
```

## Troubleshooting

### Issue: Missing gene symbols
- **Cause:** Ensembl IDs not in mouse_gene_metadata
- **Impact:** gene_symbol field will be empty string
- **Solution:** Update gene metadata file or accept empty values

### Issue: Unmapped genotypes
- **Cause:** (model, genotype) combinations in data not in rnaseq_genotype_label_map
- **Impact:** Uses original genotype as display label, result_order=999
- **Solution:** Add missing entries to genotype label map

### Issue: Empty matched_control
- **Cause:** No control genotypes present in actual data for a group
- **Impact:** matched_control field will be empty string
- **Solution:** Verify control samples exist in input data

### Issue: Unexpected tissue names
- **Cause:** Tissue names not standardized in input data
- **Impact:** Only "Right Cerebral Hemisphere" is transformed to "Hemibrain"; all other tissues are converted to sentence case
- **Solution:** Update input data or add transformations to `map_jax_tissue_name`

### Issue: Memory errors with large files
- **Cause:** Processing very large expression files
- **Impact:** Out of memory errors
- **Solution:** Files are processed group by group with explicit memory cleanup after each group; consider splitting input files further if individual groups remain too large

### Issue: ValueError — file contains rows from multiple effective_model_groups
- **Cause:** A single input CSV file contains rows for two or more models that map to different `effective_model_group` values in the label map
- **Impact:** The transform raises a `ValueError` and stops immediately. This is intentional — allowing processing to continue would produce silently incorrect `result_order` and `matched_control` values for the secondary group's rows.
- **Solution:** Split the mixed file so that each output file contains data for only one model. This is the expected and supported input format.
