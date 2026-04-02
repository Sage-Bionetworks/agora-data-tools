# RNA DE Individual Transform

## Overview

The `rna_de_individual` transform processes individual-level RNA expression (normalized expression) data from Model AD mouse models. It transforms raw individual expression measurements into a structured format that groups data by gene, tissue, and model_group, with individual data points organized by age.

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
- `model_group`: Grouping of models for display purposes (e.g., "5XFAD", "APOE4"; may be empty string or NaN — both are normalized to `None`)
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
- `sex`: Sex identifier (e.g., "Male", "Female")
- `tissue`: Tissue name (e.g., "Right Cerebral Hemisphere", "hippocampus")
- `individualid`: Unique identifier for each individual sample

**Purpose:** Contains individual expression measurements for each sample.

## Data Processing Pipeline

### Step 1: Metadata Preparation

1. **Genotype Label Map Preparation** (`prepare_genotype_label_map_df`)
   - Imported from `rna_de_individual_utils` module
   - Normalizes the `rnaseq_genotype_label_map` DataFrame: treats both `""` and `NaN` in `model_group` as "no group" and converts them to `None`, and casts `result_order` to `int`
   - Returns a DataFrame that is passed directly to `_process_individual_data_file_core` for vectorized merging
   - **Purpose:** Produces the normalized label map DataFrame used for genotype enrichment

2. **Gene Metadata Dictionary Creation** (`create_gene_metadata_dict`)
   - Imported from `rna_de_individual_utils` module
   - Maps Ensembl gene IDs to gene symbols
   - **Purpose:** Enriches output with human-readable gene names

### Step 2: File Grouping and Preprocessing

Before transform-specific processing, the main function groups input files by their `model_group` and preprocesses each file individually.

#### 2.1 File Grouping by model_group
- Reads the `model` column of each input file to determine its `model_group` using the genotype label map lookup
- Groups files with the same `model_group` together (e.g. UCI models whose data is split across two CSV files)
- Single-file groups are processed without any concatenation overhead
- This strategy keeps memory usage proportional to the largest group rather than the total dataset size
- **Single-model-per-file validation:** Before grouping, checks whether any file contains rows from more than one model. If so, a `ValueError` is raised immediately, identifying the file and the conflicting model names. This is a hard failure because `result_order` and `matched_control` cannot be computed correctly when a file spans multiple models (see [Key Assumptions: Single Model per File](#single-model-per-file))

#### 2.2 Common Preprocessing (`preprocess_data_file`)
Applied to each file individually before it is combined within its group:
- **Logging:** Logs file name, global index, row count, column count, and memory usage
- **Empty file validation:** Raises error if file is empty
- **Column validation:** Checks all required columns are present (defined by `DATA_FILE_REQUIRED_COLUMNS`)
- **Gene filtering:** Filters to mouse genes only (keeps `ENSMUSG*`, removes `ENSG*`)
- **Tissue name mapping:** Replaces `"Right Cerebral Hemisphere"` with `"Hemibrain"` and converts all tissue names to sentence case (e.g., `"hippocampus"` → `"Hippocampus"`). To add a new multi-word mapping, add another `.str.replace()` call to the chain in `preprocess_data_file`.
- **Numeric rounding:** Rounds all numeric columns to 5 decimal places
- **Type casting:** Casts `individualid` to string to ensure consistent identifier handling

After all files in a group are preprocessed, they are concatenated (via `pd.concat`) into a single DataFrame that is passed to `_process_individual_data_file_core`. Memory is explicitly freed (via `del` and `gc.collect()`) after each group is processed.

#### 2.3 Transform-Specific Processing (`_process_individual_data_file_core`)

After preprocessing and concatenation, the individual transform applies its specific logic:

**Genotype Enrichment (Vectorized Merge):**
- Selects relevant columns from the normalized genotype label map DataFrame (produced by `prepare_genotype_label_map_df` in Step 1)
- Performs left join on `(model, genotype)` to add:
  - `display_label`: Human-readable genotype label
  - `result_order`: Ordering value for display
  - `model_group`: Explicit model group
- **Merge validation:** Uses `validate="many_to_one"` to ensure each `(model, genotype)` maps to exactly one label

**name Field Assignment:**
- `name` is set directly to `model_group`
- This consolidates multi-file model_groups (e.g. all UCI models sharing "Trem2-R47H_NSS") under a single display name

**Dropping Unmatched Rows:**
- After the left merge with the label map, any row whose `(model, genotype)` pair had no match receives NA for `result_order` (since it comes from the label map and is never filled with a fallback)
- Those rows are removed with `dropna(subset=["result_order"])`
- **Purpose:** Ensures only genotype combinations that exist in the label map are processed
- **Example:** If model_group "5XFAD" has genotypes ["5XFAD_carrier", "5XFAD_noncarrier"], any rows with a different genotype receive NA for `result_order` and are dropped
- **All-rows-filtered case:** If every row is dropped (i.e., no genotype in the file matched the label map at all), a `ValueError` is raised. This strongly indicates either the wrong file was provided or the label map is missing entries for the model — silently producing empty output could mask a data pipeline misconfiguration. Check that the input file's `model`/`genotype` values match those in `rnaseq_genotype_label_map`.

### Step 3: Grouping and Output Entry Creation

#### 3.1 Grouping Strategy
- Groups data by four columns: `(ensembl_gene_id, tissue, name, age)`
  - `ensembl_gene_id`: Ensembl gene identifier
  - `tissue`: Tissue name (post-mapping and sentence-case normalization)
  - `name`: Set to `model_group`
  - `age`: Age timepoint (e.g., `"4 months"`, `"12 months"`)
- `model_group` is excluded from the groupby key to avoid issues with `None` values (pandas drops `NaN`/`None` groupby keys by default). Since `name` equals `model_group`, `model_group` is restored as a top-level output column from `name` after nesting.
- Each unique combination of these four columns defines one output row
- **Design decision:** `name` is set to `model_group` so that all models sharing the same `model_group` (including data split across multiple input files) produce a single consolidated output entry

#### 3.2 Transform-Specific Processing (`_process_individual_data_file_core`)

For each grouped combination, this function directly creates output entries (one per age timepoint) with:

**Gene Information:**
- `ensembl_gene_id`: Original Ensembl ID
- `gene_symbol`: Gene symbol from metadata (empty string if not found)

**Tissue Information:**
- `tissue`: Tissue name, already mapped and sentence-cased by `preprocess_data_file` (Step 2.2)

**Model Information:**
- `name`: `model_group` value
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
  - Scans genotypes present in the already-merged data file (not the raw metadata), so only labels that exist in the actual data are included
  - Excludes rows with an empty `display_label`
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
  - `individual_id`: Sample identifier (cast to string during preprocessing)
  - `value`: Expression value

**Processing Steps:**
1. Groups the input data by four columns: (ensembl_gene_id, tissue, name, age); model_group is restored from name after grouping
2. For each grouped combination, creates a complete output entry with all metadata fields
3. Sorts output entries by gene then numeric age for consistent ordering
4. Returns one output entry per unique (ensembl_gene_id, tissue, name, age) combination

**Unnesting Decision:** Unlike some transforms that nest age data, this transform creates **one output entry per age** (unnested structure). Each entry has a single age with its associated data points.

### Step 4: Consolidation
- Combines output entries from all processed model_groups
- Returns single list of all entries

## Key Assumptions

### 1. Gene Filtering
- **Assumption:** Input data may contain both mouse and human genes
- **Decision:** Only mouse genes (ENSMUSG*) are relevant; human genes (ENSG*) are filtered out

### 2. Result Order and Control Identification
- **Assumption:** Lower `result_order` values always represent control genotypes
- **Assumption:** The genotype with the minimum result_order in actual data is the matched control
- **Authoritative source:** `result_order` from `rnaseq_genotype_label_map` is the sole signal used to determine which genotype is the control. Any external model_info file is not consulted. Currently, `result_order` assignments and model_info file designations agree for all `model_group`s with only two genotypes, but `result_order` takes precedence if they ever diverge.
- **Limitation for 4-genotype UCI studies:** Some DE analyses pair each case genotype with a *different* control (e.g., `Trem2-R47H_NSS.5xFAD` vs `Trem2-R47H_NSS` rather than vs `C57BL/6J`). The `matched_control` field cannot represent this per-case-genotype pairing — it always contains the single genotype with the lowest `result_order` for the group, which is a simplification for these multi-control scenarios.
- **Implication:** If data is missing control samples, the matched_control field may be empty

### 3. Model Grouping Strategy
This transform is designed to handle two distinct experimental scenarios:

**Scenario A: Single Model vs. Control**
- One model compared to one control (e.g., `APOE4` vs. `C57BL/6J`)
- The `model_group` equals the model name (e.g., `"APOE4"`)
- Display shows one experimental genotype vs. one control genotype

**Scenario B: Multiple Related Models Sharing Controls**
- Multiple variants of the same model type share a common control group
- Example: `Trem2-R47H_NSS` and `Trem2-R47H_NSS.5xFAD` both belong to `model_group = "Trem2-R47H_NSS"` and their expression data lives in separate input files
- The `model_group` field links these related models together
- Display can show multiple model variants alongside their shared controls

**Implementation Details:**
- **Name Field:** The `name` field is set directly to `model_group`, consolidating multi-file model_groups (e.g. UCI models) under a single display name
- **File Grouping:** Input files are first grouped by `model_group`; only files within the same group are concatenated before processing
- **Grouping Key:** Data is grouped by `(ensembl_gene_id, tissue, name, age)` to produce one consolidated output entry per group, regardless of how many input files contributed data; `model_group` (which equals `name`) is restored as a top-level output column after nesting

### 4. Genotype Mapping Completeness
- **Assumption:** All genotypes in data files have entries in `rnaseq_genotype_label_map`
- **Behavior:** Rows whose `(model, genotype)` pair has no match in the label map receive NA for `result_order` after the left merge and are dropped by `dropna(subset=["result_order"])`. If every row in a group is dropped this way, a `ValueError` is raised.
- **Display label safeguard:** Before the dropna, `display_label` is filled with the raw `genotype` value for any unmatched row, but since those rows are subsequently dropped this has no practical effect on output.

### 5. Tissue Name Standardization
- **Assumption:** JAX models use "Right Cerebral Hemisphere" which should be standardized
- **Transformation:** "Right Cerebral Hemisphere" → "Hemibrain"
- **Sentence case conversion:** All tissue names are converted to sentence case for consistency
- **Purpose:** Ensures consistency across different data sources and standardizes capitalization

### 6. Age Format
- **Assumption:** Age values follow the format `"[N] months"` (e.g., `"3 months"`, `"6 months"`), where `N` is a non-negative integer. Every age string in the data **must** match this exact pattern.
- **Constraint (hard failure):** The `age_numeric` field is derived by matching the `age` string against the regex `(\d+) months` and casting the captured group to `int`. If any age value does not match this pattern (e.g., `"neonatal"`, `"1 year"`, or a blank string), a `ValueError` is raised with a message listing the offending values. This is an intentional fail-fast behaviour — there is no graceful fallback.
- **Current state:** All production data uses the `"N months"` format, so this has not been triggered in practice. The constraint is validated explicitly before the cast to provide a clear error message if non-standard values are ever introduced.

### 7. Expression Units
- **Fixed assumption:** All expression values are "Log2 Counts per Million"
- **Implication:** No unit conversion is performed; assumes preprocessing has normalized data

<a name="single-model-per-file"></a>

### 8. Single Model per File
- **Assumption:** Each input data file contains rows for exactly one model, and therefore belongs to exactly one `model_group`
- **Rationale:** The file-grouping step (Step 2.1) assigns each file to a group based on the first `model` value it finds. If a file contains rows from two models that map to *different* `model_group`s, the entire file is assigned to only the first group. Inside `_process_individual_data_file_core` the per-row merge still labels every row with its correct group (via the label-map merge on `(model, genotype)`), but `result_order` and `matched_control` are computed once per function call from the combined DataFrame — so the secondary group's rows receive the wrong ordering list and the wrong control label.
- **Validation:** The code checks `df["model"].unique()` on every file and raises a `ValueError` immediately if more than one model is present, regardless of whether those models share a `model_group`. This is a deliberate fail-fast behaviour — silent data corruption (wrong `result_order` and `matched_control`) is worse than an explicit error.
- **Current state:** All production input files contain data for a single model, so this check has never been triggered in practice.

## Filtering Decisions

### 1. Mouse Gene Filtering
- **What:** Keeps only genes with IDs starting with "ENSMUSG"
- **Why:** Model AD focuses on mouse models; human genes are not relevant
- **Impact:** Significantly reduces data volume if input contains human genes

### 2. Dropping Unmatched Rows
- **What:** Drops rows whose `(model, genotype)` pair had no match in the label map (NA `result_order` after left merge); if all rows are dropped a `ValueError` is raised
- **Why:** Prevents unrecognised genotype combinations from being processed
- **Example:** If processing model_group "5XFAD" with genotypes [A, B], rows with genotype C receive NA for `result_order` and are dropped
- **Impact:** Ensures data integrity and prevents mismatched comparisons

### 3. Empty File Filtering
- **What:** Raises error if data file is empty
- **Why:** Empty files indicate data pipeline issues
- **Impact:** Fails fast to alert of upstream problems

### 4. Unmapped Genotype Handling
- **What:** Drops rows whose `(model, genotype)` pair is not in `rnaseq_genotype_label_map` (they receive NA for `result_order` after the left merge and are removed)
- **Why:** Prevents unrecognised genotypes from producing output with missing ordering and control information
- **Impact:** If all rows in a group are dropped, a `ValueError` is raised

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
- **Grouping key:** (ensembl_gene_id, tissue, name, age)
- **Why:** Organizes data for efficient display and consolidates multi-file model_groups; `age` is part of the key so each age timepoint produces its own output entry; `model_group` is excluded from the groupby key (pandas drops `None` groupby keys by default) and is instead restored from `name` as a top-level output column after nesting
- **Impact:** Creates one output entry per (ensembl_gene_id, tissue, name, age) regardless of how many input files contributed data

### 3. Cross-File Merging
- **Method:** Files are grouped by `model_group`; within each group, preprocessed DataFrames are concatenated with `pd.concat` before core processing; memory is explicitly freed after each group
- **Why:** Minimizes peak memory usage — only files belonging to the same group are held in memory simultaneously
- **Trade-off:** No cross-group validation or deduplication

### 4. Model to Model Group Mapping
- **Method:** Extracts from genotype metadata (one entry per model)
- **Assumption:** All genotypes for a model have the same model_group
- **Validation:** Pre-validated by `validate_model_group_consistency`
- **Impact:** Ensures consistent model_group assignment

## Output Structure

Each output entry represents a unique combination of (gene, tissue, model_group, age) with the following schema:

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
      "sex": "Male",
      "individual_id": "sample001",
      "value": 8.12345
    },
    {
      "genotype": "APOE4",
      "sex": "Female",
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
- **name**: `model_group` value
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
3. **Group-Based File Processing:** Groups input files by `model_group` and processes one group at a time; single-file groups incur no concatenation overhead
4. **Memory Cleanup:** Explicitly deletes DataFrames and runs `gc.collect()` after each group is processed, keeping peak memory proportional to the largest group rather than the total dataset
5. **Efficient Grouping:** Uses pandas groupby instead of manual iteration

## Validation and Error Handling

### Pre-processing Validation
1. Required datasets and columns are checked
2. Model group consistency is validated (each model must have consistent model_group)
3. Empty files trigger errors

### Processing Validation
1. Merge validation ensures one-to-one genotype label mapping
2. Age strings are validated against the `(\d+) months` regex; non-matching values raise `ValueError` immediately
3. If all rows in a group are dropped after the genotype label map merge (no genotypes matched), a `ValueError` is raised with a message identifying the cause
4. **Single-model-per-file check:** Before grouping, each file is checked to confirm it contains rows for exactly one model. If more than one model is detected, a `ValueError` is raised immediately, identifying the file and the conflicting model names (see [Key Assumption 8](#single-model-per-file))

### Error Scenarios
- **Missing required datasets:** ValueError with dataset name
- **Missing required columns:** ValueError with column names
- **Empty data files:** ValueError with file name
- **Non-standard age strings (not matching `'[N] months'`):** ValueError listing the offending values; see Key Assumption 6
- **All genotypes unrecognised (post-merge empty):** ValueError raised; the file had data but no recognised genotypes, which indicates a wrong file or a misconfigured label map
- **Invalid merge relationships:** pandas MergeError with details
- **File contains multiple models:** ValueError raised with the file name and the list of conflicting model names; see [Key Assumption 8](#single-model-per-file)

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
- **Impact:** Rows are dropped; if all rows in a group are unmapped, a `ValueError` is raised
- **Solution:** Add missing entries to genotype label map

### Issue: Empty matched_control
- **Cause:** No control genotypes present in actual data for a group
- **Impact:** matched_control field will be empty string
- **Solution:** Verify control samples exist in input data

### Issue: Unexpected tissue names
- **Cause:** Tissue names not standardized in input data
- **Impact:** Only "Right Cerebral Hemisphere" is transformed to "Hemibrain"; all other tissues are converted to sentence case
- **Solution:** Update input data or add additional `.str.replace()` calls to the tissue mapping chain in `preprocess_data_file` in `rna_de_individual_utils.py`

### Issue: Memory errors with large files
- **Cause:** Processing very large expression files
- **Impact:** Out of memory errors
- **Solution:** Files are processed group by group with explicit memory cleanup after each group; consider splitting input files further if individual groups remain too large

### Issue: ValueError — file contains rows from multiple models
- **Cause:** A single input CSV file contains rows for two or more models.
- **Impact:** The transform raises a `ValueError` immediately and stops. This is intentional — allowing processing to continue would produce silently incorrect `result_order` and `matched_control` values.
- **Solution:** Split the mixed file so that each output file contains data for exactly one model. This is the expected and supported input format.
