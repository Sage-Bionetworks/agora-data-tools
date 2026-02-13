# RNA DE Individual Transform

## Overview

The `rna_de_individual` transform processes individual-level RNA expression (normalized expression) data from Model AD mouse models. It transforms raw individual expression measurements into a structured format that groups data by gene, tissue, model_group, and name, with individual data points organized by age.

**Module Location:** `src/agoradatatools/etl/transform/rna_de_individual.py`

## Purpose

This transform serves to:
1. Consolidate individual RNA expression data from multiple mouse models
2. Group individual measurements by model group to support both single and multiple control display paradigms
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
- `alias`: Gene aliases (used for lookup fallback)

**Purpose:** Provides gene symbols for Ensembl gene IDs.

### 3. Data Files (one or more CSV files)
**Required columns:**
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

1. **Genotype Metadata Dictionary Creation** (`create_genotype_metadata_dict`)
   - Imported from `rna_de_individual_utils` module
   - Creates a lookup dictionary mapping `(model, genotype)` tuples to metadata
   - Each entry contains:
     - `display_label`: Human-readable genotype label
     - `result_order`: Integer for sorting/control identification
     - `model_group`: Model group name (empty string if none)
     - `effective_model_group`: `model_group` if present, otherwise `model` name
   - **Purpose:** Enables O(1) lookup time for genotype properties during processing
   - **Note:** The individual transform uses `include_result_order=True` to include ordering and effective model group information

2. **Gene Metadata Dictionary Creation** (`create_gene_metadata_dict`)
   - Imported from `rna_de_individual_utils` module
   - Maps Ensembl gene IDs to gene symbols
   - Uses gene symbols first, falls back to aliases if needed
   - **Purpose:** Enriches output with human-readable gene names

### Step 2: File Processing (Shared Pattern)

The transform uses a **shared file processing pattern** (`process_data_files` from `rna_de_individual_utils`) that handles common preprocessing steps for all data files:

#### 2.1 Common Preprocessing (Shared)
Automatically applied to each file before transform-specific processing:
- **File iteration:** Processes files one at a time (excluding required input files)
- **Logging:** Logs file name, index, row count, column count, and memory usage
- **Empty file validation:** Raises error if file is empty
- **Column validation:** Checks all required columns are present
- **Gene filtering:** Filters to mouse genes only (keeps `ENSMUSG*`, removes `ENSG*`)
- **Numeric rounding:** Rounds all numeric columns to 5 decimal places
- **Memory cleanup:** Deletes DataFrame and runs garbage collection after processing

#### 2.2 Transform-Specific Processing (`_process_individual_data_file_core`)

After preprocessing, the individual transform applies its specific logic:

**Genotype Enrichment (Vectorized Merge):**
- Converts genotype metadata dictionary to DataFrame
- Performs left join on `(model, genotype)` to add:
  - `genotype_display`: Human-readable genotype label
  - `result_order`: Ordering value for display
- **Fallback for unmapped genotypes:**
  - `genotype_display` = original `genotype` value
  - `result_order` = 999 (treated as non-control)
- **Merge validation:** Uses `validate="many_to_one"` to ensure each `(model, genotype)` maps to exactly one label

**Model Group Assignment:**
- Maps each model to its model_group using genotype metadata
- Empty string if no model_group defined
- Adds `name` field equal to `model` (not model_group) since each file represents a single model

**Genotype Filtering by Model Group:**
- **Critical filtering step:** Filters data to include only genotypes that belong to the effective model_group
- Effective model_group = `model_group` if present, else `model` name
- Builds set of allowed `(effective_model_group, genotype)` combinations from metadata
- Removes rows with invalid genotype combinations
- **Purpose:** Ensures only valid genotype combinations are processed for each model group
- **Example:** If model_group "5XFAD" has genotypes ["5XFAD_carrier", "5XFAD_noncarrier"], any rows with different genotypes are filtered out

### Step 3: Grouping and Output Entry Creation

#### 3.1 Grouping Strategy
- Groups data by: `(ensembl_gene_id, tissue, model_group, name)`
- Each group represents a unique combination of gene, tissue, and model
- **Design decision:** Groups by `model_group` to support multiple controls paradigm

#### 3.2 Individual Results Structure Creation (`_create_individual_results_from_group`)
- Sub-groups data by age within each main group
- For each age timepoint, creates a list of individual data points:
  - `genotype`: Display label for genotype
  - `sex`: Sex identifier
  - `individual_id`: Individual sample identifier (converted to string)
  - `value`: Expression value (converted to float)
- Sorts age entries numerically (extracts numeric value from strings like "3 months")

#### 3.3 Output Entry Creation (`_create_output_entry_from_group`)

For each grouped combination, creates output entries with:

**Gene Information:**
- `ensembl_gene_id`: Original Ensembl ID
- `gene_symbol`: Gene symbol from metadata (empty string if not found)

**Tissue Information:**
- `tissue`: Tissue name with JAX-specific transformation applied
  - **Special transformation:** "Right Cerebral Hemisphere" → "Hemibrain"
  - All other tissue names remain unchanged

**Model Information:**
- `name`: Model name (from the `model` field, NOT model_group)
- `model_group`: Model group name (normalized to `None` if empty string)

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
  - `genotype`: Display label
  - `sex`: Sex identifier
  - `individual_id`: Sample identifier
  - `value`: Expression value

**Unnesting Decision:** Unlike some transforms that nest age data, this transform creates **one output entry per age** (unnested structure). Each entry has a single age with its associated data points.

### Step 4: Consolidation
- Combines output entries from all processed files
- Returns single list of all entries

## Key Assumptions

### 1. Gene Filtering
- **Assumption:** Input data may contain both mouse and human genes
- **Decision:** Only mouse genes (ENSMUSG*) are relevant; human genes (ENSG*) are filtered out

### 2. Result Order and Control Identification
- **Assumption:** Lower `result_order` values always represent control genotypes
- **Assumption:** The genotype with the minimum result_order in actual data is the matched control
- **Implication:** If data is missing control samples, the matched_control field may be empty

### 3. Effective Model Group
- **Assumption:** When `model_group` is empty, the model itself serves as its own group
- **Purpose:** Handles both grouped models (e.g., multiple 5XFAD variants) and standalone models

### 4. Name vs Model Group
- **Decision:** The `name` field uses the actual model name, NOT the model_group
- **Rationale:** Each data file represents a single model's data; preserves model-level granularity

### 5. Genotype Mapping Completeness
- **Assumption:** Most genotypes in data files have entries in rnaseq_genotype_label_map
- **Fallback:** Unmapped genotypes use the original genotype value as display_label
- **Treatment:** Unmapped genotypes get result_order=999 (treated as non-controls)

### 6. Tissue Name Standardization
- **Assumption:** JAX models use "Right Cerebral Hemisphere" which should be standardized
- **Transformation:** "Right Cerebral Hemisphere" → "Hemibrain"
- **Purpose:** Ensures consistency across different data sources

### 7. Age Format
- **Assumption:** Age values follow format "[number] months" (e.g., "3 months", "6 months")
- **Handling:** Numeric extraction for sorting; falls back to original order if format is unexpected

### 8. Expression Units
- **Fixed assumption:** All expression values are "Log2 Counts per Million"
- **Implication:** No unit conversion is performed; assumes preprocessing has normalized data

## Filtering Decisions

### 1. Mouse Gene Filtering
- **What:** Keeps only genes with IDs starting with "ENSMUSG"
- **Why:** Model AD focuses on mouse models; human genes are not relevant
- **Impact:** Significantly reduces data volume if input contains human genes

### 2. Genotype Filtering by Model Group
- **What:** Keeps only genotypes that belong to the effective model_group per metadata
- **Why:** Prevents invalid genotype combinations from being processed
- **Example:** If processing model_group "5XFAD" with genotypes [A, B], filters out any rows with genotype C
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
- **Primary group:** (ensembl_gene_id, tissue, model_group, name)
- **Secondary group:** age (within each primary group)
- **Why:** Organizes data hierarchically for efficient display
- **Impact:** Creates nested structure suitable for visualization

### 3. Cross-File Merging
- **Method:** Sequential processing with list concatenation
- **Why:** Minimizes memory usage for large datasets
- **Trade-off:** No cross-file validation or deduplication

### 4. Model to Model Group Mapping
- **Method:** Extracts from genotype metadata (one entry per model)
- **Assumption:** All genotypes for a model have the same model_group
- **Validation:** Pre-validated by `validate_model_group_consistency`
- **Impact:** Ensures consistent model_group assignment

## Output Structure

Each output entry represents a unique combination of (gene, tissue, model_group, name, age) with the following schema:

```json
{
  "ensembl_gene_id": "ENSMUSG00000000001",
  "gene_symbol": "Gnai3",
  "tissue": "Hemibrain",
  "name": "Jax.IU.Pitt_APOE4",
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
- **tissue**: Tissue name (with JAX transformation applied)
- **name**: Actual model name (not model_group)
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
3. **Sequential File Processing:** Processes files one at a time with explicit garbage collection
4. **Memory Cleanup:** Deletes DataFrames and runs gc.collect() after each file
5. **Efficient Grouping:** Uses pandas groupby instead of manual iteration

## Validation and Error Handling

### Pre-processing Validation
1. Required datasets and columns are checked
2. Model group consistency is validated (each model must have consistent model_group)
3. Empty files trigger errors

### Processing Validation
1. Merge validation ensures one-to-one genotype label mapping
2. Age sorting handles unexpected formats gracefully (falls back to original order)

### Error Scenarios
- **Missing required datasets:** ValueError with dataset name
- **Missing required columns:** ValueError with column names
- **Empty data files:** ValueError with file name
- **Invalid merge relationships:** pandas MergeError with details

## Related Transforms

- **rna_de_aggregate:** Processes aggregated differential expression (log2FC, adj p-value) data
- **Utility functions:** `rna_de_individual_utils.py` contains utility functions extracted from this transform for better code organization and potential future reuse

## Example Usage

```python
from agoradatatools.etl.transform.rna_de_individual import transform_rna_de_individual
import pandas as pd

datasets = {
    "rnaseq_genotype_label_map": pd.read_csv("genotype_labels.csv"),
    "mouse_gene_metadata": pd.read_csv("gene_metadata.csv"),
    "APOE4_expression": pd.read_csv("APOE4_normalized_expression.csv"),
    "5XFAD_expression": pd.read_csv("5XFAD_normalized_expression.csv"),
}

output = transform_rna_de_individual(datasets)
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
- **Impact:** Only "Right Cerebral Hemisphere" is transformed to "Hemibrain"
- **Solution:** Update input data or add transformations to `map_jax_tissue_name`

### Issue: Memory errors with large files
- **Cause:** Processing very large expression files
- **Impact:** Out of memory errors
- **Solution:** Files are processed sequentially with cleanup; consider splitting input files
