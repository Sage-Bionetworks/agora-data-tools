# Result Order Implementation for RNA DE Individual

## Overview
This document describes the implementation of the `result_order` and `age_numeric` fields in the `rna_de_individual` transform output.

## What is `result_order`?
The `result_order` field is an array of display labels that specifies the order in which genotypes should appear in the UI visualization for each model_group. This ensures consistent and logical ordering of data points across the application.

## What is `age_numeric`?
The `age_numeric` field is an integer value extracted from the `age` string field. For example, if `age` is "12 months", then `age_numeric` is 12. This field facilitates numerical operations and sorting based on age without having to parse the string format. If age extraction fails, the value defaults to 0.

## Example Output Structure
```json
{
  "ensembl_gene_id": "ENSMUSG00000000001",
  "gene_symbol": "Gnai3",
  "tissue": "Hemibrain",
  "name": "LOAD1",
  "model_group": "LOAD1",
  "matched_control": "C57BL6J",
  "units": "Log2 Counts per Million",
  "age": "4 months",
  "age_numeric": 4,
  "result_order": [
    "C57BL6J",      // base control
    "Trem2",        // base model
    "5xFAD",        // fancy control
    "5xFAD.Trem2"   // compound model
  ],
  "data": [...]
}
```

## Ordering Rules

### Simple Models (2 genotypes)
For models with only 2 genotypes:
1. **Case** (carrier) - First
2. **Control** (noncarrier) - Second

Example: `["5xFAD (UCI)", "C57BL/6J"]`

### Matrixed Models (4+ genotypes)
For models with 4 or more genotypes (compound models):
1. **Base control** - Real control (e.g., C57BL6J with "noncarrier" in genotype name)
2. **Base model** - Single mutation (e.g., Trem2 - no "carrier" in genotype name)
3. **Fancy control** - Another model's carrier (e.g., 5xFAD - has "carrier" in genotype name)
4. **Compound model** - Model on fancy background (has semicolon ";" in genotype name)

Example: `["C57BL/6J", "Abca7*V1599M", "5xFAD", "Abca7*V1599M.5xFAD"]`

## Implementation Details

### Function: `_determine_result_order()`
Located in: `src/agoradatatools/etl/transform/rna_de_individual.py`

This function analyzes the genotypes in a model_group and determines the correct ordering based on:
- Number of genotypes
- Presence of "noncarrier" keyword (identifies controls)
- Presence of "carrier" keyword (identifies fancy controls)
- Presence of semicolon ";" (identifies compound models)

### Function: `_extract_age_numeric()`
Located in: `src/agoradatatools/etl/transform/rna_de_individual.py`

This function extracts the numeric value from age strings:
- Takes an age string like "6 months" or "12 months"
- Returns the integer value (e.g., 6 or 12)
- Returns 0 if extraction fails (invalid format, empty string, or None)

### Modified Functions
1. **`_create_output_entry_from_group()`**
   - Added `genotypes_by_model_group` parameter
   - Calls `_determine_result_order()` to generate the result_order array
   - Calls `_extract_age_numeric()` to generate the age_numeric field
   - Includes `result_order` and `age_numeric` in the output entry

2. **`_process_single_data_file()`**
   - Updated to pass `genotypes_by_model_group` to `_create_output_entry_from_group()`

### Test Coverage
All tests pass successfully:
- Unit tests for simple models (2 genotypes)
- Unit tests for matrixed models (4 genotypes)
- Unit tests for age_numeric extraction (various formats and edge cases)
- Integration tests with synthetic data
- Tests verify correct ordering for:
  - 5xFAD (UCI) - simple model
  - Abca7*V1599M - matrixed model
  - LOAD1 - matrixed model
- Tests verify age_numeric extraction and inclusion in output

## Files Modified
1. `src/agoradatatools/etl/transform/rna_de_individual.py` - Core implementation
2. `tests/transform/test_rna_de_individual.py` - Updated tests
3. `tests/test_assets/rna_de_individual/output/synthetic_basic_output.json` - Expected output
4. `tests/test_assets/rna_de_individual/output/synthetic_model_group_output.json` - Expected output

## Validation
To validate the changes, run the tests:
```bash
pytest tests/transform/test_rna_de_individual.py -xvs
```

## Notes
- The ordering logic is deterministic and consistent across all genes and age groups
- For models with 3 genotypes or other unexpected counts, the function falls back to alphabetical sorting
- The `result_order` field is generated dynamically based on the actual genotypes present in each model_group
- The `age_numeric` field provides a convenient integer representation of age for numerical operations and sorting
- If age parsing fails, `age_numeric` defaults to 0 to prevent errors

