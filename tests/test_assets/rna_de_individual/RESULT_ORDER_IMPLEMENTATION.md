# Result Order Implementation for RNA DE Individual

## Overview
This document describes the implementation of the `result_order` field in the `rna_de_individual` transform output.

## What is `result_order`?
The `result_order` field is an array of display labels that specifies the order in which genotypes should appear in the UI visualization for each model_group. This ensures consistent and logical ordering of data points across the application.

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

## Notes
- The ordering logic is deterministic and consistent across all genes and age groups
- For models with 3 genotypes or other unexpected counts, the function falls back to alphabetical sorting
- The `result_order` field is generated dynamically based on the actual genotypes present in each model_group
