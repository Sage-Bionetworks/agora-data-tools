# RNA Individual Expression Transform - Synthetic Test Datasets

This directory contains synthetic test datasets for the `transform_rna_de_individual` function, which processes individual RNA-seq expression data for mouse models.

## Overview

The synthetic datasets are designed to mirror the structure of real production data and test various aspects of the transformation pipeline including:
- Core transformation logic (data grouping, individual expression aggregation)
- JAX tissue name mapping
- Human gene filtering
- Age sorting
- Data precision (rounding to 5 decimal places)
- Edge cases (single row data, missing metadata, empty files)
- Error handling

**Important**: Each synthetic data file contains only ONE model, mirroring the structure of real production data files where each file represents a single model's expression data.

## Directory Structure

```
rna_de_individual/
├── input/           # Input CSV files for testing
└── output/          # Expected JSON output files
```

## Input Files

### Metadata Files

These files are required for all transformations. Both are minimal synthetic subsets containing only the entries needed by the test suite:

1. **synthetic_rnaseq_genotype_label_map.csv**
   - Maps genotypes to display labels and model groups
   - Columns: model, model_group, display_label, genotype, result_order
   - Synthetic subset of production `rnaseq_genotype_label_map` data; contains only the 4 models used by the test suite (APOE4, LOAD1, Trem2-R47H_NSS, Trem2-R47H_NSS.5xFAD)

2. **synthetic_mouse_gene_metadata.csv**
   - Gene symbols for Ensembl IDs
   - Columns: ensembl_gene_id, gene_symbol
   - Synthetic subset of production `mouse_gene_metadata` data; contains only the entries needed by the test suite (ENSMUSG00000000001 → Gnai3, plus a few additional rows for realism)

3. **rnaseq_genotype_label_map_inconsistent.csv**
   - Contains inconsistent model_group values to test error handling
   - Synthetic file for testing validation logic

### Data Files

Each data file contains individual expression measurements with columns:
`ensembl_gene_id`, `individualid`, `expression`, `tissue`, `sex`, `age`, `genotype`, `model`

1. **synthetic_basic_data.csv**
   - Simple test with 2 genes (ENSMUSG00000000001, ENSMUSG00000000002)
   - Single age (6 months), single tissue (Cortex)
   - APOE4 model with real genotypes (APOE4-KI_homozygous; Trem2-R47H_WT and APOE4-KI_WT; Trem2-R47H_WT)
   - Tests: Basic transformation, metadata enrichment

2. **synthetic_jax_tissue_data.csv**
   - Gene expression in "Right Cerebral Hemisphere" tissue
   - LOAD1 model with real genotypes
   - Tests: JAX tissue name mapping to "Hemibrain"

3. **synthetic_mixed_genes_data.csv**
   - Contains both mouse (ENSMUSG*) and human (ENSG*) genes
   - APOE4 model
   - Tests: Human gene filtering (only mouse genes should be kept)

4. **synthetic_rounding_precision_data.csv**
   - Expression values with 9 decimal places
   - APOE4 model
   - Tests: Rounding to 5 decimal places

5. **synthetic_age_sorting_data.csv**
   - Single gene with data at multiple ages (12, 6, 3 months) in unsorted order
   - APOE4 model
   - Tests: Numeric age sorting

6. **synthetic_single_row_data.csv**
   - Minimal dataset with single individual measurement
   - Gene not in metadata (ENSMUSG00000000008)
   - APOE4 model
   - Tests: Edge case handling, missing metadata

7. **synthetic_empty_data.csv**
   - Empty data file (headers only)
   - Tests: Empty file error handling

8. **synthetic_missing_columns_data.csv**
   - Missing required 'age' column
   - APOE4 model
   - Tests: Missing column error handling

## Output Files

Each output JSON file contains the expected transformed data structure:

```json
[
  {
    "ensembl_gene_id": "ENSMUSG00000000001",
    "gene_symbol": "Gnai3",
    "tissue": "Cortex",
    "name": "APOE4",
    "model_group": "APOE4",
    "matched_control": "C57BL/6J",
    "units": "Log2 Counts per Million",
    "age": "6 months",
    "age_numeric": 6,
    "result_order": ["C57BL/6J", "APOE4"],
    "data": [
      {
        "genotype": "APOE4",
        "sex": "Male",
        "individual_id": "Ind001",
        "value": 5.12345
      },
      ...
    ]
  },
  ...
]
```

### Key Output Features

1. **Unnested Structure**: Each age creates a separate output entry (not nested in individual_results)
2. **Result Order**: Display labels sorted by result_order value (controls first)
3. **Matched Control**: The genotype with minimum result_order present in the data
4. **Model Group**: null for empty string, actual value otherwise
5. **Age Numeric**: Extracted numeric value for sorting
6. **Tissue Mapping**: JAX tissues mapped (e.g., "Right Cerebral Hemisphere" → "Hemibrain")
7. **Rounding**: Expression values rounded to 5 decimal places

## Test Coverage

### Unit Tests

1. **TestCreateGenotypeMetadataDict**: Tests genotype metadata dictionary creation
2. **TestDetermineResultOrder**: Tests result ordering logic
3. **TestCreateOutputEntryFromGroup**: Tests output entry creation with metadata
4. **TestProcessIndividualDataFileCore**: Tests single file processing logic

### Integration Tests

1. **test_synthetic_basic_data**: Core transformation functionality
2. **test_synthetic_jax_tissue_mapping**: JAX tissue name mapping
3. **test_synthetic_mixed_genes_filtering**: Human gene filtering
4. **test_synthetic_age_sorting**: Numeric age sorting
5. **test_synthetic_single_row_data**: Edge case with minimal data
6. **test_synthetic_empty_data_file**: Empty file error handling
7. **test_synthetic_missing_columns_data**: Missing column error handling
8. **test_synthetic_rounding_precision**: Numeric precision rounding
9. **test_inconsistent_model_group_values**: Inconsistent model_group error handling

## Notes

- **Column Names**: Use lowercase `individualid` (not `individualID`)
- **Matched Control**: Determined from actual data, not all possible genotypes
- **Result Order**: May contain duplicate display labels when multiple models share the same control genotype
- **Model Group**: Empty strings are converted to `null` in JSON output
- **Gene Metadata**: Missing genes result in empty string for gene_symbol (e.g. ENSMUSG00000000008 is intentionally absent from `synthetic_mouse_gene_metadata.csv`)
- **Real Genotypes**: Tests use actual genotypes from production data (e.g., "APOE4-KI_homozygous; Trem2-R47H_WT")
- **Real Models**: Tests use actual model names (APOE4, LOAD1, Abca7*V1599M, etc.)

## Running Tests

```bash
# Activate conda environment
conda activate adt_py310

# Install package
pip install .

# Run all rna_de_individual tests
python -m pytest tests/transform/test_rna_de_individual.py -v

# Run specific test class
python -m pytest tests/transform/test_rna_de_individual.py::TestTransformRnaDeIndividual -v

# Run specific test
python -m pytest tests/transform/test_rna_de_individual.py::TestTransformRnaDeIndividual::test_synthetic_basic_data -v
```

