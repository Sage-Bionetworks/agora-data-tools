# Synthetic Datasets for transform_rna_de_aggregate()

This directory contains human-readable synthetic datasets designed to test the `transform_rna_de_aggregate()` function. The datasets are designed to be easy to track and verify, with clear, predictable values.

## Dataset Overview

### Input Data Files

#### Main Data Files (RNA-seq differential expression data)
- **`synthetic_basic_data.csv`**: Basic test case with 2 genes, 2 ages, simple values
- **`synthetic_multi_model_data.csv`**: Multiple models (B, C) with different tissues and ages
- **`synthetic_jax_tissue_data.csv`**: Tests JAX tissue mapping (Right Cerebral Hemisphere → Hemibrain)
- **`synthetic_mixed_genes_data.csv`**: Contains both mouse (ENSMUSG*) and human (ENSG*) genes
- **`synthetic_age_sorting_data.csv`**: Tests age sorting with ages in non-numerical order
- **`synthetic_single_row_data.csv`**: Single row test case (also tests missing gene metadata handling)
- **`synthetic_rounding_precision_data.csv`**: Tests numeric rounding to 5 decimal places
- **`synthetic_multiple_biodomains_data.csv`**: Tests genes with multiple biodomain assignments
- **`synthetic_null_model_group_data.csv`**: Tests empty model_group conversion to null
- **`synthetic_empty_data.csv`**: Empty data file for error testing
- **`synthetic_missing_columns_data.csv`**: Missing required columns for error testing

#### Metadata Files
- **`synthetic_rnaseq_genotype_label_map.csv`**: Model to display label mapping
- **`synthetic_mouse_gene_metadata.csv`**: Gene ID to symbol mapping
- **`synthetic_model_info.csv`**: Model metadata (matched controls, model type)
- **`synthetic_biodom_genes_mm.csv`**: Biodomain assignments for genes
- **`synthetic_biodom_genes_mm_multiple.csv`**: Biodomain assignments with genes having multiple biodomains
- **`synthetic_mouse_gene_metadata_multi.csv`**: Extended gene metadata for multi-biodomain tests
- **`synthetic_rnaseq_genotype_label_map_no_group.csv`**: Label mapping with empty model_group
- **`synthetic_rnaseq_genotype_label_map_inconsistent.csv`**: Label mapping with inconsistent model_group values (for error testing)
- **`synthetic_model_info_no_group.csv`**: Model metadata with model having no model_group

### Output Files
- **`synthetic_*_output.json`**: Expected output for each test case


## Test Scenarios

### Scenario 1: Basic Data (`synthetic_basic_data.csv`)
```
Genes: Gene_A (ENSMUSG00000000001), Gene_B (ENSMUSG00000000002)
Model: Model_A
Ages: 3 months, 6 months
Values: Simple 1.0, 2.0, -1.5, -2.5
Expected: 2 output entries with age-sorted data
```

### Scenario 2: Multi-Model Data (`synthetic_multi_model_data.csv`)
```
Models: Model_B, Model_C
Tissues: Hippocampus, Cortex
Ages: 3, 6, 12 months (for Model_C)
Expected: 2 output entries with different models and tissues
```

### Scenario 3: JAX Tissue Mapping (`synthetic_jax_tissue_data.csv`)
```
Model: JAX_Model
Tissue: Right Cerebral Hemisphere
Expected: Tissue mapped to "Hemibrain"
```

### Scenario 4: Gene Filtering (`synthetic_mixed_genes_data.csv`)
```
Genes: Mouse (ENSMUSG00000000005, ENSMUSG00000000006), Human (ENSG00000000001)
Expected: Only mouse genes in output
```

### Scenario 5: Age Sorting (`synthetic_age_sorting_data.csv`)
```
Ages: 12 months, 3 months, 6 months (input order)
Expected: Sorted as 3 months, 6 months, 12 months
```

### Scenario 6: Single Row Data (`synthetic_single_row_data.csv`)
```
Gene: ENSMUSG00000000008 (not in mouse_gene_metadata.csv)
Model: Model_F
Age: 9 months
Expected: Single output entry with gene_symbol="" and biodomains=[]
Tests: Minimal dataset + missing gene metadata handling
```

### Scenario 7: Rounding Precision (`synthetic_rounding_precision_data.csv`)
```
Input values: 1.123456789, 0.0123456789, 2.987654321, 0.9876543219
Expected values: 1.12346, 0.01235, 2.98765, 0.98765
Tests: Numeric rounding to exactly 5 decimal places
```

### Scenario 8: Multiple Biodomains (`synthetic_multiple_biodomains_data.csv`)
```
Gene: ENSMUSG00000000005 (Gene_MultiDomain)
Biodomains: Synaptic AND Metabolic
Expected: biodomains=["Synaptic", "Metabolic"]
Tests: Genes belonging to multiple biodomains
```

### Scenario 9: Null Model Group (`synthetic_null_model_group_data.csv`)
```
Model: Model_NoGroup (has empty model_group in metadata)
Expected: model_group=null (not empty string)
Tests: Empty string to null conversion for model_group field
```

### Scenario 10: Inconsistent Model Group (`synthetic_rnaseq_genotype_label_map_inconsistent.csv`)
```
Model: Model_A with inconsistent model_group values
  - Tg genotype → GroupX
  - Wt genotype → GroupY
Model: Model_B with consistent model_group values
  - Tg genotype → GroupZ
  - Wt genotype → GroupZ
Expected: ValueError raised identifying Model_A as having inconsistent model_group values
Tests: Validation of model_group consistency across genotypes within same model
```

## Data Grouping and Case/Control Logic

### Grouping Strategy
The transform function groups data by the following key dimensions:
- `ensembl_gene_id`: Mouse gene identifier (ENSMUSG*)
- `model`: Model identifier (e.g., Model_A, APP/PS1, 5xFAD)
- `tissue`: Tissue type (e.g., Brain, Hippocampus, Cortex)
- `sex`: Sex (Male/Female)
- `case`: Case genotype (e.g., Tg, Wt)
- `control`: Control genotype (e.g., Tg, Wt)

**Important**: Each unique combination of these 6 dimensions creates **one output entry**. Multiple ages within the same group are aggregated as age-based entries within that single output object.

### Case/Control Mapping
The `case` and `control` columns in the input data represent the comparison being made:
- **Case**: The experimental condition (e.g., transgenic "Tg")
- **Control**: The control condition (e.g., wild-type "Wt")

The `rnaseq_genotype_label_map.csv` file maps these genotypes to human-readable display labels:
- `case` → `name` (e.g., "Model_A (Tg)")
- `control` → `matched_control` (e.g., "Model_A (Wt)")

### Example Grouping
Given input data:
```
ensembl_gene_id,model,case,control,age,sex,tissue
ENSMUSG00000000001,Model_A,Tg,Wt,3 months,Female,Brain
ENSMUSG00000000001,Model_A,Tg,Wt,6 months,Female,Brain
ENSMUSG00000000001,Model_A,Tg,Wt,3 months,Male,Brain
```

This creates **2 output entries**:
1. Gene + Model_A + Female + Brain + Tg/Wt → contains ages 3 months, 6 months
2. Gene + Model_A + Male + Brain + Tg/Wt → contains age 3 months

## Validation Points

When testing, verify:
1. **Gene filtering**: Only ENSMUSG* genes in output
2. **Tissue mapping**: "Right Cerebral Hemisphere" → "Hemibrain"
3. **Age sorting**: Ages sorted numerically (3, 6, 12 months)
4. **Biodomain assignment**: Correct biodomains from metadata
5. **Label mapping**: Correct display labels from genotype map
6. **Model metadata**: Correct model type and matched controls
7. **Grouping logic**: One output entry per unique gene+model+tissue+sex+case+control combination
8. **Case/control mapping**: Correct name and matched_control values from genotype labels
9. **Numeric rounding**: log2_fc and adj_p_val rounded to exactly 5 decimal places
10. **Multiple biodomains**: Genes can have multiple biodomain assignments in a list
11. **Missing metadata**: Gene not in metadata → gene_symbol="" and biodomains=[]
12. **Null model_group**: Empty model_group converted to null in output
13. **Model group consistency**: Each model must have consistent model_group values across all genotypes (raises ValueError if inconsistent)

## File Structure
```
tests/test_assets/rna_de_aggregate/
├── input/
│   ├── Main Data Files:
│   │   ├── synthetic_basic_data.csv
│   │   ├── synthetic_multi_model_data.csv
│   │   ├── synthetic_jax_tissue_data.csv
│   │   ├── synthetic_mixed_genes_data.csv
│   │   ├── synthetic_age_sorting_data.csv
│   │   ├── synthetic_single_row_data.csv
│   │   ├── synthetic_rounding_precision_data.csv
│   │   ├── synthetic_multiple_biodomains_data.csv
│   │   ├── synthetic_null_model_group_data.csv
│   │   ├── synthetic_empty_data.csv
│   │   └── synthetic_missing_columns_data.csv
│   └── Metadata Files:
│       ├── synthetic_rnaseq_genotype_label_map.csv
│       ├── synthetic_mouse_gene_metadata.csv
│       ├── synthetic_model_info.csv
│       ├── synthetic_biodom_genes_mm.csv
│       ├── synthetic_biodom_genes_mm_multiple.csv
│       ├── synthetic_mouse_gene_metadata_multi.csv
│       ├── synthetic_rnaseq_genotype_label_map_no_group.csv
│       ├── synthetic_rnaseq_genotype_label_map_inconsistent.csv
│       └── synthetic_model_info_no_group.csv
├── output/
│   ├── synthetic_basic_output.json
│   ├── synthetic_multi_model_output.json
│   ├── synthetic_jax_tissue_output.json
│   ├── synthetic_mixed_genes_output.json
│   ├── synthetic_age_sorting_output.json
│   ├── synthetic_single_row_output.json
│   ├── synthetic_rounding_precision_output.json
│   ├── synthetic_multiple_biodomains_output.json
│   └── synthetic_null_model_group_output.json
└── README_synthetic_datasets.md
```
