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
- **`synthetic_single_row_data.csv`**: Single row test case
- **`synthetic_empty_data.csv`**: Empty data file for error testing
- **`synthetic_missing_columns_data.csv`**: Missing required columns for error testing

#### Metadata Files
- **`synthetic_rnaseq_genotype_label_map.csv`**: Model to display label mapping
- **`synthetic_mouse_gene_metadata.csv`**: Gene ID to symbol mapping
- **`synthetic_model_info.csv`**: Model metadata (matched controls, model type)
- **`synthetic_biodom_genes_mm.csv`**: Biodomain assignments for genes

### Output Files
- **`synthetic_*_output.json`**: Expected output for each test case

## Key Features of Synthetic Data

### 1. **Human-Readable Values**
- Gene IDs: `ENSMUSG00000000001`, `ENSMUSG00000000002`, etc.
- Gene symbols: `Gene_A`, `Gene_B`, `Gene_C`, etc.
- Models: `Model_A`, `Model_B`, `Model_C`, etc.
- Simple numeric values: `1.0`, `2.0`, `-1.5`, etc.

### 2. **Easy to Track**
- Sequential gene IDs and symbols
- Round numbers for log2foldchange and padj values
- Clear model naming convention
- Predictable tissue and age values

### 3. **Comprehensive Test Coverage**
- **Basic functionality**: Simple 2-gene, 2-age case
- **Multi-model**: Different models with different tissues
- **JAX tissue mapping**: Tests "Right Cerebral Hemisphere" → "Hemibrain" conversion
- **Gene filtering**: Tests human gene (ENSG*) filtering
- **Age sorting**: Tests numerical age sorting
- **Edge cases**: Empty data, missing columns, single row

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

## Usage

### Running the Test Script
```bash
cd /path/to/agora-data-tools
python tests/test_assets/rna_de_aggregate/test_synthetic_data.py
```

### Manual Testing
```python
from agoradatatools.etl.transform.rna_de_aggregate import transform_rna_de_aggregate
import pandas as pd

# Load synthetic datasets
datasets = {
    "synthetic_basic_data": pd.read_csv("tests/test_assets/rna_de_aggregate/input/synthetic_basic_data.csv"),
    "rnaseq_genotype_label_map": pd.read_csv("tests/test_assets/rna_de_aggregate/input/synthetic_rnaseq_genotype_label_map.csv"),
    "mouse_gene_metadata": pd.read_csv("tests/test_assets/rna_de_aggregate/input/synthetic_mouse_gene_metadata.csv"),
    "model_info": pd.read_csv("tests/test_assets/rna_de_aggregate/input/synthetic_model_info.csv"),
    "biodom_genes_mm": pd.read_csv("tests/test_assets/rna_de_aggregate/input/synthetic_biodom_genes_mm.csv"),
}

# Transform data
output = transform_rna_de_aggregate(datasets)
print(f"Generated {len(output)} output entries")
```

## Expected Output Structure

Each output entry contains:
```json
{
  "ensembl_gene_id": "ENSMUSG00000000001",
  "gene_symbol": "Gene_A",
  "biodomains": ["Synaptic"],
  "name": "Model_A (Tg)",
  "matched_control": "Model_A (Wt)",
  "model_group": "AD",
  "model_type": "Transgenic",
  "tissue": "Brain",
  "sex": "Female",
  "3 months": {
    "log2_fc": 1.0,
    "adj_p_val": 0.01
  },
  "6 months": {
    "log2_fc": 2.0,
    "adj_p_val": 0.02
  }
}
```

## Validation Points

When testing, verify:
1. **Gene filtering**: Only ENSMUSG* genes in output
2. **Tissue mapping**: "Right Cerebral Hemisphere" → "Hemibrain"
3. **Age sorting**: Ages sorted numerically (3, 6, 12 months)
4. **Biodomain assignment**: Correct biodomains from metadata
5. **Label mapping**: Correct display labels from genotype map
6. **Model metadata**: Correct model type and matched controls

## File Structure
```
tests/test_assets/rna_de_aggregate/
├── input/
│   ├── synthetic_basic_data.csv
│   ├── synthetic_multi_model_data.csv
│   ├── synthetic_jax_tissue_data.csv
│   ├── synthetic_mixed_genes_data.csv
│   ├── synthetic_age_sorting_data.csv
│   ├── synthetic_single_row_data.csv
│   ├── synthetic_empty_data.csv
│   ├── synthetic_missing_columns_data.csv
│   ├── synthetic_rnaseq_genotype_label_map.csv
│   ├── synthetic_mouse_gene_metadata.csv
│   ├── synthetic_model_info.csv
│   └── synthetic_biodom_genes_mm.csv
├── output/
│   ├── synthetic_basic_output.json
│   ├── synthetic_multi_model_output.json
│   ├── synthetic_jax_tissue_output.json
│   ├── synthetic_mixed_genes_output.json
│   ├── synthetic_age_sorting_output.json
│   └── synthetic_single_row_output.json
├── test_synthetic_data.py
└── README_synthetic_datasets.md
```
