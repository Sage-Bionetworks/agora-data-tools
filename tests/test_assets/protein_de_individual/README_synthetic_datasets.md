# Protein Individual Expression Transform - Synthetic Test Datasets

Synthetic fixtures for `transform_protein_de_individual`, which reshapes wide Model AD
proteomics files into the RNA-style nested output. All values are invented; only the
column names, genotype vocabulary, and model relationships mirror production.

There are two fixture sets, both under `input/` with their expected results under
`output/`.

## Single-model set (`synthetic_basic_*`)

The happy path for one model, asserted by `test_synthetic_basic_data`. Covers the
wide-to-long melt, a header with no gene symbol, the harmonized metadata join, UniProt to
Ensembl mapping, tissue mapping, and exclusion of an animal whose genotype has no label
map row (`i3`, `geno_fullwt`).

| File | Notes |
|---|---|
| `synthetic_genotype_label_map.csv` | The two LOAD2 rows, matching production: `geno_hom` displays as LOAD2, `geno_wt` as its LOAD1 control |
| `synthetic_harmonized_metadata.csv` | Three animals, one of which is unmappable |
| `synthetic_basic_data.csv` | Three protein columns, one with an empty header symbol |
| `synthetic_uniprot_ensembl_map.csv` | Three accessions; `Q00003` maps to a gene with no symbol, exercising the `display_symbol` fallback |
| `synthetic_mouse_gene_metadata.csv` | Two genes, so the third accession has no symbol |

## Multi-model set (`synthetic_multimodel_*`)

The evidence that the transform supports more than one model, asserted by
`test_synthetic_multimodel_data`. Production only has LOAD2 today, so this is the only
place the multi-model output shape is checked end to end.

The shape is taken from real Model AD data rather than invented: a model group can be fed
by more than one model, split across more than one file, which is how the UCI studies are
organized.

| File | Model | Model group |
|---|---|---|
| `synthetic_multimodel_load2_data.csv` | LOAD2 | LOAD2 |
| `synthetic_multimodel_bin1_data.csv` | Bin1-K358R | Bin1K358R |
| `synthetic_multimodel_bin1_5xfad_data.csv` | Bin1-K358R.5xFAD | Bin1K358R |

The two groups deliberately have different genotype counts — LOAD2 has two, Bin1K358R has
four. That asymmetry is the point: `result_order` is computed per model group, so a
`result_order` computed once over the whole frame would produce a four-label list for the
LOAD2 entries and fail the assertion. The same holds for `name` and `matched_control`.

Every animal is around four months old and hemibrain, so the fixture isolates the model
dimension; age bucketing and tissue defaulting are covered by separate inline tests.

The metadata files are shared with the single-model set, since the same two genes and
accessions suffice.

## Regenerating

Both expected outputs are golden files produced by running the transform and verifying the
result by hand against the tables above. If a deliberate output change makes them stale,
regenerate by running the transform over the input files and re-checking the per-group
`name`, `matched_control`, and `result_order` values before committing.
