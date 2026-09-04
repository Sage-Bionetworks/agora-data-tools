# Protein Individual Expression Transform - Synthetic Test Datasets

Synthetic fixtures for `transform_protein_de_individual`, which reshapes wide Model AD
proteomics files into the RNA-style nested output. All values are invented; only the
column names, genotype vocabulary, and model relationships mirror production.

One fixture set covers the happy path for a single model, asserted by
`test_synthetic_basic_data`: the wide-to-long melt, a header with no gene symbol, the
harmonized metadata join, UniProt to Ensembl mapping, tissue mapping, and exclusion of an
animal whose genotype has no label map row (`i3`, `geno_fullwt`).

| File | Notes |
|---|---|
| `synthetic_genotype_label_map.csv` | The two LOAD2 rows, matching production: `geno_hom` displays as LOAD2, `geno_wt` as its LOAD1 control |
| `synthetic_harmonized_metadata.csv` | Three animals, one of which is unmappable |
| `synthetic_basic_data.csv` | Three protein columns, one with an empty header symbol, plus a `specimenid` column the melt must ignore |
| `synthetic_uniprot_ensembl_map.csv` | Three accessions; `Q00003` maps to a gene with no symbol, exercising the `display_symbol` fallback. The column is `ensembl_gene_id`, the name the config's `column_rename` gives the source file's `resource_identifier` |
| `synthetic_mouse_gene_metadata.json` | Two genes, so the third accession has no symbol. JSON with populated `alias` lists, matching the production `format: json` — as a CSV the aliases read back as a scalar and `_build_gene_aliases` silently returned nothing |

Multi-model support is covered by
`test_per_model_group_fields_are_not_shared_across_groups`, which builds its frames inline
rather than from fixtures. It uses the real Model AD shape, where a model group can be fed
by more than one model split across more than one file, as the UCI studies are: LOAD2 has
two genotypes and Bin1K358R has four, and that asymmetry is the point, since a
`result_order` computed once over the whole frame rather than per group would give the
LOAD2 entries a four-label list.

## Regenerating

`output/synthetic_basic_output.json` is a golden file produced by running the transform and
verifying the result by hand against the tables above. If a deliberate output change makes
it stale, regenerate by running the transform over the input files and re-checking the
`name`, `matched_control`, and `result_order` values before committing.
