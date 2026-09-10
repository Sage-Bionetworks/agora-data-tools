# Protein Individual Expression Transform - Synthetic Test Datasets

Synthetic fixtures for `transform_protein_de_individual`, asserted by
`test_synthetic_basic_data`. All values are invented; only the column names, genotype
vocabulary, and model relationships mirror production.

| File | Notes |
|---|---|
| `synthetic_genotype_label_map.csv` | The two LOAD2 rows, matching production: `geno_hom` displays as LOAD2, `geno_wt` as its LOAD1 control |
| `synthetic_harmonized_metadata.csv` | Three animals, one of which is unmappable |
| `synthetic_basic_data.csv` | Three protein columns, one with an empty header symbol, plus a `specimenid` column the melt must ignore |
| `synthetic_uniprot_ensembl_map.csv` | Three accessions; `Q00003` maps to a gene with no symbol, exercising the `display_symbol` fallback. The column is `ensembl_gene_id`, the name the config's `column_rename` gives the source file's `resource_identifier` |
| `synthetic_mouse_gene_metadata.json` | Two genes, so the third accession has no symbol. JSON with populated `alias` lists, matching the production `format: json` — as a CSV the aliases read back as a scalar and `_build_gene_aliases` silently returned nothing |

Multi-model support has no fixtures:
`test_per_model_group_fields_are_not_shared_across_groups` builds its frames inline.

`output/synthetic_basic_output.json` is a golden file. If a deliberate output change makes
it stale, re-check `name`, `matched_control`, and `result_order` by hand before committing
the regenerated file.
