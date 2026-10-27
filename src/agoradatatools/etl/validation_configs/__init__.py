"""Input validation specs, one module per input file.

Each module in this package is named exactly as the input file is named in the
pipeline config (the name field of the file entry, e.g. genotype_label_map) and
defines a single module-level SPEC of type InputSpec. The Input Validation stage
discovers the modules in this package at import time, so adding a file here is
the only step needed to validate a new input.

Specs are written against the column names produced by standardize_column_names,
before any dataset-specific column_rename, so one spec covers every dataset that
reads the file. Where consumers disagree about what a file must contain, the spec
is the union.
"""
