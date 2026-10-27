"""Input validation specs, one module per file name or shared schema.

Each module in this package defines a single module-level SPEC of type
InputSpec. The module name is the file's name in the pipeline config, or the
schema value when several files share one spec (e.g. rna_de_file). The Input
Validation stage discovers the modules in this package at import time, so
adding a file here is the only step needed to validate a new input.

Specs are written against the column names produced by standardize_column_names,
before any dataset-specific column_rename, so one spec covers every dataset that
reads the file. Where consumers disagree about what a file must contain, the spec
is the union.
"""
