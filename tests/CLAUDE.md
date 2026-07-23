<!-- Last reviewed: 2026-03 -->

## Project

Test suite for the ADT ETL pipeline. Tests are data-driven: input CSV files are transformed and compared against expected JSON output files.

## Conventions

### Test asset structure

Each transform has test data in `test_assets/<transform_name>/`:
- `input/*.csv` — Input data files (also `.tsv`, `.feather`, `.yaml` for some transforms)
- `output/*.json` — Expected output after transformation

When adding or modifying a transform, update both input and output test assets to match. Multiple input scenarios are common: good data, missing data, duplicates, extra columns, error conditions.

### Test organization

- Tests are class-based: `class TestTransformGeneInfo`, `class TestTransformOverallScores`, etc.
- Class attributes define paths: `data_files_path`, `pass_test_data`, `pass_test_ids`, `fail_test_ids`
- Use `@pytest.mark.parametrize` with tuples of (input_files, expected_output_file, test_id)
- Separate test classes for auxiliary/utility functions within the same test file (e.g., `TestGetCenterLinkUrl`, `TestCreateLookup`)
- Failure test cases use `pytest.raises(ErrorType, match="regex")` with specific error types and message patterns

### Fixtures

- `conftest.py` provides a single session-scoped `syn` fixture — a mocked `synapseclient.Synapse` via `mock.create_autospec`
- No real Synapse connections in unit tests. Integration tests run in CI via `adt` CLI against preprod configs.
- Some test classes use `@pytest.fixture(autouse=True)` for per-test setup/teardown (common in `test_gx.py`, `test_process.py`)

### Mocking patterns

- **Transform tests** (tests/transform/): No mocking — use real file I/O with test assets. Tests run actual transformation logic.
- **Unit tests** (test_extract, test_load, test_process, test_reporter): Heavy mocking with `patch.object()` on pandas, synapseclient, and internal functions. Use `patch.stopall()` in teardown.
- `mock.create_autospec()` used for realistic mocking with signature validation (not just `Mock()`)
- `test_numpyencoder.py` uses `unittest.TestCase` base class (exception, not the norm)

### Comparison patterns

- Reset DataFrame indices before comparison — Synapse table indices can cause false mismatches
- For non-deterministic ordering (e.g., missing age entries), sort results by a key column before comparing
- `pd.testing.assert_frame_equal()` for DataFrame comparisons
- Direct `assert output == expected` for list[dict] and dict comparisons (common in Model AD transforms)
- `json.load(open(...))` for loading expected JSON output files

### gene_info test specifics

`test_gene_info.py` has a custom `read_input_files_dict()` helper that dispatches file loading by extension (`.feather`, `.tsv`, `.csv`). Failure cases override specific files in a base `core_files` dict — always `.copy()` the base dict before updating.

## Constraints

- `test_assets/` is excluded from all pre-commit linting hooks — test data files are not subject to code quality checks
- When a transform's output format changes, ALL related test output JSON files must be updated to match
- The `syn` fixture is session-scoped — do not modify its state in individual tests
