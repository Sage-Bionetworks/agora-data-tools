"""Validation helpers shared by the Input Validation stage and the transforms.

ColumnRule and its subclasses express per-value constraints on one column.
The check_* and validate_* functions apply them to DataFrames or enforce
whole-frame integrity properties.
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Collection, Dict, List

import pandas as pd


class ColumnRule(ABC):
    """Abstract base class for a single content rule applied to a DataFrame column.

    Subclasses implement ``count_violations()`` to count rows that fail the rule.
    Rules that require a comparison value (e.g. a regex pattern) enforce that the
    value is provided at construction time and raise ``ValueError`` if it is ``None``.
    """

    rule: str  # Subclasses declare this as a class-level attribute.

    @abstractmethod
    def count_violations(self, series: pd.Series) -> int:
        """Return the number of values in *series* that violate this rule."""
        ...

    @property
    def value_detail(self) -> str:
        """Human-readable description of the rule's comparison value, if any."""
        return ""


class NotEmptyRule(ColumnRule):
    """Rule that every present value must be non-null and non-empty (after stripping whitespace).

    This rule checks each **cell** in the column, not whether the column has any rows.
    An empty :class:`~pandas.Series` (length 0) produces **zero** violations because there
    are no values to evaluate.
    """

    rule = "not_empty"

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of null, empty, or whitespace-only values in *series*.

        A length-0 *series* yields 0 (no values to fail the rule).
        """
        return int((series.isna() | (series.astype(str).str.strip() == "")).sum())


class MatchesRegexRule(ColumnRule):
    """Rule that every value must match a given regex pattern anchored at the start.

    Uses pandas ``str.match``, which anchors the pattern at the beginning of each
    string. To require a match across the entire value, include a trailing anchor
    in the pattern (e.g. ``"^ENSMUSG\\d+$"`` rather than just ``"^ENSMUSG"``).

    Args:
        value: The regex pattern to match against (e.g. ``"^ENSMUSG"``).

    Raises:
        ValueError: If *value* is not a non-empty string or is an invalid regex pattern.
    """

    rule = "matches_regex"

    def __init__(self, value: str) -> None:
        """Initialize the rule, raising if *value* is not a non-empty string or is an invalid regex."""
        if not isinstance(value, str) or value == "":
            raise ValueError(
                "MatchesRegexRule requires a non-None, non-empty string 'value' (the regex pattern to match against)."
            )
        try:
            re.compile(value)
        except re.error as e:
            raise ValueError(
                f"MatchesRegexRule requires a valid regex pattern: {e}"
            ) from e
        self.value = value

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of non-null values in *series* that do not match the regex at the start.

        Null values are skipped so this rule only validates the format of present
        values; use NotEmptyRule to require presence.
        """
        present = series.notna()
        return int((~series[present].astype(str).str.match(self.value, na=False)).sum())

    @property
    def value_detail(self) -> str:
        """Return the regex pattern formatted for inclusion in a violation message."""
        return f" (value={self.value!r})"


class ContainsSubstringRule(ColumnRule):
    """Rule that every value must contain a given substring.

    Args:
        value: The substring that each value must contain. Matched literally, not as a regex.

    Raises:
        ValueError: If *value* is not a non-empty string (e.g. is ``None``, ``np.nan``, or ``""``).
    """

    rule = "contains_substring"

    def __init__(self, value: str) -> None:
        """Initialize the rule, raising if *value* is not a non-empty string."""
        if not isinstance(value, str) or value == "":
            raise ValueError(
                "ContainsSubstringRule requires a non-None, non-empty string 'value' (the substring to search for)."
            )
        self.value = value

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of values in *series* that do not contain the substring.

        Non-string values are cast to string before checking. The substring is matched
        literally, not as a regex pattern.
        """
        return int(
            (~series.astype(str).str.contains(self.value, na=False, regex=False)).sum()
        )

    @property
    def value_detail(self) -> str:
        """Return the substring formatted for inclusion in a violation message."""
        return f" (value={self.value!r})"


class OneOfRule(ColumnRule):
    """Rule that every value must be a member of a given collection.

    Membership uses :meth:`pandas.Series.isin`, which compares by equality. You may include
    non-string values (e.g. ``None``, ``np.nan``, or numeric codes) in the allowed set if
    you intend those values to be valid.

    **Boolean vs integer:** ``True`` / ``False`` are equal to ``1`` / ``0`` in Python, so
    ``isin`` does not distinguish them from integer ``1`` / ``0``. Normalize dtypes on
    boolean columns before validating if you need strict boolean-only membership.

    Args:
        value: The collection of allowed values (e.g. ``{"male", "female"}``).

    Raises:
        ValueError: If *value* is ``None`` or an empty collection.
    """

    rule = "one_of"

    def __init__(self, value: Collection[Any]) -> None:
        """Initialize the rule with an allowed-values collection, raising if it is ``None`` or empty."""
        if not value:
            raise ValueError(
                "OneOfRule requires a non-None, non-empty 'value' (the collection of allowed values)."
            )
        self.value = value

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of values in *series* not present in the allowed collection."""
        return int((~series.isin(self.value)).sum())

    @property
    def value_detail(self) -> str:
        """Return the allowed collection formatted for inclusion in a violation message."""
        return f" (value={self.value!r})"


class NumericRule(ColumnRule):
    """Rule that every present value must be numeric (parseable as a number).

    Null values are skipped so this rule only validates the type of present values;
    use NotEmptyRule to require presence. Values are checked with pandas to_numeric,
    so numeric strings such as "6" or "9.9" pass.
    """

    rule = "numeric"

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of non-null values in *series* that are not numeric."""
        present = series[series.notna()]
        return int(pd.to_numeric(present, errors="coerce").isna().sum())


class NonNegativeRule(ColumnRule):
    """Rule that every present numeric value must be greater than or equal to zero.

    Null values are skipped so this rule only validates the sign of present values;
    use NotEmptyRule to require presence.

    Values that cannot be parsed as numbers are not counted here. Pair this rule with
    NumericRule on the same column so those values are still reported: doing so keeps a
    single bad cell from being counted as two separate violations.
    """

    rule = "non_negative"

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of non-null, numeric values in *series* that are negative."""
        present = series[series.notna()]
        return int((pd.to_numeric(present, errors="coerce") < 0).sum())


class UniqueRule(ColumnRule):
    """Rule that every present value in a column must appear exactly once.

    Unlike the other rules, this one is a property of the column as a whole rather than of
    each cell, so the violation count is the number of rows belonging to a duplicated group:
    two rows sharing a value count as two violations. The reported count therefore locates how
    much of the column is affected, but not which values repeat.

    Null values are skipped, so a key column that is allowed to be blank on some rows can still
    be checked for uniqueness. This matters because pandas treats null as equal to null, so an
    unfiltered duplicate check would flag a second blank row as a duplicate of the first. Empty
    and whitespace-only strings are not treated as null here; pair this rule with NotEmptyRule
    on columns where blanks are also unacceptable.
    """

    rule = "unique"

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of non-null rows in *series* whose value occurs more than once."""
        present = series[series.notna()]
        return int(present.duplicated(keep=False).sum())


def check_required_datasets_and_columns(
    datasets: Dict[str, pd.DataFrame], required_input: Dict[str, List[str]]
) -> None:
    """
    Check if all required datasets and columns are present in the input dictionary.

    Args:
        datasets (Dict[str, pd.DataFrame]): Dictionary containing dataset names as keys and their corresponding pandas DataFrames as values.
        required_input (Dict[str, List[str]]): Dictionary containing dataset names as keys and their corresponding required columns as values.

    Raises:
        ValueError: If any required columns are missing.
    """
    # Check for missing datasets
    missing_datasets = [
        dataset for dataset in required_input.keys() if dataset not in datasets
    ]
    if missing_datasets:
        raise ValueError(
            f"Missing required datasets: {', '.join(missing_datasets)}. "
            "Please ensure all required datasets are provided {required_datasets}"
        )

    # Check for missing columns
    for dataset_name, columns in required_input.items():
        dataset_columns = datasets[dataset_name].columns
        missing_columns = [col for col in columns if col not in dataset_columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in {dataset_name} dataset: {', '.join(missing_columns)}. "
                f"Please ensure the {dataset_name} dataset contains all required columns. Columns found: {', '.join(dataset_columns)}."
            )


def raise_on_duplicate_keys(df: pd.DataFrame, keys: list[str], message: str) -> None:
    """Raise when more than one row shares the same values for keys.

    message is the sentence that precedes the offending key tuples. At most ten
    tuples are included.
    """
    duplicated = df.duplicated(subset=keys, keep=False)
    if not duplicated.any():
        return
    offenders = sorted(
        df.loc[duplicated, keys].drop_duplicates().itertuples(index=False, name=None)
    )
    raise ValueError(f"{message} (first 10): {offenders[:10]}")


def validate_one_to_one_mapping(
    df: pd.DataFrame,
    left_col: str,
    right_col: str,
    bidirectional: bool = False,
) -> None:
    """Validate that two columns form a consistent mapping.

    By default this checks that each non-null value in left_col maps to at most
    one distinct value in right_col. When bidirectional is True it also checks
    the reverse direction, enforcing a true 1:1 mapping in a single call. Rows
    where the key column is null are ignored for that direction.

    Args:
        df: The DataFrame to validate.
        left_col: The first column in the pair.
        right_col: The second column in the pair.
        bidirectional: If True, validate the mapping in both directions.

    Raises:
        ValueError: If a value in the key column maps to more than one distinct
            value in the paired column.
    """
    _validate_mapping_direction(df, left_col, right_col)
    if bidirectional:
        _validate_mapping_direction(df, right_col, left_col)


def _validate_mapping_direction(df: pd.DataFrame, key_col: str, value_col: str) -> None:
    """Validate that each key in key_col maps to at most one value_col value.

    Groups the DataFrame by key_col (ignoring rows where the key is null) and
    checks that every key is associated with a single distinct value in
    value_col. Null values in value_col are counted as a distinct value.

    Args:
        df: The DataFrame containing the mapping to validate.
        key_col: Name of the column whose values act as mapping keys.
        value_col: Name of the column whose values should be uniquely determined
            by each key.

    Raises:
        ValueError: If any non-null key in key_col maps to more than one
            distinct value in value_col.
    """
    present_keys = df[key_col].notna()
    counts = df.loc[present_keys].groupby(key_col)[value_col].nunique(dropna=False)
    offending_keys = counts[counts > 1].index.tolist()
    if offending_keys:
        raise ValueError(
            f"Data Integrity Error: The following {key_col}(s) are associated with "
            f"multiple {value_col} values: {offending_keys}. "
            f"There must be a one-to-one mapping between {key_col} and {value_col}. "
            "Please fix the source data before re-running."
        )


def validate_references_exist(
    referenced: Collection,
    available: Collection,
    source_name: str,
    target_name: str,
    item_name: str,
) -> None:
    """Raise if any values in referenced are absent from available.

    Used for hand-maintained cross-file keys: a typo in the referencing file
    would otherwise drop those rows silently.

    Args:
        referenced: Values that must exist in available.
        available: The set of allowed values.
        source_name: Name of the dataset that holds the references.
        target_name: Name of the dataset that should contain them.
        item_name: What the values are called in the error message, for example
            models or result columns.

    Raises:
        ValueError: If any referenced value is missing from available.
    """
    missing = sorted(set(referenced) - set(available))
    if missing:
        raise ValueError(
            f"{source_name} references {item_name} that are not present in "
            f"{target_name}: {missing}"
        )


def _check_single_rule(
    df: pd.DataFrame,
    dataset_name: str,
    col_name: str,
    rule: ColumnRule,
) -> List[str]:
    """Check a single ColumnRule against one column and return any violation messages.

    Args:
        df: The DataFrame containing the column to check.
        dataset_name: Name of the dataset (used in violation messages).
        col_name: Name of the column to validate.
        rule: The ColumnRule to apply.

    Returns:
        A list containing a violation message if the rule is broken, or an empty list.
    """
    bad_count = rule.count_violations(df[col_name])
    if bad_count > 0:
        prefix = f"In dataset '{dataset_name}', column '{col_name}': "
        return [
            f"{prefix}{bad_count} row(s) violate rule '{rule.rule}'{rule.value_detail}."
        ]
    return []


def check_column_rules(
    datasets: Dict[str, pd.DataFrame],
    column_rules: Dict[str, Dict[str, List[ColumnRule]]],
) -> None:
    """Validate per-column content rules across one or more datasets.

    Iterates over every (dataset, column, rule) triple in `column_rules` and
    checks that every value in that column satisfies the rule. All violations
    are collected before raising so callers see every problem in a single error.

    Args:
        datasets: Dictionary mapping dataset names to their DataFrames.
        column_rules: Nested mapping of
            dataset_name -> column_name -> list of ColumnRule objects.
            Only datasets and columns present in this mapping are checked.

    Raises:
        ValueError: If any rule is violated. The message lists all violations,
            each naming the dataset, column, rule, and number of offending rows.

    Example:
        datasets = {
            "mouse_gene_metadata": pd.DataFrame({
                "ensembl_gene_id": ["ENSMUSG000000000000000000", "ENSMUSG000000000000000001"],
            }),
            "genotype_label_map": pd.DataFrame({
                "model": ["model1", "model2"],
                "model_group": ["model_group1", "model_group2"],
            }),
        }

        COLUMN_RULES = {
            "mouse_gene_metadata": {
                "ensembl_gene_id": [MatchesRegexRule(value="^ENSMUSG")],
            },
            "genotype_label_map": {
                "model": [NotEmptyRule()],
                "model_group": [NotEmptyRule()],
            },
        }
        check_column_rules(datasets, COLUMN_RULES)
    """
    violations: List[str] = []

    for dataset_name, col_rules in column_rules.items():
        if dataset_name not in datasets:
            continue
        df = datasets[dataset_name]

        for col_name, rules in col_rules.items():
            if col_name not in df.columns:
                violations.append(
                    f"In dataset '{dataset_name}', column '{col_name}' does not exist."
                )
                continue

            for rule in rules:
                violations.extend(_check_single_rule(df, dataset_name, col_name, rule))

    if violations:
        raise ValueError("\n".join(violations))


@dataclass(frozen=True)
class InputSpec:
    """What one input file must look like before any transform may read it.

    Attributes:
        required_columns: Column names that must be present.
        column_rules: Per-column content rules, column name -> list of ColumnRule.
        checks: Whole-frame checks that cannot be expressed as a ColumnRule. Each
            takes the DataFrame and raises ValueError on failure.
    """

    required_columns: list[str]
    column_rules: dict[str, list[ColumnRule]] = field(default_factory=dict)
    checks: tuple[Callable[[pd.DataFrame], None], ...] = ()
