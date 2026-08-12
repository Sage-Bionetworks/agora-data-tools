"""Column-content validation rules and the check_column_rules entry point.

Each ColumnRule subclass encapsulates a single per-value constraint on a DataFrame
column (non-empty, matches a regex, one of an allowed set, numeric, unique, ...).
check_column_rules applies a mapping of dataset -> column -> rules and collects all
violations into a single error.
"""

import re
from abc import ABC, abstractmethod
from typing import Any, Collection, Dict, List

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
        """Return the number of non-null values in *series* that are not numeric.

        A length-0 series yields 0 (no values to fail the rule).
        """
        present = series[series.notna()]
        coerced = pd.to_numeric(present, errors="coerce")
        return int(coerced.isna().sum())


class ValuesAreUniqueRule(ColumnRule):
    """Rule that every value in the column must be unique.

    Rows that share a value are all counted as violations (e.g. two "A" rows count as 2).
    Null handling follows pandas duplicated semantics: multiple nulls are treated as
    duplicates of each other and counted, but a single null is not a violation.
    """

    rule = "values_are_unique"

    def count_violations(self, series: pd.Series) -> int:
        """Return the number of rows in *series* that share a value with another row.

        A length-0 series yields 0 (no values to fail the rule).
        """
        return int(series.duplicated(keep=False).sum())


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
