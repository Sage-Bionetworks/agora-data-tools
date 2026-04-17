from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional
import re

import pandas as pd
import synapseclient
import yaml
import errno
import sys


@dataclass
class ColumnRule:
    """Describes a single content rule for a DataFrame column.

    Attributes:
        rule: The type of rule to apply. One of:
            - "not_empty": every value must be non-null and non-empty string
            - "matches_regex": every value must fully match the regex pattern in `value`
              (e.g. ``value="^ENSMUSG"`` to enforce a prefix)
            - "contains": every value must contain `value` as a substring
            - "one_of": every value must be a member of the collection `value`
        value: The expected regex pattern, substring, or allowed set, depending on `rule`.
               Not required for "not_empty".
    """

    rule: Literal["not_empty", "matches_regex", "contains", "one_of"]
    value: Optional[Any] = None


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
    prefix = f"In dataset '{dataset_name}', column '{col_name}': "

    if rule.rule == "not_empty":
        bad_count = (
            df[col_name].isna() | (df[col_name].astype(str).str.strip() == "")
        ).sum()
        if bad_count > 0:
            return [f"{prefix}{bad_count} row(s) violate rule 'not_empty'."]

    elif rule.rule == "matches_regex":
        bad_count = (~df[col_name].astype(str).str.match(rule.value, na=False)).sum()
        if bad_count > 0:
            return [
                f"{prefix}{bad_count} row(s) violate rule 'matches_regex' (pattern={rule.value!r})."
            ]

    elif rule.rule == "contains":
        bad_count = (~df[col_name].str.contains(rule.value, na=False)).sum()
        if bad_count > 0:
            return [
                f"{prefix}{bad_count} row(s) violate rule 'contains' (value={rule.value!r})."
            ]

    elif rule.rule == "one_of":
        bad_count = (~df[col_name].isin(rule.value)).sum()
        if bad_count > 0:
            return [
                f"{prefix}{bad_count} row(s) violate rule 'one_of' (value={rule.value!r})."
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

    Example::

        COLUMN_RULES = {
            "mouse_gene_metadata": {
                "ensembl_gene_id": [ColumnRule(rule="matches_regex", value="^ENSMUSG")],
            },
            "rnaseq_genotype_label_map": {
                "model": [ColumnRule(rule="not_empty")],
                "model_group": [ColumnRule(rule="not_empty")],
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


def _login_to_synapse(authtoken: str = None) -> object:
    syn = synapseclient.Synapse()
    if authtoken is None:
        syn.login()
    else:
        syn.login(authToken=authtoken)
    return syn


def _get_config(config_path: str = None):
    if not config_path:
        config_path = "./config.yaml"

    file = None
    config = None

    try:
        file = open(config_path, "r")
        config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print("File not found.  Please provide a valid path.")
        sys.exit(errno.ENOENT)
    except yaml.parser.ParserError or yaml.scanner.ScannerError:
        print("Invalid file.  Please provide a valid YAML file.")
        sys.exit(errno.EBADF)
    return config
