from typing import Dict, List, Optional

import pandas as pd

from agoradatatools.etl.utils import (
    MatchesRegexRule,
    NotEmptyRule,
    OneOfRule,
    check_column_rules,
    check_required_datasets_and_columns,
)

ENSEMBL_GENE_ID_REGEX = r"^ENSG\d{11}$"

# Genes can have multiple pharos_class values;
# we only want the 'most interesting' single value.
# Prioritized list of values, Tclin >>> Tdark
PHAROS_PRIORITY = ["Tclin", "Tchem", "Tbio", "Tdark"]

REQUIRED_INPUT = {
    "target_list": [
        "ensembl_gene_id",
        "team",
        "study",
        "input_data",
        "source",
        "initial_nomination",
    ],
    "gene_metadata": [
        "ensembl_gene_id",
        "symbol",
    ],
    "pharos_classes": [
        "ensembl_gene_id",
        "pharos_class",
    ],
}

COLUMN_RULES = {
    "target_list": {
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(ENSEMBL_GENE_ID_REGEX)],
    },
    "gene_metadata": {
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(ENSEMBL_GENE_ID_REGEX)],
    },
    "pharos_classes": {
        "ensembl_gene_id": [NotEmptyRule(), MatchesRegexRule(ENSEMBL_GENE_ID_REGEX)],
        "pharos_class": [OneOfRule(set(PHAROS_PRIORITY))],
    },
}

_OUTPUT_COLUMNS = [
    "ensembl_gene_id",
    "symbol",
    "total_nominations",
    "initial_nomination",
    "nominating_teams",
    "cohort_studies",
    "input_data",
    "programs",
    "pharos_class",
]


def _sorted_unique_split(series: pd.Series) -> List[str]:
    """Return sorted unique comma-split tokens from a series of strings.

    Each row in the series is a single string that either contains a single
    value or multiple comma-separated values (e.g. "Rush, MSBB"). This
    function separates the comma-separated values, flattens the series into a
    single set of unique values stripped of surrounding whitespace, and sorts
    them alphabetically, so the example yields ["MSBB", "Rush"]. Null values
    are ignored.
    """
    tokens = set()
    flattened = series.str.split(",").explode().dropna()
    for value in flattened.str.strip():
        if value:
            tokens.add(value)
    return sorted(tokens)


def _resolve_pharos_class(pharos_classes: pd.Series) -> Optional[str]:
    """Collapse a gene's pharos_class values to the single highest-priority one.

    Returns the first member of PHAROS_PRIORITY (Tclin > Tchem > Tbio >
    Tdark) present in pharos_classes, or None if none are present.
    """
    found = set(pharos_classes.dropna())
    for level in PHAROS_PRIORITY:
        if level in found:
            return level
    return None


def transform_nominated_targets(
    datasets: Dict[str, pd.DataFrame],
    required_input: Dict[str, List[str]] = REQUIRED_INPUT,
) -> pd.DataFrame:
    """Build the nominated_targets dataset for the Agora Nominated Targets CT interface.

        Each row in the output represents one nominated gene, keyed by its Ensembl
        gene ID, with aggregated nomination details and a single resolved Pharos
        development level. The transform collapses the per-nomination rows in
    target_list into one summary row per gene, then attaches the gene symbol
        and Pharos class.

        All nominated genes are retained: gene_metadata and pharos_classes    are left-merged onto the nominations, so genes without a Pharos class keep a
        null pharos_class instead of being dropped (fixes AG-2064).

        Processing steps:
            1. Validate required datasets and columns.
            2. Validate per-column content rules (Ensembl ID format, allowed Pharos
               levels).
            3. Group target_list by ensembl_gene_id and aggregate: nomination
               count, earliest nomination year, sorted unique nominating teams,
               cohort studies, input data types, and programs.
            4. Resolve a single pharos_class per gene by priority.
            5. Left-merge gene symbols and resolved Pharos classes.
            6. Sort rows for deterministic output.

        Args:
            datasets: Dictionary containing three DataFrames:
                "target_list" (one row per nomination event), "gene_metadata"
                (gene symbols keyed by ensembl_gene_id), and "pharos_classes"
                (Pharos development levels keyed by ensembl_gene_id, possibly
                multiple rows per gene).
            required_input: Required datasets and columns. Defaults to REQUIRED_INPUT.

        Returns:
            DataFrame with columns: ensembl_gene_id, symbol, total_nominations,
            initial_nomination, nominating_teams, cohort_studies, input_data,
            programs, pharos_class.

        Raises:
            ValueError: If required datasets or columns are missing, or column
                content rules are violated.
    """
    check_required_datasets_and_columns(datasets, required_input)

    check_column_rules(datasets, COLUMN_RULES)

    nominated_targets = (
        datasets["target_list"]
        .groupby("ensembl_gene_id")
        .agg(
            total_nominations=("ensembl_gene_id", "size"),
            initial_nomination=("initial_nomination", "min"),
            nominating_teams=("team", lambda x: sorted(set(x.dropna()))),
            cohort_studies=("study", _sorted_unique_split),
            input_data=("input_data", _sorted_unique_split),
            programs=("source", lambda x: sorted(set(x.dropna()))),
        )
        .reset_index()
    )

    # Nullable Int64 (not int): a plain int cast cannot represent missing values,
    # and a float cast would add decimal places to the year.
    nominated_targets["initial_nomination"] = nominated_targets[
        "initial_nomination"
    ].astype("Int64")

    resolved_pharos = (
        datasets["pharos_classes"]
        .groupby("ensembl_gene_id")
        .agg(pharos_class=("pharos_class", _resolve_pharos_class))
        .reset_index()
    )

    nominated_targets = pd.merge(
        left=nominated_targets,
        right=datasets["gene_metadata"][["ensembl_gene_id", "symbol"]],
        on="ensembl_gene_id",
        how="left",
        validate="m:1",
    )

    nominated_targets = pd.merge(
        left=nominated_targets,
        right=resolved_pharos,
        on="ensembl_gene_id",
        how="left",
        validate="m:1",
    )

    nominated_targets = nominated_targets.sort_values("ensembl_gene_id").reset_index(
        drop=True
    )

    nominated_targets = nominated_targets[_OUTPUT_COLUMNS]

    return nominated_targets
