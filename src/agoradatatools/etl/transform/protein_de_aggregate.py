"""Protein differential-expression aggregate transform for Model AD.

RNA-aggregate grouping over protein-resolved rows. Model, sex, age, and tissue
come from file_metadata in the config. name and matched_control come from the
genotype label map: the display labels with the lowest and highest result_order.
"""

import logging
from typing import Any

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    raise_on_duplicate_keys,
    validate_one_to_one_mapping,
    ColumnRule,
    ContainsSubstringRule,
    MatchesRegexRule,
    NotEmptyRule,
    OneOfRule,
)
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    GENOTYPE_LABEL_MAP_COLUMNS,
    GENOTYPE_LABEL_MAP_RULES,
    build_biodomain_lookup,
    build_model_to_model_group_lookup,
    build_model_type_lookup,
    create_age_entries_from_group,
    create_gene_metadata_dict,
    determine_result_order,
    normalize_tissue,
    prepare_genotype_label_map,
    validate_data_file,
)
from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    add_protein_identity_columns,
    build_uniprot_to_ensembl,
    canonical_accession,
    log_unmapped_accessions,
    map_accessions_to_genes,
)

logger = logging.getLogger(__name__)

METADATA_FIELDS = ("model", "sex", "age", "tissue")
GROUP_KEY = ["model", "tissue", "sex", "age"]
AGE_KEY = ["unique_id", "model", "tissue", "sex", "age"]

REQUIRED_INPUT = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_COLUMNS + ["model_type"],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol"],
    "biodom_genes_mm": [
        "biodomain",
        "abbr",
        "label",
        "color",
        "go_id",
        "goterm_name",
        "n_symbol",
        "symbol",
        "ensembl_id",
    ],
    "uniprot_ensembl_map": ["uniprot_id", "ensembl_gene_id"],
}

COLUMN_RULES: dict[str, dict[str, list[ColumnRule]]] = {
    "genotype_label_map": {
        **GENOTYPE_LABEL_MAP_RULES,
        "model_type": [NotEmptyRule()],
    },
    # create_gene_metadata_dict indexes on ensembl_gene_id, so a blank one becomes a live
    # lookup key rather than being dropped.
    "mouse_gene_metadata": {"ensembl_gene_id": [NotEmptyRule()]},
    "uniprot_ensembl_map": {
        "uniprot_id": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule()],
    },
}

DATA_FILE_COLUMNS = ["protein_id", "log2foldchange", "padj"]
# protein_id is gene_symbol|uniprotid; the pipe is what canonical_accession splits on.
DATA_FILE_COLUMN_RULES = {"protein_id": [NotEmptyRule(), ContainsSubstringRule("|")]}


def _validate_file_metadata(
    datasets: dict[str, pd.DataFrame],
    file_metadata: dict[str, dict[str, str]],
    required_input: dict[str, list[str]],
    known_models: set[str],
) -> dict[str, dict[str, str]]:
    """Return file metadata with sex and tissue normalized.

    Sex and tissue are normalized once here, not per row: the values are typed
    into the config. Two files that resolve to the same model, tissue, sex, and
    age raise, because both would write one age of one output entry.
    """
    available = ", ".join(sorted(set(datasets) - set(required_input))) or "(none)"
    if not file_metadata:
        raise ValueError(
            "No file_metadata provided. Each differential expression file's "
            "model, sex, age, and tissue have to be declared in the config "
            f"under custom_transformations. Data files available: {available}."
        )
    if unknown_files := sorted(set(file_metadata) - set(datasets)):
        raise ValueError(
            f"file_metadata names {unknown_files}, which are not files in this "
            "dataset. Correct the name in the config or add the file to the "
            f"dataset's files. Data files available: {available}."
        )
    if not_data_files := sorted(set(file_metadata) & set(required_input)):
        raise ValueError(
            f"{not_data_files} are metadata datasets, not differential expression "
            "files. Remove them from file_metadata."
        )
    leftover = sorted(set(datasets) - set(required_input) - set(file_metadata))
    if leftover:
        raise ValueError(
            f"{leftover} are not in file_metadata. Add them to the config or "
            "remove them from this dataset's files."
        )

    if not_mappings := [
        name for name, meta in file_metadata.items() if not isinstance(meta, dict)
    ]:
        raise ValueError(
            f"file_metadata for {not_mappings} must be a mapping of "
            f"{', '.join(METADATA_FIELDS)}."
        )

    # One row per file so the shared column checks report every problem at once.
    meta_df = pd.DataFrame.from_dict(file_metadata, orient="index")
    check_required_datasets_and_columns(
        {"file_metadata": meta_df}, {"file_metadata": list(METADATA_FIELDS)}
    )
    check_column_rules(
        {"file_metadata": meta_df},
        {
            "file_metadata": {
                "model": [NotEmptyRule(), OneOfRule(known_models)],
                "sex": [NotEmptyRule()],
                "age": [NotEmptyRule(), MatchesRegexRule(r"^\d+ months$")],
                "tissue": [NotEmptyRule()],
            }
        },
    )
    meta_df["sex"] = remap_sex_labels(meta_df["sex"])
    meta_df["tissue"] = normalize_tissue(meta_df["tissue"])
    raise_on_duplicate_keys(
        meta_df,
        GROUP_KEY,
        "More than one file resolves to the same model, tissue, sex, and age, "
        "so which fold change belongs on the page cannot be decided.",
    )
    return meta_df[list(METADATA_FIELDS)].to_dict(orient="index")


def _model_display_labels(
    genotype_label_map_df: pd.DataFrame,
    models: set[str],
) -> dict[str, dict[str, str]]:
    """Map each used model to name and matched_control.

    matched_control is the display label with the lowest result_order and name
    is the one with the highest. There is no case or control column to
    disambiguate a third label, so a used model must have exactly two.
    """
    labels: dict[str, dict[str, str]] = {}
    for model in models:
        group = genotype_label_map_df[genotype_label_map_df["model"] == model]
        ordered = determine_result_order(group)
        if len(ordered) != 2:
            raise ValueError(
                f"Model '{model}' has {len(ordered)} display labels in the "
                "genotype label map. protein_de_aggregate needs exactly two, "
                f"the comparison and its matched control. Found: {ordered}."
            )
        labels[model] = {
            "matched_control": ordered[0],
            "name": ordered[-1],
        }
    return labels


def _process_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    overlay: dict[str, str],
    uniprot_to_ensembl: dict[str, str],
    gene_symbols: dict[str, str],
) -> pd.DataFrame:
    """Stamp biology from file_metadata, resolve proteins, and keep the grouping columns."""
    data_file = data_file.copy()
    data_file["model"] = overlay["model"]
    data_file["sex"] = overlay["sex"]
    data_file["age"] = overlay["age"]
    data_file["tissue"] = overlay["tissue"]
    data_file = data_file.round(decimals=5)

    data_file["uniprotid"] = canonical_accession(data_file["protein_id"].astype(str))
    data_file = map_accessions_to_genes(
        data_file, uniprot_to_ensembl, f"in '{file_name}'"
    )
    data_file = add_protein_identity_columns(data_file, gene_symbols)
    keep = [
        "unique_id",
        "ensembl_gene_id",
        "uniprotid",
        "gene_symbol",
        "display_symbol",
        "model",
        "tissue",
        "sex",
        "age",
        "log2foldchange",
        "padj",
    ]
    return data_file[keep]


def _build_output_entry(
    group: pd.DataFrame,
    biodomain_dict: dict[str, list[str]],
    model_group_dict: dict[str, str],
    model_type_dict: dict[str, str],
    model_display_labels: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Build one Comparison Tool entry from a protein x tissue x sex group."""
    row = group.iloc[0]
    ensembl_gene_id = row.ensembl_gene_id
    model = row.model
    tissue = row.tissue
    sex = row.sex
    resolved = model_display_labels[model]
    name, matched_control = resolved["name"], resolved["matched_control"]
    age_entries = create_age_entries_from_group(
        group, ensembl_gene_id, model, tissue, sex
    )
    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": row.gene_symbol,
        "uniprotid": row.uniprotid,
        "unique_id": row.unique_id,
        "display_symbol": row.display_symbol,
        "biodomains": biodomain_dict.get(ensembl_gene_id, []),
        "name": {"link_url": f"models/{name}", "link_text": name},
        "matched_control": matched_control,
        "model_group": model_group_dict[model],
        "model_type": model_type_dict[model],
        "tissue": tissue,
        "sex": sex,
        **age_entries,
    }


def transform_protein_de_aggregate(
    datasets: dict[str, pd.DataFrame],
    file_metadata: dict[str, dict[str, str]],
    required_input: dict[str, list[str]] = REQUIRED_INPUT,
    column_rules: dict[str, dict[str, list[ColumnRule]]] = COLUMN_RULES,
) -> list[dict[str, Any]]:
    """Aggregate protein DE files into one Comparison Tool entry per protein x tissue x sex."""
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])
    validate_one_to_one_mapping(genotype_label_map_df, "model", "model_type")
    overlay = _validate_file_metadata(
        datasets,
        file_metadata,
        required_input,
        set(genotype_label_map_df["model"].unique()),
    )
    model_group_dict = build_model_to_model_group_lookup(genotype_label_map_df)
    model_type_dict = build_model_type_lookup(genotype_label_map_df)
    model_display_labels = _model_display_labels(
        genotype_label_map_df, {meta["model"] for meta in overlay.values()}
    )

    file_list = list(overlay)
    for index, file_name in enumerate(file_list):
        validate_data_file(
            file_name,
            datasets[file_name],
            index,
            len(file_list),
            DATA_FILE_COLUMNS,
            DATA_FILE_COLUMN_RULES,
        )

    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])
    biodomain_dict = build_biodomain_lookup(datasets["biodom_genes_mm"])

    uniprot_to_ensembl = build_uniprot_to_ensembl(datasets["uniprot_ensembl_map"])
    measured = pd.concat(
        [
            canonical_accession(datasets[name]["protein_id"].astype(str))
            for name in file_list
        ],
        ignore_index=True,
    ).drop_duplicates()
    log_unmapped_accessions(measured, uniprot_to_ensembl)

    combined = pd.concat(
        [
            _process_data_file(
                file_name,
                datasets[file_name],
                overlay[file_name],
                uniprot_to_ensembl,
                gene_symbols,
            )
            for file_name in file_list
        ],
        ignore_index=True,
    )
    raise_on_duplicate_keys(
        combined,
        AGE_KEY,
        "More than one row shares a protein, model, tissue, sex, and age, so "
        "which fold change belongs on the page cannot be decided.",
    )

    group_cols = [
        "unique_id",
        "ensembl_gene_id",
        "uniprotid",
        "gene_symbol",
        "model",
        "tissue",
        "sex",
    ]

    output = [
        _build_output_entry(
            group,
            biodomain_dict,
            model_group_dict,
            model_type_dict,
            model_display_labels,
        )
        for _, group in combined.groupby(group_cols, dropna=False)
    ]
    output.sort(key=lambda entry: (entry["unique_id"], entry["tissue"], entry["sex"]))
    logger.info(f"Transform protein_de_aggregate total output entries: {len(output)}")
    return output
