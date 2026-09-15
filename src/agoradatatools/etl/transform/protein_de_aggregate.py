"""Protein differential-expression aggregate transform for Model AD.

RNA-aggregate grouping over protein-resolved rows. Biology (model, sex, age,
tissue, and optional case/control) comes from the file's columns, or from the
config file_map overlay when a column is absent. Filenames are not parsed.
"""

import gc
import logging
from typing import Any

import pandas as pd

from agoradatatools.etl.utils import (
    check_column_rules,
    check_required_datasets_and_columns,
    validate_one_to_one_mapping,
    ColumnRule,
    NotEmptyRule,
)
from agoradatatools.etl.transform.transform_utils.model_ad_transform_utils import (
    remap_sex_labels,
)
from agoradatatools.etl.transform.transform_utils.model_ad_expression_utils import (
    GENOTYPE_LABEL_MAP_COLUMNS,
    GENOTYPE_LABEL_MAP_RULES,
    build_model_to_model_group,
    create_age_entries_from_group,
    create_gene_metadata_dict,
    filter_to_mouse_genes,
    normalize_tissue,
    prepare_genotype_label_map,
    validate_and_sort_age_entries,
    validate_data_file_not_empty,
)
from agoradatatools.etl.transform.transform_utils.model_ad_proteomics_utils import (
    build_gene_aliases,
    build_uniprot_candidates,
    canonical_accession,
    pairs_from_protein_ids,
    protein_display_symbol,
    protein_unique_id,
    resolve_gene_ids,
)

logger = logging.getLogger(__name__)

BIOLOGY_REQUIRED = ("model", "sex", "age", "tissue")
BIOLOGY_OPTIONAL = ("case", "control")
FOLD_CHANGE_COLUMNS = ("log2foldchange", "diff")

REQUIRED_INPUT = {
    "genotype_label_map": GENOTYPE_LABEL_MAP_COLUMNS + ["model_type"],
    "mouse_gene_metadata": ["ensembl_gene_id", "gene_symbol", "alias"],
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
    "uniprot_ensembl_map": ["uniprotkb_accession", "ensembl_gene_id"],
}

COLUMN_RULES: dict[str, dict[str, list[ColumnRule]]] = {
    "genotype_label_map": {
        **GENOTYPE_LABEL_MAP_RULES,
        "model_type": [NotEmptyRule()],
    },
    "uniprot_ensembl_map": {
        "uniprotkb_accession": [NotEmptyRule()],
        "ensembl_gene_id": [NotEmptyRule()],
    },
}


def _data_file_names(
    datasets: dict[str, pd.DataFrame],
    required_input: dict[str, list[str]],
    file_map: dict[str, dict[str, str]] | None,
) -> list[str]:
    """Return leftover dataset keys as DE files; reject file_map keys with no file."""
    file_list = [name for name in datasets if name not in required_input]
    if not file_list:
        raise ValueError(
            "No differential expression files found. Add DE files under files; "
            "they are the leftover keys after the required metadata datasets."
        )
    overlay = file_map or {}
    if unknown := sorted(set(overlay) - set(file_list)):
        raise ValueError(
            f"file_map names {unknown}, which are not differential expression "
            "files in this dataset. Correct the name in the config or add the "
            "file to the dataset's files. Inputs available: "
            f"{', '.join(sorted(file_list))}."
        )
    return file_list


def _fill_biology(
    data_file: pd.DataFrame,
    file_name: str,
    overlay: dict[str, str],
) -> pd.DataFrame:
    """Fill model, sex, age, and tissue from the file column, else the file_map overlay.

    Column wins when present. Overlay fills a missing column. Neither is a
    filename parse: Synapse names are not a contract.
    """
    data_file = data_file.copy()
    for field in BIOLOGY_REQUIRED:
        if field in data_file.columns:
            continue
        if field in overlay:
            data_file[field] = overlay[field]
            continue
        raise ValueError(
            f"Data file '{file_name}' is missing '{field}': no column and no "
            "file_map overlay. Add the column to the file or declare the "
            f"value under custom_transformations.file_map.{file_name}."
        )
    for field in BIOLOGY_OPTIONAL:
        if field not in data_file.columns and field in overlay:
            data_file[field] = overlay[field]
    return data_file


def _ensure_log2foldchange(data_file: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """Rename diff to log2foldchange so the shared age helper stays RNA-compatible."""
    if "log2foldchange" in data_file.columns:
        return data_file
    if "diff" in data_file.columns:
        return data_file.rename(columns={"diff": "log2foldchange"})
    raise ValueError(
        f"Data file '{file_name}' is missing a fold-change column "
        "(diff or log2foldchange)."
    )


def _model_display_labels(
    genotype_label_map_df: pd.DataFrame,
) -> dict[str, dict[str, str]]:
    """Map each model to name and matched_control from min/max result_order labels."""
    labels: dict[str, dict[str, str]] = {}
    for model, group in genotype_label_map_df.groupby("model"):
        ordered = group.sort_values("result_order")
        labels[model] = {
            "matched_control": ordered.iloc[0]["display_label"],
            "name": ordered.iloc[-1]["display_label"],
        }
    return labels


def _resolve_name_and_control(
    model: str,
    group: pd.DataFrame,
    label_map_dict: dict[tuple[str, str], str],
    model_display_labels: dict[str, dict[str, str]],
    ensembl_gene_id: str,
    tissue: str,
    sex: str,
) -> tuple[str, str]:
    """Resolve name and matched_control from case/control, else from result_order."""
    case = group["case"].iloc[0] if "case" in group.columns else None
    control = group["control"].iloc[0] if "control" in group.columns else None
    has_case = pd.notna(case)
    has_control = pd.notna(control)
    if has_case ^ has_control:
        raise ValueError(
            f"Data for model '{model}' has only one of case/control "
            f"(case={case!r}, control={control!r}). Provide both or neither."
        )
    if has_case and has_control:
        case_key = (model, case)
        control_key = (model, control)
        missing = [key for key in (case_key, control_key) if key not in label_map_dict]
        if missing:
            raise ValueError(
                "Label mapping not found for genotype. "
                f"Model: '{model}', missing (model, genotype) {missing}. "
                f"Gene: {ensembl_gene_id}, Tissue: {tissue}, Sex: {sex}."
            )
        return label_map_dict[case_key], label_map_dict[control_key]
    if model not in model_display_labels:
        raise ValueError(
            f"Model '{model}' is not in the genotype label map, so name and "
            "matched_control cannot be resolved. Add the model to the label "
            "map or correct the file / file_map."
        )
    resolved = model_display_labels[model]
    return resolved["name"], resolved["matched_control"]


def _process_data_file(
    file_name: str,
    data_file: pd.DataFrame,
    overlay: dict[str, str],
    uniprot_to_ensembl: dict[str, str],
    gene_symbols: dict[str, str],
) -> pd.DataFrame:
    """Validate, fill biology, resolve proteins, and keep the grouping columns."""
    validate_data_file_not_empty(file_name, data_file)
    check_required_datasets_and_columns(
        {file_name: data_file}, {file_name: ["protein_id", "padj"]}
    )
    check_column_rules(
        {file_name: data_file}, {file_name: {"protein_id": [NotEmptyRule()]}}
    )
    data_file = _ensure_log2foldchange(data_file, file_name)
    data_file = _fill_biology(data_file, file_name, overlay)
    data_file["tissue"] = normalize_tissue(data_file["tissue"])
    data_file["sex"] = remap_sex_labels(data_file["sex"])
    data_file = data_file.round(decimals=5)

    data_file["uniprotid"] = canonical_accession(data_file["protein_id"].astype(str))
    # Isoform accessions (Q8C8R3-2) inherit the base accession's gene mapping;
    # the full accession stays in the output so distinct proteoforms stay distinct.
    data_file["ensembl_gene_id"] = (
        data_file["uniprotid"].str.split("-").str[0].map(uniprot_to_ensembl)
    )
    data_file = data_file.dropna(subset=["ensembl_gene_id"])
    if data_file.empty:
        raise ValueError(
            f"No rows remained in '{file_name}' after mapping proteins to "
            "genes — check the UniProt to Ensembl mapping file."
        )
    data_file = filter_to_mouse_genes(data_file)
    if data_file.empty:
        raise ValueError(
            f"No mouse genes remained in '{file_name}' after filtering "
            "human Ensembl ids."
        )
    data_file["gene_symbol"] = data_file["ensembl_gene_id"].map(gene_symbols).fillna("")
    data_file["unique_id"] = protein_unique_id(
        data_file["ensembl_gene_id"], data_file["uniprotid"]
    )
    data_file["display_symbol"] = protein_display_symbol(
        data_file["gene_symbol"], data_file["ensembl_gene_id"], data_file["uniprotid"]
    )
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
    optional = [col for col in BIOLOGY_OPTIONAL if col in data_file.columns]
    return data_file[keep + optional]


def _build_output_entry(
    group: pd.DataFrame,
    biodomain_dict: dict[str, list[str]],
    label_map_dict: dict[tuple[str, str], str],
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
    if model not in model_group_dict:
        raise ValueError(
            f"Model '{model}' is not in the genotype label map. Add the "
            "model to the label map or correct the file / file_map."
        )
    name, matched_control = _resolve_name_and_control(
        model,
        group,
        label_map_dict,
        model_display_labels,
        ensembl_gene_id,
        tissue,
        sex,
    )
    age_entries = create_age_entries_from_group(
        group, ensembl_gene_id, model, tissue, sex
    )
    sorted_ages = validate_and_sort_age_entries(
        age_entries, ensembl_gene_id, model, tissue, sex
    )
    return {
        "ensembl_gene_id": ensembl_gene_id,
        "gene_symbol": row.gene_symbol,
        "uniprotid": row.uniprotid,
        "unique_id": row.unique_id,
        "display_symbol": row.display_symbol,
        # Biodomains come from biodom_genes_mm, same source as rna_de_aggregate.
        "biodomains": biodomain_dict.get(ensembl_gene_id, []),
        "name": {"link_url": f"models/{name}", "link_text": name},
        "matched_control": matched_control,
        "model_group": model_group_dict[model],
        "model_type": model_type_dict.get(model, ""),
        "tissue": tissue,
        "sex": sex,
        **sorted_ages,
    }


def transform_protein_de_aggregate(
    datasets: dict[str, pd.DataFrame],
    file_map: dict[str, dict[str, str]] | None = None,
    required_input: dict[str, list[str]] = REQUIRED_INPUT,
    column_rules: dict[str, dict[str, list[ColumnRule]]] = COLUMN_RULES,
) -> list[dict[str, Any]]:
    """Aggregate protein DE files into one Comparison Tool entry per protein x tissue x sex."""
    check_required_datasets_and_columns(datasets, required_input)
    check_column_rules(datasets, column_rules)

    file_list = _data_file_names(datasets, required_input, file_map)
    overlay = file_map or {}
    for file_name in file_list:
        validate_data_file_not_empty(file_name, datasets[file_name])
        check_required_datasets_and_columns(
            {file_name: datasets[file_name]},
            {file_name: ["protein_id", "padj"]},
        )
        check_column_rules(
            {file_name: datasets[file_name]},
            {file_name: {"protein_id": [NotEmptyRule()]}},
        )
        if not any(col in datasets[file_name].columns for col in FOLD_CHANGE_COLUMNS):
            raise ValueError(
                f"Data file '{file_name}' is missing a fold-change column "
                "(diff or log2foldchange)."
            )

    genotype_label_map_df = prepare_genotype_label_map(datasets["genotype_label_map"])
    validate_one_to_one_mapping(genotype_label_map_df, "model", "model_type")
    label_map_dict = genotype_label_map_df.set_index(["model", "genotype"])[
        "display_label"
    ].to_dict()
    model_group_dict = build_model_to_model_group(genotype_label_map_df)
    model_type_dict = (
        genotype_label_map_df.drop_duplicates("model")
        .set_index("model")["model_type"]
        .to_dict()
    )
    model_display_labels = _model_display_labels(genotype_label_map_df)
    gene_symbols = create_gene_metadata_dict(datasets["mouse_gene_metadata"])
    biodom_genes_mm_df = datasets["biodom_genes_mm"].dropna(
        axis="index", subset=["ensembl_id"]
    )
    biodomain_dict = (
        biodom_genes_mm_df[["ensembl_id", "biodomain"]]
        .drop_duplicates()
        .groupby("ensembl_id")["biodomain"]
        .apply(list)
        .to_dict()
    )

    header_pairs = pd.concat(
        [pairs_from_protein_ids(datasets[name]["protein_id"]) for name in file_list],
        ignore_index=True,
    ).drop_duplicates()
    uniprot_to_ensembl = resolve_gene_ids(
        header_pairs=header_pairs,
        candidates=build_uniprot_candidates(datasets["uniprot_ensembl_map"]),
        gene_symbols=gene_symbols,
        gene_aliases=build_gene_aliases(datasets["mouse_gene_metadata"]),
    )

    frames = []
    for file_name in file_list:
        frames.append(
            _process_data_file(
                file_name,
                datasets[file_name],
                overlay.get(file_name, {}),
                uniprot_to_ensembl,
                gene_symbols,
            )
        )
    combined = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()

    group_cols = [
        "unique_id",
        "ensembl_gene_id",
        "uniprotid",
        "gene_symbol",
        "model",
        "tissue",
        "sex",
    ]
    if "case" in combined.columns and "control" in combined.columns:
        group_cols.extend(["case", "control"])

    output = [
        _build_output_entry(
            group,
            biodomain_dict,
            label_map_dict,
            model_group_dict,
            model_type_dict,
            model_display_labels,
        )
        for _, group in combined.groupby(group_cols, dropna=False)
    ]
    output.sort(key=lambda entry: (entry["unique_id"], entry["tissue"], entry["sex"]))
    logger.info(f"Transform protein_de_aggregate total output entries: {len(output)}")
    return output
