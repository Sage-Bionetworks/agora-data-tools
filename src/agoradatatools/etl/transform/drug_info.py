import numpy as np
import pandas as pd

from agoradatatools.etl.utils import nest_fields

# alphabatizes nominations by PI last name, using fancy AI-generated string splitting logic
def sort_by_pi_lastname(noms):
    if not isinstance(noms, list):
        return noms

    def get_sort_key(nomination):
        name = nomination.get("contact_pi")
        if not name or not isinstance(name, str):
            return ""

        # 1. Split by comma and take only the first part
        # "Marina Sirota, Junior" -> "Marina Sirota"
        # "John Smith, PhD, MD" -> "John Smith"
        name_part = name.split(',')[0].strip()

        # 2. Split the remaining name part by whitespace
        parts = name_part.split()

        # 3. Return the last word in lowercase
        return parts[-1].lower() if parts else ""

    # Sort the list of dictionaries
    noms.sort(key=get_sort_key)
    return noms

# finds a valid iupac_id for a group of rows with the same chembl_id
def get_best_iupac_id(group):
    # Drop 'Unknown' and NaNs to find a real ID
    valid_ids = group.dropna()
    valid_ids = valid_ids[valid_ids != "Unknown"]

    if not valid_ids.empty:
        return valid_ids.iloc[0]
    return "Unknown"


def transform_drug_info(
    datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called drug_info by joining drug metadata drawn from OpenTargets with the
    list of drug nominations. By design, the drug metadata file
    """
    drug_metadata = datasets["ot_drug_metadata"]
    drug_list = datasets["drug_list"]
    gene_metadata = datasets["gene_metadata"] # TODO resolve hgnc_symbols for ENSG link display values

    # Validate & prepare drug_list data
    # Validate that each drug_list.chembl_id has exactly one common_name value
    id_counts = drug_list.groupby("chembl_id")["common_name"].nunique()
    if (id_counts > 1).any():
        offending_ids = id_counts[id_counts > 1].index.tolist()
        raise ValueError(
            f"Data Integrity Error: The following chembl_id(s) are associated with multiple "
            f"common_names, which would cause duplicate output rows: {offending_ids}"
        )

    # Ensure that all rows with the same chembl_id use the same iupac_id value, preferring non-null values
    drug_list["iupac_id"] = drug_list.groupby("chembl_id")["iupac_id"].transform(get_best_iupac_id)

    # Clean and prepare drug_metadata data
    # Resolve hgnc_symbol from ensembl_gene_id via gene_metadata
    ensembl_ids = drug_metadata["linked_targets"].explode().unique()
    gene_map = (
        gene_metadata[gene_metadata["ensembl_gene_id"].isin(ensembl_ids)]
        .set_index("ensembl_gene_id")["symbol"]
        .to_dict()
    )

    def resolve_targets(target_list):
        if not isinstance(target_list, list):
            return []

        return [
            {
                "ensembl_gene_id": g_id,
                "hgnc_symbol": gene_map.get(g_id, g_id) # Fallback to ID if symbol missing
            }
            for g_id in target_list if pd.notnull(g_id)
        ]

    drug_metadata["linked_targets"] = drug_metadata["linked_targets"].apply(resolve_targets)

    # Remap Clinical Trial phase values
    phase_map = {
        1: "Phase I",
        2: "Phase II",
        3: "Phase III",
        4: "Phase IV",
        -1: "Unknown"
    }

    # Apply mapping and handle the null (NaN) case specifically
    drug_metadata["maximum_clinical_trial_phase"] = (
        drug_metadata["maximum_clinical_trial_phase"]
        .map(phase_map)
        .fillna("Preclinical")
    )

    # Clean & prepare drug_list data
    # Convert 'published' string to boolean
    # drug_list["published"] = drug_list["published"].map({"Yes": True, "No": False, "yes": True, "no": False})

    # Merge computational & experimental status fields into a single list
    status_cols = ["computational_validation_status", "experimental_validation_status"]
    for col in status_cols:
        drug_list[col] = drug_list[col].str.strip().replace("", np.nan)

    drug_list["validation_status"] = drug_list[status_cols].apply(
        lambda row: [val for val in row if pd.notnull(val)],
        axis=1
    )

    # Merge computational & experimental results fields into a single list
    results_cols = ["computational_validation_results", "experimental_validation_results"]
    for col in results_cols:
        drug_list[col] = drug_list[col].str.strip().replace("", np.nan)

    drug_list["validation_results"] = drug_list[results_cols].apply(
        lambda row: [val for val in row if pd.notnull(val)],
        axis=1
    )

    # Nest drug_list nomination fields
    # Temporarily replace NA iupac_id values
    drug_list["iupac_id"] = drug_list["iupac_id"].fillna("Unknown")

    # Nest nomination fields, keep drug fields at the top level
    drug_list = nest_fields(
        df=drug_list,
        grouping=["chembl_id", "common_name", "iupac_id"],
        new_column="drug_nominations",
        drop_columns=["priority_score", "priority_score_criteria", "published",
                      "computational_validation_status", "experimental_validation_status",
                      "computational_validation_results", "experimental_validation_results"],
    )

    # collapse duplicates when multiple nominations for the same drug
    drug_list = drug_list.groupby("chembl_id", as_index=False).agg({
        "common_name": "first",
        # if more than one iupac_id value for this chembl_id, ensure that we pick one that isn't null
        "iupac_id":  "first",
        "drug_nominations": "sum"
    })

    # sort nested nomination objects by PI last name
    drug_list["drug_nominations"] = drug_list["drug_nominations"].apply(sort_by_pi_lastname)

    # Remove duplicate nested fields
    def clean_nominations(row):
        noms = row['drug_nominations']
        if not isinstance(noms, list):
            return noms

        for d in noms:
            # Remove redundant fields from the nested objects
            for col in ["chembl_id", "common_name", "iupac_id"]:
                d.pop(col, None)
        return noms

    drug_list["drug_nominations"] = drug_list.apply(clean_nominations, axis=1)

    # Undo temp iupac_id change
    drug_list["iupac_id"] = drug_list["iupac_id"].replace("Unknown", None)


    # build drug_info dataset
    drug_info = drug_metadata

    # Merge in other datasets by chembl_id
    for dataset in [
        drug_list
    ]:
        drug_info = pd.merge(
            left=drug_info,
            right=dataset,
            on="chembl_id",
            how="outer", # this may not be the best choice, but I don't want to silently lose any rows
            validate="one_to_one"
        )

    # Convert specific columns to nullable integers
    cols_to_fix = ["year_of_first_approval"]

    for col in cols_to_fix:
        drug_info[col] = drug_info[col].astype("Int64")

    # Order fields
    column_order = [
        "common_name",
        "description",
        "iupac_id",
        "chembl_id",
        "drug_bank_id",
        "aliases",
        "modality",
        "year_of_first_approval",
        "maximum_clinical_trial_phase",
        "linked_targets",
        "mechanisms_of_action",
        "drug_nominations"
    ]
    drug_info = drug_info.reindex(columns=column_order)


    # This function capitalizes only the first character of a string,
    # without modifying any other characters to preserve valid values like APOE or DRIAD-SP
    # Handles single values and lists, for top-level and nested fields
    # Thanks, Gemini!
    def apply_sentence_case(df, fields):
        """
        Capitalizes the first character of a string, without modifying casing for any other characters to
        preserve valid values like APOE or DRIAD-SP.

        df: The DataFrame to modify
        fields: A list of strings. Use 'parent_object.field' for nested keys.
        """

        def capitalize_text(text):
            if not isinstance(text, str) or not text:
                return text
            return text[0].upper() + text[1:]

        def process_nested(noms, target_key):
            if not isinstance(noms, list):
                return noms
            for d in noms:
                if target_key in d:
                    val = d[target_key]
                    if isinstance(val, list): # Handle validation_status/results
                        d[target_key] = [capitalize_text(i) for i in val]
                    else: # Handle strings if any exist
                        d[target_key] = capitalize_text(val)
            return noms

        for field in fields:
            if "." in field:
                # Handle nested fields (e.g., "drug_nominations.validation_status")
                parent, child = field.split(".")
                df[parent] = df[parent].apply(lambda x: process_nested(x, child))
            else:
                # Handle top-level fields
                if field not in df.columns:
                    continue

                # Check if the column is a list of strings or a single string
                sample = df[field].dropna().iloc[0] if not df[field].dropna().empty else None

                if isinstance(sample, list):
                    df[field] = df[field].apply(lambda x: [capitalize_text(i) for i in x] if isinstance(x, list) else x)
                else:
                    df[field] = df[field].apply(capitalize_text)

        return df

    # Apply consistent sentence casing to select user-supplied and external metadata values
    cols_to_fix = [
        "common_name",
        "description",
        "evidence",
        "data_used",
        "ad_moa",
        "additional_evidence",
        "drug_nominations.validation_status",
        "drug_nominations.validation_results"
    ]

    drug_info = apply_sentence_case(drug_info, cols_to_fix)

    return drug_info

