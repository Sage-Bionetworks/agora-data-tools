import numpy as np
import pandas as pd

from agoradatatools.etl.utils import nest_fields

def transform_drug_info(
    datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called drug_info by joining drug metadata drawn from OpenTargets with the
    list of drug nominations. By design, the drug metadata file
    """

    # TODO: what about rows with 2 chembl_id values? (cefaclor + [not provided: CEFACLOR ANHYDROUS], CHEMBL680 + CHEMBL1201018, Krumseik)
    # TODO: what about rows with 2 common_name values? (Letrozole + irinotecan, [not provided -> CHEMBL1201 + CHEMBL494], Sirota)

    # TODO: special handling for Combination Therapy
    # - Details: 2 pages, use a distinct banner with a linkage to the other drug page
    # --- need a way to determine which banner(s) should be displayed on each drug page [nomination_type: drug, combo, or both]
    # - CT: I think this has to be 2 rows, or we break our nav to details page paradigm
    #       If we use inverted compound values for the drug name columns,then we don't need to try to aggregate across drugs and combos or add nom_type to the table
    #
    # --- Letrozole ... Letrozole info & link...
    # --- Letrozole (with Irinotecan) ... Letrozole info & link...
    # --- Irinotecan (with Letrozole)... Irinotecan info & link...
    # - Source data & ETL considerations: how to generate the output we want with the correct linkages / name variants?
    # --- current source: 1 row for Letrozole, 1 row for Letrozole + Irinotecan
    # --- option 1: 1 row for Letrozole, 1 row for Letrozole (with Irinotecan), 1 row for Irinotecan (with Letrozole)
    # ------ # nominations no longer strictly map 1:1 to CT rows or the harmonized source file
    # ------ # Each details page has its own copy of the original combo nomination, need to ensure no drift over time
    # ------ # Transform logic is straightforward
    # --- option 2: keep 1 row for the combo, engineer the transform to detect combos based on input data
    # ------ # nominations no longer strictly map 1:1 to CT rows, but do still map 1:1 to the harmonized source file
    # ------ # Each details page shares the same combo nomination, ensuring no drift over time
    # ------ # Requires more complex transform logic


    # TODO year_of...to inital_nomination for consistency with CT, check drugs too

    drug_metadata = datasets["drug_metadata"]
    drug_list = datasets["drug_list"]
    gene_metadata = datasets["gene_metadata"] # TODO resolve hgnc_symbols for ENSG link display values

    # Clean and prepare drug_metadata
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

    # Clean & prepare drug_list data
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

    # Rename source
    drug_list = datasets["drug_list"].rename(columns={"source": "program"})

    # Nest drug_list nomination fields
    # Temporarily replace NA iupac_id values
    drug_list["iupac_id"] = drug_list["iupac_id"].fillna("Unknown")

    # Nest nomination fields, keep drug fields at the top level
    drug_list = nest_fields(
        df=drug_list,
        grouping=["chembl_id", "common_name", "iupac_id"],
        new_column="drug_nominations",
        drop_columns=["priority_score", "priority_score_criteria",
                      "computational_validation_status", "experimental_validation_status",
                      "computational_validation_results", "experimental_validation_results"],
    )

    # collapse duplicates when multiple nominations for the same drug
    drug_list = drug_list.groupby("chembl_id", as_index=False).agg({
        "common_name": "first",
        "iupac_id": "first",
        "drug_nominations": "sum"
    })

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

    drug_info = drug_info.rename(columns={"source": "program"})

    return drug_info


