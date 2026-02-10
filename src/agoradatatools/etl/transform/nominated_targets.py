import numpy as np
import pandas as pd

from agoradatatools.etl.utils import nest_fields


def transform_nominated_targets(datasets: dict) -> pd.DataFrame:
    """
    This function creates a dataset called nominated_targets.
    """
    gene_metadata = datasets["gene_metadata"]
    target_list = datasets["target_list"]
    pharos_classes = datasets["pharos_classes"]

    # Group data by ensembl_gene_id before merging
    nested_targets = nest_fields(
        df=target_list,
        grouping="ensembl_gene_id",
        new_column="target_object",
        drop_columns=["ensembl_gene_id"],
    )

    nested_pharos = nest_fields(
        df=pharos_classes,
        grouping="ensembl_gene_id",
        new_column="pharos_object",
        drop_columns=["ensembl_gene_id"],
    )

    # Merge all the datasets
    nominated_targets = gene_metadata

    for dataset in [
        nested_targets,
        nested_pharos
        #pharos_class
    ]:
        nominated_targets = pd.merge(
            left=nominated_targets,
            right=dataset,
            on="ensembl_gene_id",
            how="inner",
            validate="one_to_one",
        )

    # create 'total_nominations' field by counting grouped nominations
    nominated_targets["total_nominations"] = nominated_targets.apply(
        lambda row: (
            len(row["target_object"])
            if isinstance(row["target_object"], list)
            else np.NaN
        ),
        axis=1,
    )

    # create 'year_first_nominated' field by finding the smallest initial_nomination value
    nominated_targets["initial_nomination"] = nominated_targets.apply(
        lambda row: (
            min((item["initial_nomination"] for item in row["target_object"] if item.get("initial_nomination")), default=np.NaN)
            if isinstance(row["target_object"], list) and row["target_object"]
            else np.NaN
        ),
        axis=1,
    )

    # Create 'nominating_teams' field by collecting unique team values
    nominated_targets["nominating_teams"] = nominated_targets.apply(
        lambda row: (
            list({item["team"] for item in row["target_object"] if item.get("team")})
            if isinstance(row["target_object"], list)
            else []
        ),
        axis=1,
    )

    # Create 'cohort_studies' field - splits strings like "Rush, MSBB" into individual items
    nominated_targets["cohort_studies"] = nominated_targets.apply(
        lambda row: (
            list({
                sub_item.strip()
                for item in row["target_object"] if item.get("study")
                for sub_item in str(item["study"]).split(",")
            })
            if isinstance(row["target_object"], list)
            else []
        ),
        axis=1,
    )

    # Create 'input_data' field by collecting unique input_data values
    nominated_targets["input_data"] = nominated_targets.apply(
        lambda row: (
            list({
                sub_item.strip()
                for item in row["target_object"] if item.get("input_data")
                for sub_item in str(item["input_data"]).split(",")
            })
            if isinstance(row["target_object"], list)
            else []
        ),
        axis=1,
    )

    # Create 'programs' field by collecting unique source values
    nominated_targets["programs"] = nominated_targets.apply(
        lambda row: (
            list({item["source"] for item in row["target_object"] if item.get("source")})
            if isinstance(row["target_object"], list)
            else []
        ),
        axis=1,
    )

    # Genes can have multiple pharos_class values; we only want the 'most interesting' single value
    # Prioritized list of values, Tclin >>> Tdark
    PHAROS_PRIORITY = ["Tclin", "Tchem", "Tbio", "Tdark"]

    # Resolve the highest priority single value
    def resolve_pharos_class(pharos_list):
        if not isinstance(pharos_list, list):
            return None

        # Extract all classes present in the objects
        found_classes = {item.get("pharos_class") for item in pharos_list if item.get("pharos_class")}

        # Return the first one that matches the priority list
        for p_class in PHAROS_PRIORITY:
            if p_class in found_classes:
                return p_class

        return None

    # Create 'pharos_class' field
    nominated_targets["pharos_class"] = nominated_targets["pharos_object"].apply(resolve_pharos_class)


    # Keep only the columns we need
    nominated_targets = nominated_targets[
        [
            "ensembl_gene_id",
            "symbol",
            "total_nominations",
            "initial_nomination",
            "nominating_teams",
            "cohort_studies",
            "input_data",
            "programs",
            "pharos_class"
        ]
    ]

    # Make sure there are no N/A Ensembl IDs
    nominated_targets = nominated_targets.dropna(subset=["ensembl_gene_id"])

    return nominated_targets
