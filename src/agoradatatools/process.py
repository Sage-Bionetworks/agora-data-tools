import logging
import typing
from enum import Enum

import synapseclient
from pandas import DataFrame
from typer import Argument, Option, Typer

from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.etl import extract, load, transform, utils
from agoradatatools.gx import GreatExpectationsRunner
from agoradatatools.logs import log_time

logger = logging.getLogger(__name__)


class Platform(Enum):
    LOCAL = "LOCAL"
    GITHUB = "GITHUB"
    NEXTFLOW = "NEXTFLOW"


# TODO refactor to avoid so many if's - maybe some sort of mapping to callables
def apply_custom_transformations(datasets: dict, dataset_name: str, dataset_obj: dict):
    if not isinstance(datasets, dict) or not isinstance(dataset_name, str):
        return None
    if dataset_name == "biodomain_info":
        return transform.transform_biodomain_info(datasets=datasets)
    if dataset_name == "genes_biodomains":
        return transform.transform_genes_biodomains(datasets=datasets)
    if dataset_name == "overall_scores":
        df = datasets["overall_scores"]
        return transform.transform_overall_scores(df=df)
    if dataset_name == "distribution_data":
        return transform.transform_distribution_data(
            datasets=datasets,
            overall_max_score=dataset_obj["custom_transformations"][
                "overall_max_score"
            ],
            genetics_max_score=dataset_obj["custom_transformations"][
                "genetics_max_score"
            ],
            omics_max_score=dataset_obj["custom_transformations"]["omics_max_score"],
        )
    if dataset_name == "team_info":
        return transform.transform_team_info(datasets=datasets)
    if dataset_name == "rnaseq_differential_expression":
        return transform.transform_rnaseq_differential_expression(datasets=datasets)
    if dataset_name == "gene_info":
        return transform.transform_gene_info(
            datasets=datasets,
            adjusted_p_value_threshold=dataset_obj["custom_transformations"][
                "adjusted_p_value_threshold"
            ],
            protein_level_threshold=dataset_obj["custom_transformations"][
                "protein_level_threshold"
            ],
        )
    if dataset_name == "rna_distribution_data":
        return transform.transform_rna_distribution_data(datasets=datasets)
    if dataset_name == "proteomics_distribution_data":
        return transform.transform_proteomics_distribution_data(datasets=datasets)
    else:
        return None


@log_time(func_name="process_dataset", logger=logger)
def process_dataset(
    dataset_obj: dict,
    staging_path: str,
    gx_folder: str,
    syn: synapseclient.Synapse,
    upload: bool = True,
) -> None:
    """Takes in a dataset from the configuration file and passes it through the ETL process

    Args:
        dataset_obj (dict): A dataset defined in the configuration file
        staging_path (str): Staging path
        gx_folder (str): Synapse ID of the folder where Great Expectations reports should be uploaded
        syn (synapseclient.Synapse): synapseclient.Synapse session.
        upload (bool, optional): Whether or not to upload the data to Synapse. Defaults to True.
    """

    dataset_name = list(dataset_obj.keys())[0]
    entities_as_df = {}

    for entity in dataset_obj[dataset_name]["files"]:
        entity_id = entity["id"]
        entity_format = entity["format"]
        entity_name = entity["name"]

        df = extract.get_entity_as_df(syn_id=entity_id, source=entity_format, syn=syn)
        df = utils.standardize_column_names(df=df)
        df = utils.standardize_values(df=df)

        # the column rename gets applied to all entities in a dataset
        if "column_rename" in dataset_obj[dataset_name].keys():
            df = utils.rename_columns(
                df=df, column_map=dataset_obj[dataset_name]["column_rename"]
            )

        entities_as_df[entity_name] = df

    if "custom_transformations" in dataset_obj[dataset_name].keys():
        df = apply_custom_transformations(
            datasets=entities_as_df,
            dataset_name=dataset_name,
            dataset_obj=dataset_obj[dataset_name],
        )
    else:
        df = entities_as_df[list(entities_as_df)[0]]

    if "agora_rename" in dataset_obj[dataset_name].keys():
        df = utils.rename_columns(
            df=df, column_map=dataset_obj[dataset_name]["agora_rename"]
        )

    if isinstance(df, dict):
        json_path = load.dict_to_json(
            df=df,
            staging_path=staging_path,
            filename=dataset_name + "." + dataset_obj[dataset_name]["final_format"],
        )
    else:
        json_path = load.df_to_json(
            df=df,
            staging_path=staging_path,
            filename=dataset_name + "." + dataset_obj[dataset_name]["final_format"],
        )

    # run great expectations on dataset if expectation suite exists
    if "gx_enabled" in dataset_obj[dataset_name].keys():
        gx_runner = GreatExpectationsRunner(
            syn=syn,
            dataset_path=json_path,
            dataset_name=dataset_name,
            upload_folder=gx_folder if upload else None,
            nested_columns=(
                dataset_obj[dataset_name]["gx_nested_columns"]
                if "gx_nested_columns" in dataset_obj[dataset_name].keys()
                else None
            ),
        )
        gx_runner.run()

    if upload:
        load.load(
            file_path=json_path,
            provenance=dataset_obj[dataset_name]["provenance"],
            destination=dataset_obj[dataset_name]["destination"],
            syn=syn,
        )


def create_data_manifest(
    syn: synapseclient.Synapse, parent: synapseclient.Folder = None
) -> typing.Union[DataFrame, None]:
    """Creates data manifest (dataframe) that has the IDs and version numbers of child synapse folders

    Args:
        syn (synapseclient.Synapse): Synapse client session.
        parent (synapseclient.Folder/str, optional): synapse folder or synapse id pointing to parent synapse folder. Defaults to None.

    Returns:
        Dataframe containing IDs and version numbers of folders within the parent directory, or None if parent is None
    """

    if not parent:
        return None

    folders = syn.getChildren(parent)
    folder = [
        {"id": folder["id"], "version": folder["versionNumber"]} for folder in folders
    ]

    return DataFrame(folder)


@log_time(func_name="process_all_files", logger=logger)
def process_all_files(
    syn: synapseclient.Synapse,
    config_path: str = None,
    platform: Platform = Platform.LOCAL,
    upload: bool = True,
):
    """This function will read through the entire configuration and process each file listed.

    Args:
        syn (synapseclient.Session): Synapse client session
        config_path (str, optional): path to configuration file. Defaults to None.
        platform (Platform, optional): Platform where the process is being run. One of LOCAL, GITHUB, NEXTFLOW. Defaults to LOCAL.
        upload (bool, optional): Whether or not to upload the data to Synapse. Defaults to True.
    """
    if platform == Platform.LOCAL and upload is True:
        logger.warning(
            """Data will be uploaded to Synapse despite the platform being set to `LOCAL`.
            Make sure you have provided a configuration file with alternative upload `destination` and `gx_folder`.
            See the contributing guide for more information."""
        )

    config = utils._get_config(config_path=config_path)

    datasets = config["datasets"]
    destination = config["destination"]

    # create staging location
    staging_path = config.get("staging_path", None)
    if staging_path is None:
        staging_path = "./staging"

    load.create_temp_location(staging_path)

    error_list = []
    if datasets:
        for dataset in datasets:
            try:
                process_dataset(
                    dataset_obj=dataset,
                    staging_path=staging_path,
                    gx_folder=config["gx_folder"],
                    syn=syn,
                    upload=upload,
                )
            except Exception as e:
                error_list.append(
                    f"{list(dataset.keys())[0]}: " + str(e).replace("\n", "")
                )

    if error_list:
        raise ADTDataProcessingError(
            "\nData Processing has failed for one or more data sources. Refer to the list of errors below to address issues:\n"
            + "\n".join(error_list)
        )

    manifest_df = create_data_manifest(syn=syn, parent=destination)
    manifest_path = load.df_to_csv(
        df=manifest_df, staging_path=staging_path, filename="data_manifest.csv"
    )

    if upload:
        load.load(
            file_path=manifest_path,
            provenance=manifest_df["id"].to_list(),
            destination=destination,
            syn=syn,
        )


app = Typer()

input_path_arg = Argument(..., help="Path to configuration file for processing run")

platform_opt = Option(
    "LOCAL",
    "--platform",
    "-p",
    help="Platform that is running the process. Must be one of LOCAL, GITHUB, or NEXTFLOW.",
    show_default=True,
)
upload_opt = Option(
    False,
    "--upload",
    "-u",
    help="Toggles whether or not files will be uploaded to Synapse.",
    show_default=True,
)
synapse_auth_opt = Option(
    None,
    "--token",
    "-t",
    help="Synapse authentication token. Defaults to environment variable $SYNAPSE_AUTH_TOKEN via syn.login() functionality",
    show_default=False,
)


@app.command()
def process(
    config_path: str = input_path_arg,
    platform: str = platform_opt,
    upload: bool = upload_opt,
    auth_token: str = synapse_auth_opt,
):
    syn = utils._login_to_synapse(token=auth_token)
    platform_enum = Platform(platform)
    process_all_files(
        syn=syn, config_path=config_path, platform=platform_enum, upload=upload
    )


if __name__ == "__main__":
    app()
