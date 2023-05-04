import logging
import synapseclient
from pandas import DataFrame
from typer import Argument, Option, Typer

import agoradatatools.etl.extract as extract
import agoradatatools.etl.load as load
import agoradatatools.etl.transform as transform
import agoradatatools.etl.utils as utils
from agoradatatools.errors import ADTDataProcessingError
from agoradatatools.logs import log_time

logger = logging.getLogger(__name__)


@log_time(func_name="process_dataset", logger=logger)
def process_dataset(
    dataset_obj: dict, staging_path: str, syn: synapseclient.Synapse
) -> tuple:
    """Takes in a dataset from the configuration file and passes it through the ETL process

    Args:
        dataset_obj (dict): A dataset defined in the configuration file
        staging_path (str): Staging path
        syn (synapseclient.Synapse): synapseclient.Synapse session.

    Returns:
        syn_obj (tuple): Tuple containing the id and version number of the uploaded file.
    """

    dataset_name = list(dataset_obj.keys())[0]
    entities_as_df = {}

    for entity in dataset_obj[dataset_name]["files"]:
        entity_id = entity["id"]
        entity_format = entity["format"]
        entity_name = entity["name"]

        df = extract.get_entity_as_df(syn_id=entity_id, source=entity_format, syn=syn)
        df = transform.standardize_column_names(df=df)
        df = transform.standardize_values(df=df)

        # the column rename gets applied to all entities in a dataset
        if "column_rename" in dataset_obj[dataset_name].keys():
            df = transform.rename_columns(
                df=df, column_map=dataset_obj[dataset_name]["column_rename"]
            )

        entities_as_df[entity_name] = df

    if "custom_transformations" in dataset_obj[dataset_name].keys():
        df = transform.apply_custom_transformations(
            datasets=entities_as_df,
            dataset_name=dataset_name,
            dataset_obj=dataset_obj[dataset_name],
        )
    else:
        df = entities_as_df[list(entities_as_df)[0]]

    if "agora_rename" in dataset_obj[dataset_name].keys():
        df = transform.rename_columns(
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

    syn_obj = load.load(
        file_path=json_path,
        provenance=dataset_obj[dataset_name]["provenance"],
        destination=dataset_obj[dataset_name]["destination"],
        syn=syn,
    )

    return syn_obj


def create_data_manifest(parent=None, syn=None) -> DataFrame:
    """Creates data manifest (dataframe) that has the IDs and version numbers of child synapse folders

    Args:
        parent (synapseclient.Folder/str, optional): synapse folder or synapse id pointing to parent synapse folder. Defaults to None.
        syn (synapseclient.Synapse, optional): Synapse client session. Defaults to None.

    Returns:
        DataFrame: Dataframe containing IDs and version numbers of folders within the parent directory
    """

    if not parent:
        return None

    if not syn:
        syn = utils._login_to_synapse()

    folders = syn.getChildren(parent)
    folder = [folders]
    folder = [
        {"id": folder["id"], "version": folder["versionNumber"]} for folder in folders
    ]

    return DataFrame(folder)


@log_time(func_name="process_all_files", logger=logger)
def process_all_files(config_path: str = None, syn=None):
    """This function will read through the entire configuration and process each file listed.

    Args:
        config_path (str, optional): path to configuration file. Defaults to None.
        syn (synapseclient.Session, optional): Synapse client session. Defaults to None.
    """

    # if not syn:
    #     syn = utils._login_to_synapse()

    if config_path:
        config = utils._get_config(config_path=config_path)
    else:
        config = utils._get_config()

    datasets = utils._find_config_by_name(config, "datasets")

    # create staging location
    staging_path = utils._find_config_by_name(config, "staging_path")
    if staging_path is None:
        staging_path = "./staging"

    load.create_temp_location(staging_path)

    error_list = []
    if datasets:
        for dataset in datasets:
            try:
                process_dataset(dataset_obj=dataset, staging_path=staging_path, syn=syn)
            except Exception as e:
                error_list.append(
                    f"{list(dataset.keys())[0]}: " + str(e).replace("\n", "")
                )

    destination = utils._find_config_by_name(config, "destination")

    if not error_list:
        # create manifest if there are no errors
        manifest_df = create_data_manifest(parent=destination, syn=syn)
        manifest_path = load.df_to_csv(
            df=manifest_df, staging_path=staging_path, filename="data_manifest.csv"
        )

        load.load(
            file_path=manifest_path,
            provenance=manifest_df["id"].to_list(),
            destination=destination,
            syn=syn,
        )
    else:
        raise ADTDataProcessingError(
            "\nData Processing has failed for one or more data sources. Refer to the list of errors below to address issues:\n"
            + "\n".join(error_list)
        )


app = Typer()


input_path_arg = Argument(..., help="Path to configuration file for processing run")
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
    auth_token: str = synapse_auth_opt,
):
    syn = utils._login_to_synapse(token=auth_token)
    process_all_files(config_path=config_path, syn=syn)


if __name__ == "__main__":
    app()
