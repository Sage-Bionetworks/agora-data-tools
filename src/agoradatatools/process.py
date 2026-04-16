import logging
import warnings
from collections import defaultdict
from typing import Optional, Union, Dict, Any, List
import inspect
import synapseclient
from pandas import DataFrame
from typer import Argument, Option, Typer

from agoradatatools.errors import ADTDataProcessingError, ADTDataValidationError
from agoradatatools.etl import extract, load, transform, utils
from agoradatatools.gx import GreatExpectationsRunner
from agoradatatools.logs import log_time
from agoradatatools.reporter import ADTGXReporter, DatasetReport
from agoradatatools.constants import Platform


logger = logging.getLogger(__name__)


def check_provenance_id_file_id_consistency(
    provenance_ids: list[str], file_ids: list[str]
) -> None:
    """Check that provenance IDs in config are consistent with file IDs.
    If file id and provenance id share the same base id, their versions must match.

    Args:
        provenance_ids: List of provenance IDs defined in the configuration (after flattening)
        file_ids: List of file IDs defined in the configuration

    Raises:
        ValueError: If any provenance ID has different version than the corresponding file ID

    Example:
        file_ids = ["syn123.4", "syn456.2"]
        provenance_ids = ["syn123.4", "syn789.1"]  # OK - syn123 versions match
        provenance_ids = ["syn123.5", "syn789.1"]  # ERROR - syn123 versions differ
    """
    if not provenance_ids:
        return
    # Build a mapping of base ID to full ID(s) for file IDs
    file_id_map = defaultdict(set)
    for file_id in file_ids:
        base_id = file_id.split(".")[0]
        file_id_map[base_id].add(file_id)

    # Raise error if any provenance ID has different version than the corresponding file ID
    for prov_id in provenance_ids:
        base_id = prov_id.split(".")[0]
        if base_id in file_id_map:
            if prov_id not in file_id_map[base_id]:
                file_versions_str = ", ".join(sorted(file_id_map[base_id]))
                raise ValueError(
                    f"Version mismatch: Provenance ID '{prov_id}' conflicts with "
                    f"file ID(s) '{file_versions_str}'. When the same Synapse entity "
                    f"appears in both provenance and files, their versions must match."
                )


def get_provenance_ids(
    dataset_obj: Dict[str, Any], dataset_name: str, file_ids: list[str]
) -> list[str]:
    """Get combined provenance IDs from config and file IDs.

    Args:
        dataset_obj: Dataset configuration object
        dataset_name: Name of the dataset
        file_ids: List of file IDs defined in the configuration

    Returns:
        Combined list of provenance IDs (file IDs + config provenance)
    """
    provenance = dataset_obj[dataset_name].get("provenance", [])
    flattened_provenance = set()

    if provenance:
        if not isinstance(provenance, list):
            raise ValueError(f"Provenance for dataset '{dataset_name}' must be a list")
        for item in provenance:
            if isinstance(item, list):
                flattened_provenance.update(item)
            elif isinstance(item, str):
                flattened_provenance.add(item)
        provenance_ids = set(file_ids).union(flattened_provenance)
        check_provenance_id_file_id_consistency(
            provenance_ids=list(flattened_provenance), file_ids=file_ids
        )
    else:
        provenance_ids = file_ids

    return list(provenance_ids)


def apply_custom_transformations(
    datasets: Dict[str, Any],
    dataset_name: str,
    dataset_obj: Dict[str, Any],
) -> Union[DataFrame, Dict[str, Any], List[Dict[str, Any]], None]:
    """Apply custom transformations to the dataset based on the provided function names and parameters.

    Args:
        datasets (dict): datasets to be transformed
        dataset_name (str): name of the datasets
        dataset_obj (dict): dataset object from the configuration file

    Returns:
        Union[DataFrame, dict, None]: result of transformation.
    """
    function_info = dataset_obj.get("custom_transformations", "")
    if (
        not isinstance(datasets, dict)
        or not isinstance(dataset_name, str)
        or not function_info
    ):
        if not function_info:
            warnings.warn(
                f"No custom transformation function provided for dataset {dataset_name}. Skipping."
            )
        return None
    if isinstance(function_info, str):
        function_name = function_info
        config_defined_params = {}
    else:
        # Retrieve the function name and its parameters
        # Assumes a single function in the dictionary
        if not isinstance(function_info, dict):
            raise TypeError(
                f"Custom transformation in the config for dataset '{dataset_name}' should be mapped to a function name "
                f"with custom parameters if needed. Received: {type(function_info).__name__}."
            )
        if len(function_info.items()) != 1:
            warnings.warn(
                "Please provide a single custom transformation function in the configuration file. Only the first function will be used if multiple are provided."
            )
        function_name, config_defined_params = next(iter(function_info.items()))
    if not hasattr(transform, function_name):
        raise AttributeError(
            f"Function {function_name} not found in the transform module. Please provide the correct function name."
        )

    retrieved_function = getattr(transform, function_name)
    function_params = inspect.signature(retrieved_function).parameters

    standard_params = {
        "df": datasets.get(dataset_name, DataFrame()),
        "datasets": datasets,
        "dataset_name": dataset_name,
    }
    new_standard_params = {
        k: v for k, v in standard_params.items() if k in function_params
    }

    # Holds all the parameters to be passed to the transformation function
    combined_params = {**new_standard_params, **config_defined_params}
    return retrieved_function(**combined_params)


def upload_dataversion_metadata(
    syn: synapseclient.Synapse,
    file_id: str,
    file_version: str,
    staging_path: str,
    destination: str,
    team_images_id: Optional[str] = None,
) -> None:
    """Uploads dataversion.json file to Synapse with metadata about the manifest file.
    Model-AD runs do not have a team_images_id, which will be left out of the dataversion.json file.

    Args:
        syn (synapseclient.Synapse): Synapse client session
        file_id (str): Synapse ID of the manifest file
        file_version (str): Version number of the manifest file
        staging_path (str): Path to the staging directory
        destination (str): Synapse ID of the destination folder
        team_images_id (str, optional): Synapse ID of the team_images folder if provided. Defaults to None.
    """
    dataversion_dict = {
        "data_file": file_id,
        "data_version": file_version,
    }
    if team_images_id:
        dataversion_dict["team_images_id"] = team_images_id

    dataversion_json_path = load.dict_to_json(
        data_as_dict=dataversion_dict,
        staging_path=staging_path,
        filename="dataversion.json",
    )
    load.load(
        file_path=dataversion_json_path,
        provenance=[file_id],
        destination=destination,
        syn=syn,
    )


@log_time(func_name="process_dataset", logger=logger)
def process_dataset(
    dataset_obj: dict,
    staging_path: str,
    gx_folder: str,
    syn: synapseclient.Synapse,
    upload: bool = True,
) -> Union[DatasetReport, None]:
    """Takes in a dataset from the configuration file and passes it through the ETL process

    Args:
        dataset_obj (dict): A dataset defined in the configuration file
        staging_path (str): Staging path
        gx_folder (str): Synapse ID of the folder where Great Expectations reports should be uploaded
        syn (synapseclient.Synapse): synapseclient.Synapse session.
        upload (bool, optional): Whether or not to upload the data to Synapse. Defaults to True.

    Returns:
        None if GX is not enabled. Otherwise, a DatasetReport object.
    """
    dataset_name = list(dataset_obj.keys())[0]
    dataset_report = DatasetReport(data_set=dataset_name)
    file_ids = []

    entities_as_df = {}
    for entity in dataset_obj[dataset_name]["files"]:
        entity_id = entity["id"]
        entity_format = entity["format"]
        entity_name = entity["name"]
        file_ids.append(entity_id)

        df = extract.get_entity_as_df(syn_id=entity_id, source=entity_format, syn=syn)
        df = utils.standardize_column_names(df=df)
        df = utils.standardize_values(df=df)

        if "column_rename" in dataset_obj[dataset_name].keys():
            df = utils.rename_columns(
                data=df, column_map=dataset_obj[dataset_name]["column_rename"]
            )

        entities_as_df[entity_name] = df

    # get provenance if any
    provenance_ids = get_provenance_ids(dataset_obj, dataset_name, file_ids)

    if "custom_transformations" in dataset_obj[dataset_name].keys():
        transform_result = apply_custom_transformations(
            datasets=entities_as_df,
            dataset_name=dataset_name,
            dataset_obj=dataset_obj[dataset_name],
        )

    else:
        transform_result = entities_as_df[list(entities_as_df)[0]]

    if "agora_rename" in dataset_obj[dataset_name].keys():
        transform_result = utils.rename_columns(
            data=transform_result, column_map=dataset_obj[dataset_name]["agora_rename"]
        )

    if isinstance(transform_result, dict):
        json_path = load.dict_to_json(
            data_as_dict=transform_result,
            staging_path=staging_path,
            filename=dataset_name + "." + dataset_obj[dataset_name]["final_format"],
        )
    elif isinstance(transform_result, list):
        json_path = load.list_to_json(
            data_as_list=transform_result,
            staging_path=staging_path,
            filename=dataset_name + "." + dataset_obj[dataset_name]["final_format"],
        )
    else:
        json_path = load.df_to_json(
            data_as_df=transform_result,
            staging_path=staging_path,
            filename=dataset_name + "." + dataset_obj[dataset_name]["final_format"],
        )

    gx_enabled = dataset_obj[dataset_name].get("gx_enabled", False)

    if gx_enabled:
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
            dtype=dataset_obj[dataset_name].get("gx_dtype"),
        )
        gx_runner.run()

        dataset_report.set_attributes(
            gx_report_file=gx_runner.report_file,
            gx_report_version=gx_runner.report_version,
            gx_report_link=DatasetReport.format_link(
                syn_id=gx_runner.report_file, version=gx_runner.report_version
            ),
            gx_failures=gx_runner.failures,
            gx_failure_message=gx_runner.failure_message,
            gx_warnings=gx_runner.warnings,
            gx_warning_message=gx_runner.warning_message,
        )

        if upload and not gx_runner.failures:
            file_id, file_version = load.load(
                file_path=json_path,
                provenance=provenance_ids,
                destination=dataset_obj[dataset_name]["destination"],
                syn=syn,
            )

            dataset_report.set_attributes(
                adt_output_file=file_id,
                adt_output_version=file_version,
                adt_output_link=DatasetReport.format_link(
                    syn_id=file_id, version=file_version
                ),
            )
        return dataset_report

    else:
        if upload:
            file_id, file_version = load.load(
                file_path=json_path,
                provenance=provenance_ids,
                destination=dataset_obj[dataset_name]["destination"],
                syn=syn,
            )
        return None


def create_data_manifest(
    syn: synapseclient.Synapse, parent: Union[synapseclient.Folder, str] = None
) -> Union[DataFrame, None]:
    """Creates data manifest (dataframe) that has the IDs and version numbers of child synapse files

    Args:
        syn (synapseclient.Synapse): Synapse client session.
        parent (synapseclient.Folder/str, optional): synapse folder or synapse id pointing to parent synapse folder. Defaults to None.

    Returns:
        Dataframe containing IDs and version numbers of folders within the parent directory, or None if parent is None
    """

    if not parent:
        return None

    files = syn.getChildren(parent)

    manifest_rows = [
        {
            "id": file["id"],
            "version": (
                file["versionNumber"] + 1
                if file["name"] == "data_manifest.csv"
                or file["name"] == "dataversion.json"
                else file["versionNumber"]
            ),
        }
        for file in files
    ]

    return DataFrame(manifest_rows)


@log_time(func_name="process_all_files", logger=logger)
def process_all_files(
    syn: synapseclient.Synapse,
    config_path: str = None,
    platform: Platform = Platform.LOCAL,
    run_id: str = None,
    upload: bool = True,
):
    """This function will read through the entire configuration and process each file listed.

    Args:
        syn (synapseclient.Session): Synapse client session
        config_path (str, optional): path to configuration file. Defaults to None.
        platform (Platform, optional): Platform where the process is being run. One of LOCAL, GITHUB, NEXTFLOW. Defaults to LOCAL.
        run_id (str, optional): Unique identifier for the processing run. Defaults to None.
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
    gx_table = config["gx_table"]

    staging_path = config.get("staging_path", None)
    load.create_temp_location(staging_path=staging_path or "./staging")

    reporter = ADTGXReporter(
        syn=syn,
        platform=platform,
        run_id=run_id,
        table_id=gx_table,
    )

    error_list = []
    for dataset in datasets:
        try:
            dataset_report = process_dataset(
                dataset_obj=dataset,
                staging_path=staging_path,
                gx_folder=config["gx_folder"],
                syn=syn,
                upload=upload,
            )
            if dataset_report:
                reporter.add_report(dataset_report)
                if dataset_report.gx_failures:
                    raise ADTDataValidationError(dataset_report.gx_failure_message)
        except Exception as e:
            import traceback

            error_message = (
                f"{list(dataset.keys())[0]}:\n {str(e)}\n{traceback.format_exc()}"
            )
            error_list.append(error_message)

    if error_list:
        reporter.update_table()

        raise ADTDataProcessingError(
            "\nData Processing has failed for one or more data sources. Refer to the list of errors below to address issues:\n"
            + "\n".join(error_list)
        )

    manifest_df = create_data_manifest(syn=syn, parent=destination)
    manifest_path = load.df_to_csv(
        df=manifest_df, staging_path=staging_path, filename="data_manifest.csv"
    )

    if upload:
        file_id, file_version = load.load(
            file_path=manifest_path,
            provenance=manifest_df["id"].to_list(),
            destination=destination,
            syn=syn,
        )

        upload_dataversion_metadata(
            syn=syn,
            file_id=file_id,
            file_version=file_version,
            team_images_id=config.get("team_images_id", None),
            staging_path=staging_path,
            destination=destination,
        )

        reporter.data_manifest_file = file_id
        reporter.data_manifest_version = file_version
        reporter.data_manifest_link = DatasetReport.format_link(
            syn_id=file_id, version=file_version
        )

    reporter.update_table()


app = Typer(add_completion=False)

input_path_arg = Argument(
    ..., help="Path to configuration file for processing run (Required)."
)

platform_opt = Option(
    "LOCAL",
    "--platform",
    "-p",
    help="Platform that is running the process. Must be one of LOCAL, GITHUB, or NEXTFLOW (Optional, defaults to LOCAL).",
    show_default=True,
)
run_id_opt = Option(
    None,
    "--run_id",
    "-r",
    help="Run ID of the process. This is used to identify the run in the GX table. (Optional)",
    show_default=True,
)
upload_opt = Option(
    False,
    "--upload",
    "-u",
    help="Boolean value that toggles whether or not files or GX reports will be uploaded to Synapse. The absence of this option means "
    "`False` - that neither output data files nor GX reports will be uploaded to Synapse. Setting "
    "`--upload` in the command will cause both to be uploaded. (Optional, defaults to False)",
    show_default=True,
)
synapse_auth_opt = Option(
    None,
    "--token",
    "-t",
    help="Synapse authentication token. (Required, Defaults to environment variable SYNAPSE_AUTH_TOKEN via syn.login() functionality "
    "https://python-docs.synapse.org/reference/client/?h=syn.login#synapseclient.Synapse.login)",
    show_default=False,
)


@app.command()
def process(
    config_path: str = input_path_arg,
    platform: str = platform_opt,
    run_id: str = run_id_opt,
    upload: bool = upload_opt,
    auth_token: str = synapse_auth_opt,
) -> None:
    """Process the configuration file and execute the data processing pipeline based on options.

    Args:
        config_path (str): Path to the configuration file for the processing run.
        platform (str): Platform that is running the process. Must be one of LOCAL, GITHUB, or NEXTFLOW.
        run_id (str): Run ID of the process. Used to identify the run in the GX table.
        upload (bool): Boolean value to toggle whether files will be uploaded to Synapse.
        auth_token (str): Synapse authentication token. Defaults to environment variable SYNAPSE_AUTH_TOKEN.
    """
    syn = utils._login_to_synapse(token=auth_token)
    platform_enum = Platform(platform)
    process_all_files(
        syn=syn,
        config_path=config_path,
        platform=platform_enum,
        run_id=run_id,
        upload=upload,
    )


if __name__ == "__main__":
    app()
