import argparse

from pandas import DataFrame

import agoradatatools.etl.extract as extract
import agoradatatools.etl.transform as transform
import agoradatatools.etl.load as load
import agoradatatools.etl.test as test
import agoradatatools.etl.utils as utils


def process_dataset(dataset_obj: dict, results: dict, syn=None):
    """
    Takes in a dataset from the configuration file and passes it through the
    ETL process
    :param dataset_obj: a dataset defined in the configuration file
    :param syn: synapse object
    """

    dataset_name = list(dataset_obj.keys())[0]
    entities_as_df = {}

    for entity in dataset_obj[dataset_name]['files']:
        entity_id = entity['id']
        entity_format = entity['format']
        entity_name = entity['name']

        df = extract.get_entity_as_df(syn_id=entity_id,
                                      format=entity_format,
                                      syn=syn)
        df = transform.standardize_column_names(df=df)
        df = transform.standardize_values(df=df)

        # the column rename gets applied to all entities in a dataset
        if "column_rename" in dataset_obj[dataset_name].keys():
            df = transform.rename_columns(df=df,
                                          column_map=dataset_obj[dataset_name]['column_rename'])

        entities_as_df[entity_name] = df
    # print(dataset_name)
    if "custom_transformations" in dataset_obj[dataset_name].keys():
        df = transform.apply_custom_transformations(datasets=entities_as_df,
                                                    dataset_name=dataset_name,
                                                    dataset_obj=dataset_obj[dataset_name])
    else:
        df = entities_as_df[list(entities_as_df)[0]]

    if "agora_rename" in dataset_obj[dataset_name].keys():
        df = transform.rename_columns(df=df,
                                      column_map=dataset_obj[dataset_name]['agora_rename'])

    results[dataset_name] = test.describe_dataset(df)

    try:
        if type(df) == dict:
            json_path = load.dict_to_json(df=df,
                                          filename=dataset_name + "." + dataset_obj[dataset_name]['final_format'])
        else:
            json_path = load.df_to_json(df=df,
                                        filename=dataset_name + "." + dataset_obj[dataset_name]['final_format'])

        syn_obj = load.load(file_path=json_path,
                            provenance=dataset_obj[dataset_name]['provenance'],
                            destination=dataset_obj[dataset_name]['destination'],
                            syn=syn)
    except Exception as error:
        print(error)
        return

    return syn_obj


def create_data_manifest(parent=None, syn=None) -> DataFrame:

    if not parent:
        return None

    if not syn:
        syn = utils._login_to_synapse()

    folders = syn.getChildren(parent)
    folder = [folders]
    folder = [{'id': folder['id'], 'version': folder['versionNumber']} for folder in folders]

    return DataFrame(folder)


def process_all_files(config_path: str = None, syn=None):
    """
    This function will run read through the entire configuration
    and process each file.
    :param config_path: the path to the configuration file
    """

    # if not syn:
    #     syn = utils._login_to_synapse()

    if config_path:
        config = utils._get_config(config_path=config_path)
    else:
        config = utils._get_config()

    datasets = config[1]['datasets']

    # create staging location
    load.create_temp_location()

    results = {}

    if datasets:
        for dataset in datasets:
            new_syn_tuple = process_dataset(dataset_obj=dataset, results=results, syn=syn)
            # in the future we should log new_syn_tuples that are none

    # create manifest
    manifest_df = create_data_manifest(parent=config[0]['destination'], syn=syn)
    manifest_path = load.df_to_csv(df=manifest_df,
                                   filename="data_manifest.csv")

    load.load(file_path=manifest_path,
              provenance=manifest_df['id'].to_list(),
              destination=config[0]['destination'],
              syn=syn)

    # sage results
    results_path = load.dict_to_json(df=results, filename='results.json')
    load.load(file_path=results_path,
              provenance=[],
              destination=config[0]['destination'])


def build_parser():
    """Builds the argument parser and returns the result."""
    parser = argparse.ArgumentParser(description="Agora data processing")
    parser.add_argument(
        "configpath",
        help="Agora processing yaml configuration",
    )
    parser.add_argument(
        "-a",
        "--authtoken",
        help="Synapse PAT",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    syn = utils._login_to_synapse(authtoken=args.authtoken)
    process_all_files(config_path=args.configpath, syn=syn)


if __name__ == "__main__":
    main()
