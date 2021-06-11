import etl.extract as extract
import etl.transform as transform
import etl.load as load
import etl.utils as utils
import sys
from pandas import DataFrame

def process_single_file(file_obj: dict, syn=None):
    """
    Puts a single file through the entire ETL process.
    If process is successful, it will return a string
    with the Synapse id of the newly loaded file
    :param file_obj: a file object from the configuration
    :return: a Synapse id
    """

    # individual exceptions are defined in each file
    try:
        df = extract.get_entity_as_df(syn_id=file_obj['id'], format=file_obj['format'], syn=syn)
    except Exception as extract_error:
        print("There was an error extracting " + file_obj['id'])
        print(extract_error)
        return

    try:
        df = transform.standardize_column_names(df=df)
        df = transform.standardize_values(df=df)
    except Exception as transform_error:
        print("There was an error transforming " + file_obj['id'])
        print(transform_error)
        return

    try:
        json_path = load.df_to_json(df=df, filename=file_obj['final_filename'])
        syn_obj = load.load(file_path=json_path, provenance=file_obj['provenance'], destination=file_obj['destination'], syn=syn)
    except Exception as load_error:
        print("There was an error loading " + file_obj['id'])
        print(load_error)
        return

    return syn_obj


def create_data_manifest(manifest: list[tuple]) -> DataFrame:
    return DataFrame(manifest, columns=['id', 'version'])


def process_all_files(config_path: str = None):

    syn = utils._login_to_synapse()
    manifest = []

    if config_path:
        config = utils._get_config(config_path=config_path)
    else:
        config = utils._get_config()

    files = config[1]['files']

    # create staging location
    load.create_temp_location()

    for file in files:
        new_syn_tuple = process_single_file(file_obj=file)
        manifest.append(new_syn_tuple)

    # create manifest
    manifest_df = create_data_manifest(manifest=manifest)
    manifest_path = load.df_to_csv(df=manifest_df, filename="data_manifest")

    load.load(file_path=manifest_path, provenance=manifest_df['id'].to_list(), destination=config[0]['destination'])

def main():
    process_all_files(config_path=sys.argv[1])

if __name__ == "__main__":
    main()