# agora-data-tools

- [Intro](#intro)
- [Running the pipeline](#running-the-pipeline)
  - [Seqera Platform](#seqera-platform)
  - [Configuring Synapse Credentials](#configuring-synapse-credentials)
  - [Locally](#locally)
  - [Docker](#docker)
- [Testing Github Workflow](#testing-github-workflow)
- [Unit Tests](#unit-tests)
- [Pipeline Configuration](#pipeline-configuration)

## Intro
A place for Agora's ETL, data testing, and data analysis

This configuration-driven data pipeline uses a config file - which is easy for
engineers, analysts, and project managers to understand - to drive the entire ETL process.  The code in `src/agoradatatools` uses
parameters defined in a config file to determine which kinds of extraction and transformations a particular
dataset needs to go through before the resulting data is serialized as json files that can be loaded into Agora's data repository.

In the spirit of importing datasets with the minimum amount of transformations, one can simply add a dataset to the config file,
and run the tool.

This `src/agoradatatools` implementation was influenced by the "Modern Config Driven ELT Framework for Building a
Data Lake" talk given at the Data + AI Summit of 2021.

Python notebooks that describe the custom logic for various datasets are located in `/data_analysis/notebooks`.

## Running the pipeline
The json files generated by `src/agoradatatools` are written to folders in the [Agora Synapse project](https://www.synapse.org/#!Synapse:syn11850457/files/) by default,
although you can modify the destination Synapse folder in the [config file](#config).

Note that running the pipeline does _not_ automatically update the Agora database in any environment.  Ingestion of generated json files
into the Agora databases is handled by [agora-data-manager](https://github.com/Sage-Bionetworks/agora-data-manager/).

You can run the pipeline in any of the following ways:
1. **Seqera Platform**: is the simplest, but least flexible, way to run the pipeline; it does not require Synapse permissions, creating a Synapse PAT, or setting up the Synapse Python client.
2. **Locally**: requires installing Python and Pipenv, obtaining the required Synapse permissions, creating a Synpase PAT, and setting up the Synapse Python client.
3. **Docker**: requires installing Docker, obtaining the required Synapse permissions, and creating a Synpase PAT.

When running the pipeline, you must specify the config file that will be used. There are two config files that are checked into this repo:
* ```test_config.yaml``` places the transformed datasets in the [Agora Testing Data](https://www.synapse.org/#!Synapse:syn17015333) folder in synapse; write files to this folder to perform data validation.
* ```config.yaml``` places the transformed datasets the [Agora Live Data](https://www.synapse.org/#!Synapse:syn12177492) synapse folder; write files to this folder once you've validated that the ETL process is generating files suitable for release.
Note that files in the Agora Live Data folder are not automatically released, so if 'bad' file versions do get written to this folder it's not the end of the world. A releasable manifest file can be generated by a subsequent ETL processing run into the folder, or manually if necessary.

You may also create a custom config file to use locally to target specific dataset(s) or transforms of interest, and/or to write the generated json files to a different Synapse
location. See the [config file](#config) section for additional information.

### Seqera Platform
This pipeline can be executed without any local installation, permissions, or credentials; the Sage Bionetworks Seqera Platform workspace is configured to use Agora's Synapse credentials, which can be found in LastPass in the "Shared-Agora" Folder.

The instructions to trigger the workflow can be found at [Sage-Bionetworks-Workflows/nf-agora](https://github.com/Sage-Bionetworks-Workflows/nf-agora)

### Configuring Synapse Credentials

1. Obtain download access to all required source files in Synapse, including accepting the terms of use on the AD Knowledge Portal backend [here](https://www.synapse.org/#!Synapse:syn5550378).  If you see a green unlocked lock icon, then you should be good to go.
2. Obtain write access to the destination Synapse project, e.g. [Agora Synapse project](https://www.synapse.org/#!Synapse:syn11850457/files/)
3. Create a Synapse personal access token (PAT)
4. [Set up](https://help.synapse.org/docs/Client-Configuration.1985446156.html) your Synapse Python client locally

Your configured Synapse credentials can be used to run this package both locally and using Docker, as outlined below.

### Locally
Perform the following one-time steps to set up your local environment and obtain the required Synapse permissions:

1. This package uses Python, if you have not already, please install [pyenv](https://github.com/pyenv/pyenv#installation) to manage your Python versions. Versions supported by this package are all versions >=3.7 and <3.11. If you do not install `pyenv` make sure that Python and `pip` are installed correctly and have been added to your PATH by running `python3 --version` and `pip3 --version`. If your installation was successful, your terminal will return the versions of Python and `pip` that you installed.  **Note**: If you have `pyenv` it will install a specific version of Python for you.

2. Install `pipenv` by running `pip install pipenv`.

3. Install `git` if you have not done so already using [these instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

4. Clone this Github Repository to your local machine by opening your terminal, navigating to the directory that you want this repository to be cloned and running `git clone https://github.com/Sage-Bionetworks/agora-data-tools.git`. After cloning is complete, navigate into the newly created `agora-data-tools` directory.

5. Install `agoradatatools` locally using pipenv:

    * pipenv
      ```bash
      pipenv install
      # To develop locally you want to add --dev
      # pipenv install --dev
      pipenv shell
      ```

6. You can check if the package was installed correctly by running `adt --help` in the terminal. If it returns instructions about how to use the CLI, installation was successful and you can run the pipeline by providing the desired [config file](#config) as an argument. Be sure to review these instructions prior to executing a processing run. The following example command will execute the pipeline using ```test_config.yaml``` and the default options:

    ```bash
    adt test_config.yaml
    ```

### Docker

There is a publicly available [GHCR repository]([https://hub.docker.com/r/sagebionetworks/agora-data-tools](https://github.com/Sage-Bionetworks/agora-data-tools/pkgs/container/agora-data-tools)) automatically built via GitHub Actions. That said, you may want to develop using Docker locally on a feature branch.

If you don't want to deal with Python paths and dependencies, you can use Docker to run the pipeline. Perform the following one-time step to set up your Docker environment and obtain the required Synapse permissions:
1. Install [Docker](https://docs.docker.com/get-docker/).

Once you have completed the one-time setup step outlined above, execute the pipeline by running the following command and providing your PAT and the desired [config file](#config) as an argument. The following example command will execute the pipeline in Docker using ```test_config.yaml```:

```
# This creates a local Docker image
docker build -t agora-data-tools .
docker run -e SYNAPSE_AUTH_TOKEN=<your PAT> agora-data-tools adt test_config.yaml
```

## Testing Github Workflow
In order to test the GitHub Actions workflow locally:
- install [act](https://github.com/nektos/act) and [Docker](https://github.com/docker/docker-install)
- create a .secrets file in the root directory of the folder with a SYNAPSE_USER and a SYNAPSE_PASS value*

Then run:
```bash
act -v --secret-file .secrets
```

The repository is currently using Agora's credentials for Synapse.  Those can be found in LastPass in the "Shared-Agora" Folder.

## Unit Tests
Unit tests can be run by calling pytest from the command line.
```bash
python -m pytest
```

## Pipeline Configuration
Parameters:
- `destination`: Defines the default target location (folder) that the generated json files are written to; this value can be overridden on a per-dataset basis
- `staging_path`: Defines the location of the staging folder that the generated json files are written to
- `gx_folder`: Defines the Synapse ID of the folder that generated GX reports are written to. This key must always be present in the config file. A valid Synapse ID assigned to `gx_folder` is required if `gx_enabled` is set to `true` for any dataset. If this key is missing from the dataset, or if it is set to `none` when `gx_enabled` is `true` for any dataset, an error will be thrown.
- `gx_table`: Defines the Synapse ID of the table that generated GX reporting is posted to. This key must always be present in the config file. A valid Synapse ID assigned to `gx_table` is required if `gx_enabled` is set to `true` for any dataset. If this key is missing from the dataset, or if it is set to `none` when `gx_enabled` is `true` for any dataset, an error will be thrown.
- `sources/<source>`: Source files for each dataset are defined in the `sources` section of the config file.
- `sources/<source>/<source>_files`: A list of source file information for the dataset.
- `sources/<source>/<source>_files/name`: The name of the source file/dataset.
- `sources/<source>/<source>_files/id`: The Synapse ID of the source file. Dot notation is supported to indicate the version of the file to use.
- `sources/<source>/<source>_files/format`: The format of the source file.
- `datasets/<dataset>`: Each generated json file is named `<dataset>.json`
- `datasets/<dataset>/files`: A list of source files for the dataset
    - `name`: The name of the source file (this name is the reference the code will use to retrieve a file from the configuration)
    - `id`: Synapse id of the file
    - `format`: The format of the source file
- `datasets/<dataset>/final_format`: The format of the generated output file.
- `datasets/<dataset>/gx_enabled`: Whether or not GX validation should be run on the dataset. `true` will run GX validation, `false` or the absence of this key will skip GX validation.
- `datasets/<dataset>/gx_nested_columns`: A list of nested columns that should be validated using GX nested validation. Failure to include this key and a valid list of columns will result in an error because the nested fields will not be converted to a JSON-parseable string prior to validation. This key is not needed if `gx_enabled` is not set to `true` or if the dataset does not have nested fields.
- `datasets/<dataset>/provenance`: The Synapse id of each entity that the dataset is derived from, used to populate the generated file's Synapse provenance. (The Synapse API calls this "Activity")
- `datasets/<dataset>/destination`: Override the default destination for a specific dataset by specifying a synID, or use `*dest` to use the default destination
- `datasets/<dataset>/column_rename`: Columns to be renamed prior to data transformation
- `datasets/<dataset>/agora_rename`: Columns to be renamed after data transformation, but prior to json serialization
- `datasets/<dataset>/custom_transformations`: The list of additional transformations to apply to the dataset; a value of 1 indicates the default transformation
