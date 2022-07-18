# agora-data-tools

- [agora-data-tools](#agora-data-tools)
  - [Intro](#intro)
  - [Running the pipeline locally](#running-the-pipeline-locally)
  - [Testing Github Workflow](#testing-github-workflow)
  - [Unit Tests](#unit-tests)
  - [Config](#config)

## Intro
A place for Agora's ETL, data testing, and data analysis

In this configuration-driven data pipeline, the idea is to use a configuration file - that is easy for 
engineers, analysts, and project managers to understand- to drive the entire ETL process.  The code in /agoradatatools uses 
parameters defined in the configuration file to determine which kinds of extraction and transformations a particular 
dataset needs to go through before being loaded into the data repository for Agora.  In the spirit of importing datasets
with the minimum amount of transformations, one can simply add a dataset to the config file, and run the scripts. 

*this refactoring of the /agoradatatools was influenced by the "Modern Config Driven ELT Framework for Building a 
Data Lake" talk given at the Data + AI Summit of 2021.



## Running the pipeline locally
There are two configuration files:  ```test_config``` places the transformed datasets into Agora's testing data site, 
```config.yaml``` places them in the live data site.  Running the pipeline does not mean Agora will be updated.  The files 
still need to be picked up by [agora-data-manager](https://github.com/Sage-Bionetworks/agora-data-manager/). Here are the [files](https://www.synapse.org/#!Synapse:syn11850457/files/) for Agora on Synapse.

1. Install the package locally with:

    ```bash
    pip install .
    pip install -r requirements.txt
    ```

1. This is where the [testing files](https://www.synapse.org/#!Synapse:syn17015333) live on Synapse.  For testing purposes, you will need to obtain write permissions to the project and create a test folder within the "Agora Testing Data".  After doing so, you will replace `- destination: &dest syn17015333` with the Synapse id of the new folder.

1. Prior to executing the code, you will want to accept the terms of use on the AD Knowledge Portal backend [here](https://www.synapse.org/#!Synapse:syn5550378).  If you see a green unlocked lock icon, then you should be good to go.

1. In order to run the pipeline, run process.py providing the configuration file as an argument.

    ```bash
    python ./agoradatatools/process.py test_config.yaml
    ```

## Testing Github Workflow
In order to test the workflow locally:
- install [act](https://github.com/nektos/act) and (docker)[https://github.com/docker/docker-install]
- create a .secrets file in the root directory of the folder with a SYNAPSE_USER and a SYNAPSE_PASS value*

Then run:
```bash
act -v --secret-file .secrets
```

*the repository is currently using Agora's credentials for Synapse.  Those can be found in LastPass.

## Unit Tests
Unit tests can be run by calling pytest from the command line.
```bash
python -m pytest
```

## Config
Parameters:
- destination: defines a default place for datasets; can be overriden individually
- files: source files for each dataset
    - name: name of the file (this name is the reference the code will use to retrieve a file from the confituration)
    - id: synapse id of the file
    - format: the format of the file at the source
- provenance: synapse entities the dataset comes from. *The Synapse API calls this "Activity"
- destination(optional): overrides the default destination
- column_rename: columns to be renamed
- agora_rename: while the front end doesn't refactor its hardcoded names, we cannot standardize the name of the features.
  These are the old names.
- additional_transformations: lists additional transformations for the file to undergo 