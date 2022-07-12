# agora_data_tools
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
still need to be picked up by [agora-data-manager](https://github.com/Sage-Bionetworks/agora-data-manager/).

In order to run the pipeline, run process.py providing the configuration file as an argument.  Install the package locally with 
```bash
pip install .
```
then:
```bash
python ./agoradatatools/process.py test_config.yaml
```

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
