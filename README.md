# agora_data_tools
A place for Agora's ETL and Data Testing

In this configuration-driven data pipeline, the idea is to use a configuration file - that is easy for 
engineers, analysts, and project managers to understand- to drive the entire ETL process.  The code in /agoradatatools uses 
parameters defined in the configuration file to determine which kinds of extraction and transformations a particular 
dataset needs to go through before being loaded into the data repository for Agora.  In the spirit of importing datasets
with the minimum amount of transformations, one can simply add a dataset to the config file, and run the scripts. 

*this refactoring of the /agoradatatools was influenced by the "Modern Config Driven ELT Framework for Building a 
Data Lake" talk given at the Data + AI Summit of 2021.

## Run
```bash
python ./agoradatatools/process.py config.yaml
# The config file can be swapped out
```

## Test
In order to run tests locally
```bash
python -m pytest
```

## Config
Parameters:
- destination: defines a default place for datasets; can be overriden individually
- files: each individial dataset
- id: synapse id of the dataset
- format: format of the dataset at the source
- final_filename: filename at the end of the pipeline.  Needs to include extension
- provenance: synapse entities the file comes from. *The Synapse API calls this "Activity"
- files.destination(optional): overrides the default destination
- column_rename: columns to be renamed
- additional_transformations: lists additional transformations for the file to undergo 