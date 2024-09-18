"""
This module contains the transformation logic for the biomarkers dataset.
This is for the Model AD project.
"""
import pandas as pd


def transform_biomarkers(datasets: dict) -> list:
    """
    Takes dictionary of dataset DataFrames, extracts the biomarkers
    DataFrame, and transforms it into a list of dictionaries grouped by
    'model', 'type', 'ageDeath', 'tissue', and 'units'.

    Args:
        datasets (dict[str, pd.DataFrame]): dictionary of dataset names mapped to their DataFrame

    Returns:
        dict: a dictionary of biomarkers data modeled after intended final JSON structure
    """
    biomarkers_dataset = datasets["biomarkers"]

    # Check that the dataset looks like what we expect
    if not isinstance(biomarkers_dataset, pd.DataFrame):
        raise ValueError("Biomarker dataset is not a pandas DataFrame")
    if not biomarkers_dataset.columns.isin(['model', 'type', 'ageDeath', 'tissue', 'units', 'genotype', 'measurement', 'sex']).all():
      raise ValueError("Biomarker dataset does not contain expected columns")
    
    data_as_list = []
    grouped = biomarkers_dataset.groupby(['model', 'type', 'ageDeath', 'tissue', 'units'])

    for (model, type_, ageDeath, tissue, units), group in grouped:
      # Create the base structure for each group
      entry = {
          "model": model,
          "type": type_,
          "ageDeath": ageDeath,
          "tissue": tissue,
          "units": units,
          "points": []
      }

      # Append the measurement, genotype, and sex for each row
      for _, row in group.iterrows():
          point = {
              "genotype": row['genotype'],
              "measurement": row['measurement'],
              "sex": row['sex']
          }
          entry['points'].append(point)

      # Add the entry to the list
      data_as_list.append(entry)

    return data_as_list
