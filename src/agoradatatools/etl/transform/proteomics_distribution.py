import pandas as pd
import numpy as np
from agoradatatools.etl import utils


def transform_proteomics_distribution_data(datasets: dict) -> pd.DataFrame:
    """Takes dictionary of dataset DataFrames and calculates the distribution
    of the "log2_fc" column by tissue for each dataset. Data sets must be named
    'proteomics' (for LFQ data) and 'proteomics_tmt' (for TMT data).

    Args:
        datasets (dict[str, pd.DataFrame]): dictionary of dataset names mapped to their DataFrame

    Returns:
        pd.DataFrame: a Dataframe that is a concatenation of LFQ and TMT distribution data,
                      containing columns "tissue", "min", "max", "first_quartile",
                      "median", "third_quartile", and "type", where "type" is LFQ or TMT.
    """
    transformed = []
    for name, dataset in datasets.items():
        if name == "proteomics":
            df = utils.calculate_distribution(dataset, "tissue", "log2_fc")
            df["type"] = "LFQ"
            transformed.append(df)
        elif name == "proteomics_tmt":
            df = utils.calculate_distribution(dataset, "tissue", "log2_fc")
            df["type"] = "TMT"
            transformed.append(df)

    return pd.concat(transformed)
