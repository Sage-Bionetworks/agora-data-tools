import pandas as pd

from agoradatatools.etl import utils, transform


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
        # Remove contaminant ("CON__") entries and rows with NA uniqids before calculating distribution
        dataset = transform.transform_proteomics(df=dataset)

        df = utils.calculate_distribution(
            df=dataset, grouping="tissue", distribution_column="log2_fc"
        )

        if name == "proteomics":
            df["type"] = "LFQ"
        elif name == "proteomics_tmt":
            df["type"] = "TMT"
        elif name == "proteomics_srm":
            df["type"] = "SRM"
        else:
            raise ValueError(f"Proteomics data type '{name}' not supported.")

        transformed.append(df)

    return pd.concat(transformed)
