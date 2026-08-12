import pandas as pd

from agoradatatools.etl import transform
from agoradatatools.etl.utils import general_utils as gu


# Unlike most transforms that use a REQUIRED_INPUT dict keyed by a fixed set of dataset
# names, this transform accepts a variable number of proteomics datasets (e.g. LFQ, TMT,
# SRM) and applies the same column requirements to each one dynamically in a loop. A flat
# list is therefore used and the dataset name key is constructed at runtime.
DATASET_REQUIRED_COLUMNS = ["uniqid", "log2_fc", "tissue"]


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
        gu.check_required_datasets_and_columns(
            {name: dataset}, {name: DATASET_REQUIRED_COLUMNS}
        )
        # Remove contaminant ("CON__") entries and rows with NA uniqids before calculating distribution
        dataset = transform.transform_proteomics(df=dataset)

        df = gu.calculate_distribution(
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
