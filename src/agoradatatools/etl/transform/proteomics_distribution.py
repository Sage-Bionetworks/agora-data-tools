import pandas as pd
import numpy as np


def transform_proteomics_distribution_data(
    proteomics_df: pd.DataFrame, datatype: str
) -> pd.DataFrame:
    """Transform proteomics data
    Args:
        proteomics_df (pd.DataFrame): Dataframe
        datatype (str): Data Type
    Returns:
        pd.DataFrame: Transformed data
    """
    proteomics_df = (
        proteomics_df.groupby(["tissue"])
        .agg("describe")["log2_fc"]
        .reset_index()[["tissue", "min", "max", "25%", "50%", "75%"]]
    )

    proteomics_df.rename(
        columns={"25%": "first_quartile", "50%": "median", "75%": "third_quartile"},
        inplace=True,
    )

    proteomics_df["IQR"] = (
        proteomics_df["third_quartile"] - proteomics_df["first_quartile"]
    )
    proteomics_df["min"] = proteomics_df["first_quartile"] - (
        1.5 * proteomics_df["IQR"]
    )
    proteomics_df["max"] = proteomics_df["third_quartile"] + (
        1.5 * proteomics_df["IQR"]
    )

    for col in ["min", "max", "median", "first_quartile", "third_quartile"]:
        proteomics_df[col] = np.around(proteomics_df[col], 4)

    proteomics_df.drop("IQR", axis=1, inplace=True)
    proteomics_df["type"] = datatype

    return proteomics_df


# should be own transformation combined with one above
def create_proteomics_distribution_data(datasets: dict) -> pd.DataFrame:
    transformed = []
    for name, dataset in datasets.items():
        if name == "proteomics":
            transformed.append(
                transform_proteomics_distribution_data(
                    proteomics_df=dataset, datatype="LFQ"
                )
            )
        elif name == "proteomics_tmt":
            transformed.append(
                transform_proteomics_distribution_data(
                    proteomics_df=dataset, datatype="TMT"
                )
            )

    return pd.concat(transformed)
