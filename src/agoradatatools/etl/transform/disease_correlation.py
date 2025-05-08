"""
This module contains the transformation logic for the disease correlation dataset.
This is for the Model AD project.
"""

import pandas as pd
from typing import Dict, List

from agoradatatools.etl.utils import check_required_datasets, check_required_columns

REQUIRED_COLUMNS = {}


def transform_disease_correlation(
    datasets: Dict[str, pd.DataFrame],
    required_datasets: List[str] = ["nanostring_legacy_results", "model_info", "model_allele_info"],
    required_columns: Dict[str, List[str]] = REQUIRED_COLUMNS
) -> pd.DataFrame:
    """
    
    """
    check_required_datasets(datasets, required_datasets)
    check_required_columns(datasets, required_columns)

    # Load and prepare datasets
    nanostring_legacy_results_df = datasets["nanostring_legacy_results"].fillna("")
    model_info_df = datasets["model_info"].fillna("")
    model_allele_info_df = datasets["model_allele_info"].fillna("")

    