"""Input Data Validation stage.

Runs in process_dataset after extraction and standardization and before the
dataset's column_rename and transform. Each input file that has a spec in the
validation_configs package is validated against it, so the contract for that
file is defined once rather than in every transform that reads it.

Checks that depend on more than one file, on dataset configuration, or on an
intermediate frame stay in the transform; this stage only validates one file at
a time.
"""

import importlib
import logging
import pkgutil

import pandas as pd

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl import validation_configs
from agoradatatools.etl.validation_utils import (
    InputSpec,
    check_column_rules,
    check_required_datasets_and_columns,
)

logger = logging.getLogger(__name__)


def load_input_specs() -> dict[str, InputSpec]:
    """Collect the SPEC from every module in validation_configs, keyed by module name.

    The module name is the input file's name in the pipeline config, so a spec
    cannot be registered under a name that differs from the file it validates.
    """
    specs = {}
    for module_info in pkgutil.iter_modules(validation_configs.__path__):
        module = importlib.import_module(
            f"{validation_configs.__name__}.{module_info.name}"
        )
        specs[module_info.name] = module.SPEC
    return specs


INPUT_SPECS: dict[str, InputSpec] = load_input_specs()


def _validate_one(name: str, df: pd.DataFrame, spec: InputSpec) -> list[str]:
    """Run one spec against one frame and return every violation message.

    Column rules and whole-frame checks assume the required columns exist, so a
    missing column is reported alone rather than followed by KeyErrors.
    """
    try:
        check_required_datasets_and_columns({name: df}, {name: spec.required_columns})
    except ValueError as error:
        return [str(error)]

    messages = []
    try:
        check_column_rules({name: df}, {name: spec.column_rules})
    except ValueError as error:
        messages.append(str(error))
    for check in spec.checks:
        try:
            check(df)
        except ValueError as error:
            messages.append(f"In dataset '{name}': {error}")
    return messages


def validate_inputs(datasets: dict[str, pd.DataFrame]) -> None:
    """Validate every input that has a spec in validation_configs.

    All violations across all inputs are collected before raising so one run
    reports every problem. Inputs with no spec are logged and passed through.

    Args:
        datasets: Input name -> DataFrame, as extracted and standardized, before
            column_rename.

    Raises:
        ADTDataValidationError: If any input violates its spec.
    """
    messages: list[str] = []
    for name, df in datasets.items():
        spec = INPUT_SPECS.get(name)
        if spec is None:
            logger.info(f"No input validation spec registered for {name}. Skipping.")
            continue
        messages.extend(_validate_one(name, df, spec))

    if messages:
        raise ADTDataValidationError(
            "Input data validation failed:\n" + "\n".join(messages)
        )
