"""Input Data Validation stage.

Runs in process_dataset after extraction and standardization and before the
dataset's column_rename and transform. Each input file that has a spec in the
input_specs package is validated against it, so the contract for that
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
from agoradatatools.etl import input_specs
from agoradatatools.etl.validation_utils import (
    InputSpec,
    check_column_rules,
    check_required_columns,
)

logger = logging.getLogger(__name__)


def load_input_specs() -> dict[str, InputSpec]:
    """Collect the SPEC from every module in input_specs, keyed by module name.

    The module name is the file's name in the pipeline config, or the schema
    value when several files share one spec (e.g. rna_de_file).
    """
    specs = {}
    for module_info in pkgutil.iter_modules(input_specs.__path__):
        module = importlib.import_module(f"{input_specs.__name__}.{module_info.name}")
        specs[module_info.name] = module.SPEC
    return specs


INPUT_SPECS: dict[str, InputSpec] = load_input_specs()


def _validate_one(name: str, df: pd.DataFrame, spec: InputSpec) -> list[str]:
    """Run one spec against one frame and return every violation message.

    Missing columns trigger an immediate return of the raised error, without
    checking column rules. If there are no missing columns, the function goes
    on to check all column rules in the spec and collects all error messages
    for all rules into a list before returning. If there were no errors,
    returns an empty list.
    """
    try:
        check_required_columns(df, name, spec.required_columns)
    except ValueError as error:
        return [str(error)]

    messages = []
    try:
        check_column_rules({name: df}, {name: spec.column_rules})
    except ValueError as error:
        messages.append(str(error))
    for check_fn in spec.checks:
        try:
            check_fn(df)
        except ValueError as error:
            messages.append(f"In dataset '{name}': {error}")
    return messages


def validate_inputs(
    datasets: dict[str, pd.DataFrame],
    schemas: dict[str, str] | None = None,
    specs: dict[str, InputSpec] | None = None,
) -> None:
    """Validate every input that has a spec in input_specs.

    All violations across all inputs are collected before raising so one run
    reports every problem. Inputs with no spec are logged and passed through.

    Args:
        datasets: Input name -> DataFrame, as extracted and standardized, before
            column_rename.
        schemas: Optional input name -> spec key. When omitted, the input name
            is the spec key. process_dataset passes each file's schema field,
            defaulting to its name.
        specs: Optional spec key -> InputSpec registry. Defaults to INPUT_SPECS
            at call time.

    Raises:
        ADTDataValidationError: If any input violates its spec.
    """
    registry = INPUT_SPECS if specs is None else specs
    messages: list[str] = []
    for name, df in datasets.items():
        spec_key = (schemas or {}).get(name, name)
        spec = registry.get(spec_key)
        if spec is None:
            logger.info(f"No input validation spec registered for {name}. Skipping.")
            continue
        messages.extend(_validate_one(name, df, spec))

    if messages:
        raise ADTDataValidationError(
            "Input data validation failed:\n" + "\n".join(messages)
        )
