"""Input Data Validation stage.

Runs in process_dataset after extraction and standardization and before the
dataset's column_rename and transform. Each input file must have a spec in the validation_configs package.
The contract for that file is defined once rather than in every transform
that reads it.

Checks that depend on more than one file, on dataset configuration, or on an
intermediate frame stay in the transform; this stage only validates one file at
a time.
"""

import importlib
import pkgutil

import pandas as pd

from agoradatatools.errors import ADTDataValidationError
from agoradatatools.etl import validation_configs
from agoradatatools.etl.validation_utils import (
    InputSpec,
    check_column_rules,
    check_required_datasets_and_columns,
)


def load_input_specs() -> dict[str, InputSpec]:
    """Collect the SPEC from every module in validation_configs, keyed by module name.

    The module name is the file's name in the pipeline config, or the schema
    value when several files share one spec (e.g. rna_de_file).
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


def validate_inputs(
    datasets: dict[str, pd.DataFrame],
    schemas: dict[str, str] | None = None,
) -> None:
    """Validate every input against a spec in validation_configs.

    All violations across all inputs are collected before raising so one run
    reports every problem. An input with no spec is a failure.

    Args:
        datasets: Input name -> DataFrame, as extracted and standardized, before
            column_rename.
        schemas: Optional input name -> spec key. When omitted, the input name
            is the spec key. process_dataset passes each file's schema field,
            defaulting to its name.

    Raises:
        ADTDataValidationError: If any input has no spec or violates its spec.
    """
    messages: list[str] = []
    for name, df in datasets.items():
        spec_key = (schemas or {}).get(name, name)
        spec = INPUT_SPECS.get(spec_key)
        if spec is None:
            if spec_key != name:
                messages.append(
                    f"No input validation spec registered for {name} "
                    f"(schema {spec_key})."
                )
            else:
                messages.append(f"No input validation spec registered for {name}.")
            continue
        messages.extend(_validate_one(name, df, spec))

    if messages:
        raise ADTDataValidationError(
            "Input data validation failed:\n" + "\n".join(messages)
        )
