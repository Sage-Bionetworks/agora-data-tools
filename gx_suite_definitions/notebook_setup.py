"""
Shared setup for the GX expectation suite authoring notebooks in this folder.

The notebooks run with this folder as their working directory, so they can import this module
directly: `from notebook_setup import configure_notebook_output`.
"""

import logging
import warnings

# synapseclient logs through one of these loggers, not a "synapseclient" logger. A [debug] section
# in a user's ~/.synapseConfig switches the client to the debug logger.
SYNAPSE_CLIENT_LOGGERS = ["synapseclient_default", "synapseclient_debug"]

# (warning category, message regex, why it's noise)
_SYNAPSE_LOG_SUPPRESSIONS = [
    (
        "ifcollision=keep.both is being IGNORED",
        "The download destination is Synapse's cache, so the client always "
        "overwrites the existing cached copy — this is expected behavior, no "
        "action needed.",
    ),
]

_PYTHON_WARNING_SUPPRESSIONS = [
    (
        UserWarning,
        r".*result_format.*configured at the Validator-level.*",
        "Great Expectations is reminding us that a setting we're not using "
        "(result_format on the Validator) won't be saved — doesn't apply to "
        "how this notebook runs.",
    ),
    (
        DeprecationWarning,
        r"'parseString' deprecated",
        "A library Great Expectations depends on (pyparsing) is using one of "
        "its own old method names internally. That's GX's dependency to "
        "update, not something in our code.",
    ),
]

class SynapseCollisionFilter(logging.Filter):
    """Drops the synapseclient banner about ifcollision being ignored for downloads to the
    Synapse cache, letting every other record through."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Returns False only for the ifcollision cache banner."""
        # return "ifcollision=keep.both is being IGNORED" not in record.getMessage()
        return not any(text in record.getMessage() for text, _why in _SYNAPSE_LOG_SUPPRESSIONS)


def configure_notebook_output() -> None:
    """Quiets duplicated and known-noisy log/warning output in the GX authoring notebooks.

    synapseclient prints each record through its own handlers, then propagates it to the root
    logger, whose handler (from agoradatatools' logging.basicConfig) prints it a second time.
    Python warnings take the same path through the py.warnings logger, which synapseclient gives
    its own handlers. Turning off propagation on those loggers prints each record once.

    Also hides the synapseclient ifcollision cache banner, GX's Validator-level result_format
    warning, and pyparsing's parseString deprecation warning. Call it before gx.get_context(),
    which is where the parseString warning fires. Safe to call more than once.
    """
    for logger_name in SYNAPSE_CLIENT_LOGGERS + ["py.warnings"]:
        logging.getLogger(logger_name).propagate = False

    for logger_name in SYNAPSE_CLIENT_LOGGERS:
        logger = logging.getLogger(logger_name)
        if not any(isinstance(f, SynapseCollisionFilter) for f in logger.filters):
            logger.addFilter(SynapseCollisionFilter())


    for category, pattern, _why in _PYTHON_WARNING_SUPPRESSIONS:
        warnings.filterwarnings("ignore", category=category, message=pattern)

    total = len(_SYNAPSE_LOG_SUPPRESSIONS) + len(_PYTHON_WARNING_SUPPRESSIONS)
    print(f"notebook_setup: suppressing {total} known warnings:")
    for _text, why in _SYNAPSE_LOG_SUPPRESSIONS:
        print(f"  - {why}")
    for _cat, _pattern, why in _PYTHON_WARNING_SUPPRESSIONS:
        print(f"  - {why}")
