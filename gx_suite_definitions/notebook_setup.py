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


class SynapseCollisionFilter(logging.Filter):
    """Drops the synapseclient banner about ifcollision being ignored for downloads to the
    Synapse cache, letting every other record through."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Returns False only for the ifcollision cache banner."""
        return "ifcollision=keep.both is being IGNORED" not in record.getMessage()


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

    # Suppress GX's internal result_format validator noise
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        message=".*result_format.*configured at the Validator-level.*",
    )

    # Suppress pyparsing's deprecation of parseString, which GX 0.18 still calls when
    # gx.get_context() reads the store backend id. PyparsingDeprecationWarning subclasses
    # DeprecationWarning, so this matches it without importing pyparsing.
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message="'parseString' deprecated",
    )
