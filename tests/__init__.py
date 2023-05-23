import sys
import logging

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s: %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
