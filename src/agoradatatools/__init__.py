import sys
import logging

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="INFO | %(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
