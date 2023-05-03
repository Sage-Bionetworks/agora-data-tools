import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="INFO: %(message)s")
logger = logging.getLogger(__name__)
