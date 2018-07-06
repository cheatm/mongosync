import logging
import sys


def init():
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s", 
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout
    )