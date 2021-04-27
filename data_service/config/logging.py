import logging.config
from functools import lru_cache
from logging import Logger

import yaml


@lru_cache()
def get_logger(name: str) -> Logger:
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    return logging.getLogger(name)
