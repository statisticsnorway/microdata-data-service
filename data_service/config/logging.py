import logging.config
from functools import lru_cache
from logging import Logger

import yaml


@lru_cache()
def get_logger(name: str) -> Logger:
    path_to_config_file = 'data_service/config/logging.yaml'

    with open(path_to_config_file, 'r') as f:
        config_contents = yaml.safe_load(f.read())
        logging.config.dictConfig(config_contents)

    return logging.getLogger(name)
