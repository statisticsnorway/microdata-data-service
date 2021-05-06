from functools import lru_cache

from data_service.config.config import get_settings
from data_service.core.processor import Processor


@lru_cache()
def get_processor():
    return Processor(get_settings())
