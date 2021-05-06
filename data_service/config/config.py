import logging
from functools import lru_cache

from pydantic import BaseSettings, ValidationError

module_logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    DATASTORE_ROOT: str
    BUCKET_NAME: str
    DATA_SERVICE_URL: str
    FILE_SERVICE_DATASTORE_ROOT_PREFIX: str
    STORAGE_ADAPTER: str

    class Config:
        env_file = "data_service/config/.env"


@lru_cache()
def get_settings():
    try:
        return Settings()
    except ValidationError as e:
        module_logger.exception(e)


def to_string():
    storage_adapter = "STORAGE_ADAPTER: {}, ".format(get_settings().STORAGE_ADAPTER)
    datastore_root = "DATASTORE_ROOT: {}, ".format(get_settings().DATASTORE_ROOT)
    file_service_datastore_root_prefix = "FILE_SERVICE_DATASTORE_ROOT_PREFIX: {}, " \
        .format(get_settings().FILE_SERVICE_DATASTORE_ROOT_PREFIX)

    return storage_adapter + file_service_datastore_root_prefix + datastore_root
