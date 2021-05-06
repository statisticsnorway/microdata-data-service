import logging
import os
from functools import lru_cache

from pydantic import BaseSettings, ValidationError

module_logger = logging.getLogger(__name__)


class GoogleCloudSettings(BaseSettings):
    DATASTORE_ROOT: str
    BUCKET_NAME: str
    DATA_SERVICE_URL: str
    STORAGE_ADAPTER: str

    class Config:
        env_file = "data_service/config/.env.gcs"


class LocalFileSettings(BaseSettings):
    DATASTORE_ROOT: str
    DATA_SERVICE_URL: str
    FILE_SERVICE_DATASTORE_ROOT_PREFIX: str
    STORAGE_ADAPTER: str

    def __str__(self):
        return f'DATASTORE_ROOT: {self.DATASTORE_ROOT}, ' \
               f'DATA_STORAGE_ADAPTER: {self.STORAGE_ADAPTER}, ' \
               f'STORAGE_ADAPTER: {self.STORAGE_ADAPTER}, ' \
               f'FILE_SERVICE_DATASTORE_ROOT_PREFIX: {self.FILE_SERVICE_DATASTORE_ROOT_PREFIX}'

    class Config:
        env_file = "data_service/config/.env.local_file"


@lru_cache()
def get_settings():
    try:
        if os.getenv('STORAGE_ADAPTER') == 'GCS':
            return GoogleCloudSettings()
        else:
            return LocalFileSettings()
    except ValidationError as e:
        module_logger.exception(e)
