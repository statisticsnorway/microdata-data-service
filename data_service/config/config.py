import logging
import os
from functools import lru_cache

from pydantic import BaseSettings, ValidationError

module_logger = logging.getLogger(__name__)


class GoogleCloudSettings(BaseSettings):
    DATASTORE_ROOT: str
    DATA_SERVICE_URL: str
    BUCKET_NAME: str

    def print(self):
        return f'Using GoogleCloudSettings: ' \
               f'DATASTORE_ROOT: {self.DATASTORE_ROOT}, ' \
               f'DATA_SERVICE_URL: {self.DATA_SERVICE_URL}, ' \
               f'BUCKET_NAME: {self.BUCKET_NAME}'

    class Config:
        env_file = "data_service/config/.env.gcs"


class LocalFileSettings(BaseSettings):
    DATASTORE_ROOT: str
    DATA_SERVICE_URL: str
    FILE_SERVICE_DATASTORE_ROOT_PREFIX: str

    def print(self):
        return f'Using LocalFileSettings: ' \
               f'DATASTORE_ROOT: {self.DATASTORE_ROOT}, ' \
               f'DATA_SERVICE_URL: {self.DATA_SERVICE_URL}, ' \
               f'FILE_SERVICE_DATASTORE_ROOT_PREFIX: {self.FILE_SERVICE_DATASTORE_ROOT_PREFIX}'

    class Config:
        env_file = "data_service/config/.env.local_file"


@lru_cache()
def get_settings():
    try:
        if os.getenv('CONFIG_PROFILE') == 'GCS':
            return GoogleCloudSettings()
        else:
            return LocalFileSettings()
    except ValidationError as e:
        module_logger.exception(e)
