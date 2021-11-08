import logging
import os
from functools import lru_cache

from pydantic import BaseSettings, ValidationError

module_logger = logging.getLogger(__name__)


class GoogleCloudSettings(BaseSettings):
    DATASTORE_ROOT: str
    DATA_SERVICE_URL: str
    BUCKET_NAME: str
    BLOB_DOWNLOAD_ROOT: str

    def print(self):
        return f'Using GoogleCloudSettings: ' \
               f'DATASTORE_ROOT: {self.DATASTORE_ROOT}, ' \
               f'DATA_SERVICE_URL: {self.DATA_SERVICE_URL}, ' \
               f'BUCKET_NAME: {self.BUCKET_NAME}, ' \
               f'BLOB_DOWNLOAD_ROOT: {self.BLOB_DOWNLOAD_ROOT}'

    class Config:
        env_file = "data_service/config/.env.gcs"


class LocalFileSettings(BaseSettings):
    DATA_SERVICE_URL: str
    DATASTORE_DIR: str
    RESULTSET_DIR: str

    def print(self):
        return f'Using LocalFileSettings: ' \
               f'DATA_SERVICE_URL: {self.DATA_SERVICE_URL}, ' \
               f'DATASTORE_ROOT_PREFIX: {self.DATASTORE_DIR}' \
               f'RESULTSET_DIR: {self.RESULTSET_DIR}'

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
