import logging
import os

from fastapi import Depends

from data_service.config import config
from data_service.config.config import get_settings
from data_service.adapters.storage.file_adapter import FileAdapter


class LocalFileAdapter(FileAdapter):
    def __init__(self, settings: config.LocalFileSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.LocalFileAdapter')
        self.settings = settings

    def get_file(self, dataset_name: str, version: str) -> str:
        dataset_version = version.replace('.', '_')[:3]
        return self.__create_download_path(dataset_name, dataset_version)

    def __create_download_path(self, dataset_name: str, version: str) -> str:
        path = (
            f"{self.settings.DATASTORE_DIR}/data/"
            f"{dataset_name}/{dataset_name}__{version}"
        )
        return path if os.path.isdir(path) else f"{path}.parquet"
