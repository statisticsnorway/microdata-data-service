from fastapi import Depends

from data_service.config import config
from data_service.config.config import get_settings
from data_service.core.file_adapter import FileAdapter


class LocalFileAdapter(FileAdapter):
    def __init__(self, settings: config.Settings = Depends(get_settings)):
        super().__init__()
        self.settings = settings

    def get_file(self, path: str) -> str:
        return self.__create_download_path(path)

    def __create_download_path(self, path: str) -> str:
        return self.settings.FILE_SERVICE_DATASTORE_ROOT_PREFIX + '/' + self.settings.DATASTORE_ROOT \
               + '/dataset/' + path + '/' + path + '__1_0.parquet'
