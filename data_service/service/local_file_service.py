from data_service.service.service import FileService
from data_service.util.util import getenv


class LocalFileService(FileService):
    def __init__(self):
        super().__init__()

    def get_file(self, path: str) -> str:
        return self.__create_download_path(path)

    def __create_download_path(self, path: str) -> str:
        return getenv('DATASTORE_ROOT') + '/dataset/' + path + '/' + path + '__1_0.parquet'