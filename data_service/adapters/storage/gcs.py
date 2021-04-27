from fastapi import Depends
from google.cloud import storage

from data_service.config import config
from data_service.config.config import get_settings
from data_service.config.logging import get_logger
from data_service.core.file_adapter import FileAdapter

log = get_logger(__name__)


class GcsBucketAdapter(FileAdapter):
    def __init__(self, settings: config.Settings = Depends(get_settings)):
        super().__init__()
        self.settings = settings

    def get_file(self, path: str) -> str:
        return self.__download_file_from_storage(path)

    def __download_file_from_storage(self, path: str) -> str:
        download_filename = path + '__1_0.parquet'
        bucket_name = self.settings.BUCKET_NAME
        blob_download_path = self.__create_download_path(path)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_download_path)

        log.info(f'Trying to download blob {blob_download_path}')
        blob.download_to_filename(download_filename)

        log.info(f'Downloaded blob {blob_download_path} to {download_filename} from bucket {bucket_name}')
        return download_filename

    def __create_download_path(self, path: str) -> str:
        return self.settings.DATASTORE_ROOT + '/dataset/' + path + '/' + path + '__1_0.parquet'
