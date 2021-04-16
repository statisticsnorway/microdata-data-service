from google.cloud import storage

from data_service.service.service import FileService
from data_service.util.util import getenv


class GcsFileService(FileService):
    def __init__(self):
        super().__init__()

    def get_file(self, path: str) -> str:
        return self.__download_file_from_storage(path)

    def __download_file_from_storage(self, path: str) -> str:
        download_filename = path + '__1_0.parquet'
        bucket_name = getenv('BUCKET_NAME')
        blob_download_path = self.__create_download_path(path)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_download_path)

        print("Trying to download blob {}".format(blob_download_path))
        blob.download_to_filename(download_filename)

        print("Downloaded blob {} to {} from bucket {}.".format(blob_download_path, download_filename, bucket_name))
        return download_filename

    def __create_download_path(self, path: str) -> str:
        return getenv('DATASTORE_ROOT') + '/dataset/' + path + '/' + path + '__1_0.parquet'
