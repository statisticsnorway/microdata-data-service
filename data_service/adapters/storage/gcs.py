import logging
import os
import shutil

from fastapi import Depends
from google.cloud import storage

from data_service.config import config
from data_service.config.config import get_settings
from data_service.adapters.storage.file_adapter import FileAdapter


class GcsBucketAdapter(FileAdapter):
    """ This adapter is not currently maintained. See git history for previous version """

    def __init__(self, settings: config.GoogleCloudSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.GcsBucketAdapter')
        self.settings = settings

    def get_parquet_file_path(self, data_structure_name: str, version: str) -> str:
        raise NotImplementedError

    def __download_file_from_storage(self, path: str, version: str) -> str:
        dataset_version = version.replace('.', '_')[:3]
        destination_uri = f'{path}__{dataset_version}.parquet'
        bucket_name = self.settings.BUCKET_NAME
        blob_download_path = self.__create_download_path(path)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_download_path)

        if blob is None:
            return None

        self.log.info(f'Trying to download blob {blob_download_path}')
        blob.download_to_filename(destination_uri)

        self.log.info(
            f'Downloaded blob {blob_download_path} to '
            f'{destination_uri} from bucket {bucket_name}'
        )
        return destination_uri

    def __download_partitioned_file_from_storage(self, path: str,
                                                 version: str) -> str:
        bucket_name = self.settings.BUCKET_NAME
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blob_download_path = f'{self.settings.BLOB_DOWNLOAD_ROOT}/{path}'
        if os.path.exists(blob_download_path):
            shutil.rmtree(blob_download_path)

        os.makedirs(blob_download_path)

        dataset_dir_in_bucket = f'{self.settings.DATASTORE_ROOT}/{"dataset"}/{path}/{path}/'
        blobs = bucket.list_blobs(prefix=dataset_dir_in_bucket)

        for blob in blobs:
            self.log.info(f'Blob: {blob.name}')
            file_path = blob.name.partition(dataset_dir_in_bucket)[2]
            start_year_dir = file_path.split("/")[0]

            parquet_file_dir = f'{blob_download_path}/{start_year_dir}'
            if not os.path.exists(parquet_file_dir):
                os.makedirs(parquet_file_dir)

            destination_uri = f'{blob_download_path}/{file_path}'
            blob.download_to_filename(destination_uri)
            self.log.info(f'Downloaded blob {blob_download_path} to {destination_uri} from bucket {bucket_name}')

        if blobs.num_results == 0:
            return None

        return blob_download_path

    def __create_download_path(self, path: str) -> str:
        return self.settings.DATASTORE_ROOT + '/dataset/' + path + '/' + path + '__1_0.parquet'
