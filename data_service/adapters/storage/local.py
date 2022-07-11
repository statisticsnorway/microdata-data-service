import json
import logging
import os
from typing import Union

from fastapi import Depends

from data_service.adapters.storage.file_adapter import FileAdapter
from data_service.config import config
from data_service.config.config import get_settings
from data_service.exceptions import NotFoundException


class LocalFileAdapter(FileAdapter):
    def __init__(
        self, settings: config.LocalFileSettings = Depends(get_settings)
    ):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.LocalFileAdapter')
        self.settings = settings

    def get_parquet_file_path(
        self, dataset_name: str, version: str
    ) -> str:
        path_prefix = (
            f"{self.settings.DATASTORE_DIR}/data/{dataset_name}"
        )
        if version.startswith("0"):
            full_path = self._get_draft_file_path(path_prefix, dataset_name)
            if full_path is None:
                latest = self._get_latest_version()
                file_name = self._get_file_name_from_data_versions(
                    latest, dataset_name
                )
                full_path = f'{path_prefix}/{file_name}'
        else:
            file_name = self._get_file_name_from_data_versions(
                version, dataset_name
            )
            full_path = f'{path_prefix}/{file_name}'

        if not os.path.exists(full_path):
            self.log.error(f"Path {full_path} does not exist")
            raise NotFoundException("No such data structure")

        return full_path

    def _get_file_name_from_data_versions(
        self, version: str, dataset_name: str
    ) -> str:
        version = self._to_underscored_version(version)
        data_versions_file = (
            f"{self.settings.DATASTORE_DIR}/datastore"
            f"/data_versions__{version}.json"
        )
        with open(data_versions_file, encoding="utf-8") as f:
            data_versions = json.load(f)

        if dataset_name not in data_versions:
            raise NotFoundException(
                f"No {dataset_name} in data_versions file "
                f"for version {version}"
            )
        return data_versions[dataset_name]

    def _get_draft_file_path(
        self, path_prefix: str, dataset_name: str
    ) -> Union[None, str]:
        parquet_path = f"{path_prefix}/{dataset_name}__DRAFT.parquet"
        partitioned_parquet_path = parquet_path.replace('.parquet', '')
        if os.path.isfile(parquet_path):
            return parquet_path
        elif os.path.isdir(partitioned_parquet_path):
            return partitioned_parquet_path
        else:
            return None

    def _get_latest_version(self):
        datastore_files = os.listdir(
            f'{self.settings.DATASTORE_DIR}/datastore'
        )
        data_versions_files = [
            file for file in datastore_files
            if file.startswith('data_versions')
        ]
        data_versions_files.sort()
        latest_data_versions_file = data_versions_files[-1]
        return (
            latest_data_versions_file.strip('.json').strip('data_versions__')
        )

    def _to_underscored_version(self, version: str) -> str:
        version = version.replace('.', '_')
        return version
