import json
import logging
import os

from fastapi import Depends

from data_service.adapters.storage.file_adapter import FileAdapter
from data_service.config import config
from data_service.config.config import get_settings
from data_service.exceptions import NotFoundException


class LocalFileAdapter(FileAdapter):
    def __init__(self, settings: config.LocalFileSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.LocalFileAdapter')
        self.settings = settings

    def get_file_path(self, data_structure_name: str, version: str) -> str:
        version_underscored = version.replace('.', '_')[:5]
        path_prefix = f"{self.settings.DATASTORE_DIR}/data/{data_structure_name}"

        if version == "0.0.0":
            parquet_file_path = f"{data_structure_name}__{version_underscored[:3]}"
            full_path = (
                f"{path_prefix}/{parquet_file_path}"
            )
            full_path = full_path if os.path.isdir(full_path) else f"{full_path}.parquet"
        else:
            data_versions = self.__get_data_versions__(version_underscored)
            if data_structure_name not in data_versions:
                raise NotFoundException(
                    f"No such data structure in data_versions file for version {version}")
            parquet_file_path = data_versions[data_structure_name]

            full_path = (
                f"{path_prefix}/{parquet_file_path}"
            )

        if not os.path.exists(full_path):
            self.log.error(f"Path {parquet_file_path} not does not exist")
            raise NotFoundException("No such data structure")

        return full_path

    def __get_data_versions__(self, version_underscored: str) -> str:
        data_versions_file = (
            f"{self.settings.DATASTORE_DIR}/datastore/data_versions__{version_underscored}.json"
        )
        with open(data_versions_file, encoding="utf-8") as f:
            return json.load(f)
