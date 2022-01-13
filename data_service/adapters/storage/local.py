import json
import logging

from fastapi import Depends

from data_service.adapters.storage.file_adapter import FileAdapter
from data_service.config import config
from data_service.config.config import get_settings


class LocalFileAdapter(FileAdapter):
    def __init__(self, settings: config.LocalFileSettings = Depends(get_settings)):
        super().__init__()
        self.log = logging.getLogger(__name__ + '.LocalFileAdapter')
        self.settings = settings

    def get_file_path(self, path: str) -> str:
        return f"{self.settings.DATASTORE_DIR}/data/{path}"

    def get_data_versions(self, version: str) -> str:
        version_underscored = version.replace('.', '_')[:3]
        data_versions_file = (
            f"{self.settings.DATASTORE_DIR}/datastore/data_versions__{version_underscored}.json"
        )
        with open(data_versions_file, encoding="utf-8") as f:
            return json.load(f)
