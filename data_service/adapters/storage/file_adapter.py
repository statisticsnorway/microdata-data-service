from abc import ABC, abstractmethod


class FileAdapter(ABC):

    @abstractmethod
    def get_file_path(self, path: str) -> str:
        pass

    @abstractmethod
    def get_data_versions(self, version: str) -> str:
        pass
