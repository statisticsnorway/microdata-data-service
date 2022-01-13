from abc import ABC, abstractmethod


class FileAdapter(ABC):

    @abstractmethod
    def get_file_path(self, data_structure_name: str, version: str) -> str:
        pass
