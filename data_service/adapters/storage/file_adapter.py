from abc import ABC, abstractmethod


class FileAdapter(ABC):

    @abstractmethod
    def get_file(self, path: str, version: str) -> str:
        pass
