from abc import ABC, abstractmethod


class FileAdapter(ABC):

    @abstractmethod
    def get_file(self, path: str) -> str:
        pass
