from abc import ABC, abstractmethod


class FileAdapter(ABC):

    @abstractmethod
    def get_parquet_file_path(self, dataset_name: str,
                              version: str) -> str:
        """
        Returns the parquet file path for the given parameters.

            Parameters:
                dataset_name (str): The name of the data structure

                version (str): A 4 part semantic version ex.: 1.0.0.0

            Returns:
                parquet file path (str)
        """
