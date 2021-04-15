from data_service.service.service import FileService


class LocalFileService(FileService):
    def __init__(self):
        super().__init__()

    def get_file(self, path: str) -> str:
        return path + '__1_0.parquet'
