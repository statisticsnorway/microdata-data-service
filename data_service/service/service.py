
class FileService:

    # This method must be defined by subclasses
    def get_file(self, path: str) -> str:
        raise NotImplementedError
