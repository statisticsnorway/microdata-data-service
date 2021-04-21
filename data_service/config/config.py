from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    DATASTORE_ROOT: str
    BUCKET_NAME: str
    DATA_SERVICE_URL: str
    FILE_SERVICE_DATASTORE_ROOT_PREFIX: str

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    return Settings()
