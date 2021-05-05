import logging
from functools import lru_cache

from pydantic import BaseSettings, ValidationError

module_logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    DATASTORE_ROOT: str
    BUCKET_NAME: str
    DATA_SERVICE_URL: str
    FILE_SERVICE_DATASTORE_ROOT_PREFIX: str
    STORAGE_ADAPTER: str

    class Config:
        env_file = "data_service/config/.env"


@lru_cache()
def get_settings():
    try:
        return Settings()
    except ValidationError as e:
        module_logger.exception(e)


def to_string():
    return "Settings: FILE_SERVICE_DATASTORE_ROOT_PREFIX: {}, DATASTORE_ROOT: {}, "\
                .format(get_settings().FILE_SERVICE_DATASTORE_ROOT_PREFIX, get_settings().DATASTORE_ROOT)
