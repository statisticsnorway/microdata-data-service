import os


def _initialize_environment() -> dict:
    return {
        "PORT": int(os.environ["PORT"]),
        "DATASTORE_DIR": os.environ["DATASTORE_DIR"],
        "DOCKER_HOST_NAME": os.environ["DOCKER_HOST_NAME"],
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
