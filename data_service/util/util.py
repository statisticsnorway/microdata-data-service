import os


def getenv(key: str) -> str:
    if not os.getenv(key):
        raise KeyError('Key ' + key + ' not found in environment')

    return os.getenv(key)
