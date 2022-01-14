from data_service.adapters.storage.local import LocalFileAdapter
from data_service.config import config


local_file_adapter = LocalFileAdapter(
    settings=config.LocalFileSettings(
        DATASTORE_DIR='tests/resources/datastore_unit_test',
    ))


def test_get_file_path():
    assert local_file_adapter.get_parquet_file_path("TEST_PERSON_INCOME", "0.0.0.1") \
            == "tests/resources/datastore_unit_test/data/TEST_PERSON_INCOME/TEST_PERSON_INCOME__0_0.parquet"


def test_get_file_path_draft():
    assert local_file_adapter.get_parquet_file_path("TEST_PERSON_INCOME", "draft") \
            == "tests/resources/datastore_unit_test/data/TEST_PERSON_INCOME/TEST_PERSON_INCOME__0_0.parquet"
