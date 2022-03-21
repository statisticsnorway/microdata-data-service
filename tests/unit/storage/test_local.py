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


def test_to_underscored_version():
    assert "13_0_0" == local_file_adapter.__to_underscored_version__("13.0.0.0")
    assert "1_0_0" == local_file_adapter.__to_underscored_version__("1.0.0.0")
    assert "2_0_0" == local_file_adapter.__to_underscored_version__("2.0.0")
