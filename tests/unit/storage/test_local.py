# pylint: disable=protected-access
from data_service.adapters.storage.local import LocalFileAdapter
from data_service.config import config

TEST_DIR = 'tests/resources/datastore_unit_test/data'
TEST_PERSON_INCOME_PATH = f'{TEST_DIR}/TEST_PERSON_INCOME/TEST_PERSON_INCOME'
TEST_PERSON_INCOME_PATH_1_0 = f'{TEST_PERSON_INCOME_PATH}__1_0.parquet'
TEST_PERSON_INCOME_PATH_DRAFT = f'{TEST_PERSON_INCOME_PATH}__DRAFT.parquet'
TEST_STUDIEPOENG_PATH_1_0 = (
    f'{TEST_DIR}/TEST_STUDIEPOENG/TEST_STUDIEPOENG__1_0'
)


local_file_adapter = LocalFileAdapter(
    settings=config.LocalFileSettings(
        DATASTORE_DIR='tests/resources/datastore_unit_test',
    ))


def test_get_file_path():
    assert TEST_PERSON_INCOME_PATH_1_0 == (
        local_file_adapter.get_parquet_file_path(
            "TEST_PERSON_INCOME", "1.0.0.0"
        )
     )


def test_get_file_path_draft():
    assert TEST_PERSON_INCOME_PATH_DRAFT == (
        local_file_adapter.get_parquet_file_path(
            "TEST_PERSON_INCOME", "0.0.0.1"
        )
     )


def test_get_partitioned_file_path():
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        local_file_adapter.get_parquet_file_path(
            "TEST_STUDIEPOENG", "1.0.0.0"
        )
    )


def test_to_underscored_version():
    assert local_file_adapter._to_underscored_version(
        "13.0.0.0"
    ) == "13_0_0_0"
    assert local_file_adapter._to_underscored_version(
        "1.2.0.0"
    ) == "1_2_0_0"
